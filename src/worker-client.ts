import { manageStream } from './flow.js'
import {
  abortReason,
  hasOnlyKeys,
  isRecord,
  type ByteStream,
  type CompressionWorker,
  type Operation,
  type ParsedOptions,
} from './options.js'
import { acquireWorker, defaultWorker, terminateWorker, type WorkerLease } from './worker-pool.js'
import { zstdUrls } from './zstd-module.js'

type WorkerFailure = { error: string; name: string; domException: boolean }

function parseResponse(value: unknown): ByteStream | WorkerFailure | undefined {
  if (!isRecord(value)) {
    return
  }
  const { readable, writable, error, name, domException } = value
  if (
    readable instanceof ReadableStream &&
    writable instanceof WritableStream &&
    !readable.locked &&
    !writable.locked &&
    hasOnlyKeys(value, 'readable', 'writable')
  ) {
    return { readable, writable }
  }
  if (
    typeof error === 'string' &&
    typeof name === 'string' &&
    typeof domException === 'boolean' &&
    hasOnlyKeys(value, 'error', 'name', 'domException')
  ) {
    return { error, name, domException }
  }
}

function initializationError({ error, name, domException }: WorkerFailure): Error {
  // The marker preserves every DOMException name, including custom names.
  if (domException) {
    return new DOMException(error, name)
  }
  switch (name) {
    case 'RangeError':
      return new RangeError(error)
    case 'TypeError':
      return new TypeError(error)
    default: {
      const failure = new Error(error)
      failure.name = name
      return failure
    }
  }
}

export async function createWorkerStream(
  operation: Operation,
  options: ParsedOptions,
  onDispose: () => void = () => {},
): Promise<ByteStream> {
  options.signal?.throwIfAborted()
  return new Promise((resolve, reject) => {
    const controller = new AbortController()
    let worker: CompressionWorker | undefined
    let lease: WorkerLease | undefined
    let ready = false
    let closed = false
    const remove: (() => void)[] = []
    const dispose = (reusable = false) => {
      if (closed) {
        return
      }
      closed = true
      clearTimeout(timer)
      options.signal?.removeEventListener('abort', abort)
      let cleanupFailed = false
      for (const unsubscribe of remove) {
        try {
          unsubscribe()
        } catch {
          cleanupFailed = true
        }
      }
      lease?.signal.removeEventListener('abort', poolAbort)
      if (lease) {
        lease.release(reusable && !cleanupFailed)
      } else if (worker) {
        void terminateWorker(worker)
      }
      onDispose()
    }
    const fail = (error: unknown) => {
      if (closed) {
        return
      }
      controller.abort(error)
      if (!ready) {
        reject(error)
      }
      dispose()
    }
    const abort = () => fail(abortReason(options.signal))
    const poolAbort = () => fail(lease!.signal.reason)
    const timer = setTimeout(
      () => fail(new DOMException('Compression initialization timed out', 'TimeoutError')),
      options.initializationTimeoutMs,
    )
    const message = (value: unknown) => {
      if (closed) {
        return
      }
      try {
        const data = parseResponse(value)
        if (ready || !data) {
          throw new Error('Invalid compression worker response')
        }
        if ('error' in data) {
          throw initializationError(data)
        }
        const pair = manageStream(
          { readable: data.readable, writable: data.writable },
          { ...options, signal: controller.signal },
          (failed) => dispose(!failed),
        )
        ready = true
        clearTimeout(timer)
        resolve(pair)
      } catch (error) {
        fail(error)
      }
    }
    const initialize = async () => {
      try {
        if (options.workerPool) {
          lease = await acquireWorker(options.workerPool, controller.signal)
          worker = lease.worker
          lease.signal.addEventListener('abort', poolAbort, { once: true })
          if (lease.signal.aborted) {
            poolAbort()
          }
        } else {
          worker = options.workerFactory ? options.workerFactory() : await defaultWorker()
        }
        if (closed) {
          if (lease) {
            lease.release(false)
          } else {
            void terminateWorker(worker)
          }
          return
        }
        if (worker.addEventListener && (!lease || worker.removeEventListener)) {
          const onMessage = (event: MessageEvent | ErrorEvent) =>
            message((event as MessageEvent).data)
          const onError = (event: MessageEvent | ErrorEvent) =>
            fail(new Error((event as ErrorEvent).message || 'Compression worker failed'))
          const onMessageError = () => fail(new Error('Invalid compression worker message'))
          for (const [event, listener] of [
            ['message', onMessage],
            ['error', onError],
            ['messageerror', onMessageError],
          ] as const) {
            worker.addEventListener(event, listener)
            remove.push(() => worker!.removeEventListener?.(event, listener))
          }
        } else if (worker.on) {
          const onError = (error: unknown) => fail(error)
          const onExit = (code: unknown) => fail(new Error(`Compression worker exited (${code})`))
          const onMessageError = () => fail(new Error('Invalid compression worker message'))
          for (const [event, listener] of [
            ['message', message],
            ['error', onError],
            ['exit', onExit],
            ['messageerror', onMessageError],
          ] as const) {
            worker.on(event, listener)
            remove.push(() => worker!.off?.(event, listener))
          }
        } else {
          fail(new TypeError('Invalid compression worker'))
          return
        }
        const {
          signal: _signal,
          workerFactory: _factory,
          workerPool: _pool,
          ...serializable
        } = options
        const customAssets =
          options.format === 'zstd' &&
          (options.loaderUrl !== undefined || options.wasmUrl !== undefined)
        worker.postMessage({
          operation,
          options: {
            ...serializable,
            execution: 'direct',
            ...(customAssets ? zstdUrls(options) : {}),
          },
        })
      } catch (error) {
        fail(error)
      }
    }
    options.signal?.addEventListener('abort', abort, { once: true })
    if (options.signal?.aborted) {
      abort()
      return
    }
    void initialize()
  })
}
