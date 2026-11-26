import { importRuntime } from './runtime-import.js'
import type { CompressionWorker } from './options.js'

export interface WorkerPoolOptions {
  /** Maximum simultaneous operations. Default: 1 (one reusable worker). */
  size?: number
  /** Maximum additional outstanding operations beyond size. Default: 64. */
  maxQueuedOperations?: number
  /** Retained input/dictionary bytes across outstanding operations. Default: 64 MiB. */
  maxPendingBytes?: number
  workerFactory?: () => CompressionWorker
}

export class WorkerPoolCapacityError extends Error {
  override readonly name = 'WorkerPoolCapacityError'
  constructor(readonly limit: 'maxQueuedOperations' | 'maxPendingBytes') {
    super(`Compression worker pool exceeds ${limit}`)
  }
}

/** @internal */
export interface WorkerReservation {
  release(): void
}

export interface CompressionWorkerPool {
  readonly size: number
  /** Reject queued/active operations and terminate all workers. Idempotent. */
  close(): Promise<void>
}

/** @internal */
export interface WorkerLease {
  worker: CompressionWorker
  signal: AbortSignal
  release(reusable: boolean): void
}

type Request = {
  signal: AbortSignal
  resolve(lease: WorkerLease): void
  reject(error: unknown): void
  detach(): void
}

type Slot = {
  worker?: CompressionWorker
  busy: boolean
  dead: boolean
  controller: AbortController
  remove: (() => void)[]
  pending?: Promise<void>
}

type State = {
  slots: Set<Slot>
  queue: Request[]
  closed: boolean
  closing?: Promise<void>
  acquire(signal: AbortSignal): Promise<WorkerLease>
  reserve(bytes: number): WorkerReservation
}

const pools = new WeakMap<CompressionWorkerPool, State>()

/** @internal */
export async function defaultWorker(): Promise<CompressionWorker> {
  if (typeof Worker !== 'undefined') {
    return new Worker(new URL('./worker.js', import.meta.url), { type: 'module' })
  }
  // Keep Node builtins out of the browser dependency graph and parent flags
  // such as --input-type out of the file-based worker entry point.
  const specifier = 'node:worker_threads'
  const { Worker: NodeWorker } = await importRuntime(specifier)
  return new NodeWorker(new URL('./worker.js', import.meta.url), {
    execArgv: [],
  }) as CompressionWorker
}

/** @internal */
export async function terminateWorker(worker: CompressionWorker): Promise<void> {
  try {
    await worker.terminate()
  } catch {
    /* A failed custom worker may already be disposed. */
  }
}

/** @internal */
export function isWorkerPool(value: unknown): value is CompressionWorkerPool {
  return typeof value === 'object' && value !== null && pools.has(value as CompressionWorkerPool)
}

/** @internal */
export function acquireWorker(
  pool: CompressionWorkerPool,
  signal: AbortSignal,
): Promise<WorkerLease> {
  return pools.get(pool)!.acquire(signal)
}

/** @internal Synchronous admission, before input/dictionary snapshots or ownership transfer. */
export function reserveWorker(pool: CompressionWorkerPool, bytes: number): WorkerReservation {
  return pools.get(pool)!.reserve(bytes)
}

export function createWorkerPool(options: WorkerPoolOptions = {}): CompressionWorkerPool {
  if (typeof options !== 'object' || options === null || Array.isArray(options)) {
    throw new TypeError('Expected worker pool options')
  }
  for (const key of Object.keys(options)) {
    if (!['size', 'workerFactory', 'maxQueuedOperations', 'maxPendingBytes'].includes(key)) {
      throw new TypeError(`Unexpected worker pool option: ${key}`)
    }
  }
  const {
    size = 1,
    workerFactory = defaultWorker,
    maxQueuedOperations = 64,
    maxPendingBytes = 64 * 1024 * 1024,
  } = options
  if (!Number.isSafeInteger(size) || size < 1) {
    throw new TypeError('Worker pool size must be a positive safe integer')
  }
  if (typeof workerFactory !== 'function') {
    throw new TypeError('workerFactory must be a function')
  }
  for (const [name, value] of Object.entries({ maxQueuedOperations, maxPendingBytes })) {
    if (value !== Infinity && (!Number.isSafeInteger(value) || value < 0)) {
      throw new TypeError(`${name} must be a nonnegative safe integer or Infinity`)
    }
  }
  let pendingOperations = 0
  let pendingBytes = 0
  const state: State = { slots: new Set(), queue: [], closed: false, acquire, reserve }
  const terminations = new Set<Promise<void>>()
  const terminate = (worker: CompressionWorker) => {
    const pending = terminateWorker(worker)
    terminations.add(pending)
    void pending.then(() => terminations.delete(pending))
    return pending
  }
  const closedError = () => new DOMException('Compression worker pool closed', 'AbortError')
  function reserve(bytes: number): WorkerReservation {
    if (state.closed) {
      throw closedError()
    }
    if (pendingOperations >= size && pendingOperations - size >= maxQueuedOperations) {
      throw new WorkerPoolCapacityError('maxQueuedOperations')
    }
    if (bytes > maxPendingBytes - pendingBytes || bytes > Number.MAX_SAFE_INTEGER - pendingBytes) {
      throw new WorkerPoolCapacityError('maxPendingBytes')
    }
    pendingOperations++
    pendingBytes += bytes
    let released = false
    return {
      release() {
        if (!released) {
          released = true
          pendingOperations--
          pendingBytes -= bytes
        }
      },
    }
  }
  const destroy = (slot: Slot, reason?: unknown): Promise<void> => {
    if (slot.dead) {
      return slot.pending ?? Promise.resolve()
    }
    slot.dead = true
    state.slots.delete(slot)
    slot.controller.abort(reason)
    for (const remove of slot.remove) {
      try {
        remove()
      } catch {
        /* Continue teardown. */
      }
    }
    const pending = slot.worker ? terminate(slot.worker) : (slot.pending ?? Promise.resolve())
    drain()
    return pending
  }
  const watch = (slot: Slot) => {
    const worker = slot.worker!
    const error = (value: unknown) => {
      const reason =
        value instanceof Error
          ? value
          : new Error(
              typeof ErrorEvent !== 'undefined' && value instanceof ErrorEvent
                ? value.message || 'Compression worker failed'
                : `Compression worker failed or exited (${String(value)})`,
            )
      void destroy(slot, reason)
    }
    if (worker.addEventListener && worker.removeEventListener) {
      for (const event of ['error', 'messageerror']) {
        worker.addEventListener(event, error)
        slot.remove.push(() => worker.removeEventListener!(event, error))
      }
    } else if (worker.on && worker.off) {
      for (const event of ['error', 'exit', 'messageerror']) {
        worker.on(event, error)
        slot.remove.push(() => worker.off!(event, error))
      }
    } else {
      throw new TypeError('Reusable workers require removable event listeners')
    }
  }
  const grant = (request: Request, slot: Slot) => {
    request.detach()
    if (request.signal.aborted || state.closed || slot.dead) {
      request.reject(request.signal.aborted ? request.signal.reason : closedError())
      void destroy(slot)
      return
    }
    let released = false
    request.resolve({
      worker: slot.worker!,
      signal: slot.controller.signal,
      release(reusable) {
        if (released) {
          return
        }
        released = true
        if (!reusable || state.closed) {
          void destroy(slot)
        } else {
          slot.busy = false
          drain()
        }
      },
    })
  }
  function drain() {
    if (state.closed) {
      return
    }
    while (state.queue.length) {
      let slot = [...state.slots].find((slot) => !slot.busy && !slot.dead)
      if (!slot && state.slots.size >= size) {
        return
      }
      const request = state.queue.shift()!
      if (slot) {
        slot.busy = true
        grant(request, slot)
      } else {
        slot = { busy: true, dead: false, controller: new AbortController(), remove: [] }
        const starting = slot
        state.slots.add(starting)
        starting.pending = (async () => {
          try {
            starting.worker = await workerFactory()
            if (state.closed || request.signal.aborted) {
              await terminate(starting.worker)
              request.detach()
              request.reject(request.signal.aborted ? request.signal.reason : closedError())
              starting.dead = true
              state.slots.delete(starting)
              drain()
              return
            }
            watch(starting)
            grant(request, starting)
          } catch (error) {
            request.detach()
            request.reject(error)
            // Do not await the startup promise from inside itself.
            starting.pending = undefined
            await destroy(starting, error)
          }
        })()
      }
    }
  }
  function acquire(signal: AbortSignal): Promise<WorkerLease> {
    if (state.closed) {
      return Promise.reject(closedError())
    }
    if (signal.aborted) {
      return Promise.reject(signal.reason)
    }
    return new Promise((resolve, reject) => {
      const abort = () => {
        const index = state.queue.indexOf(request)
        if (index !== -1) {
          state.queue.splice(index, 1)
        }
        request.detach()
        reject(signal.reason)
      }
      const request: Request = {
        signal,
        resolve,
        reject,
        detach: () => signal.removeEventListener('abort', abort),
      }
      signal.addEventListener('abort', abort, { once: true })
      state.queue.push(request)
      drain()
    })
  }
  const pool: CompressionWorkerPool = Object.freeze({
    size,
    close() {
      if (state.closing) {
        return state.closing
      }
      state.closed = true
      const reason = closedError()
      for (const request of state.queue.splice(0)) {
        request.detach()
        request.reject(reason)
      }
      state.closing = Promise.all([...state.slots].map((slot) => destroy(slot, reason))).then(
        async () => {
          await Promise.all(terminations)
        },
      )
      return state.closing
    },
  })
  pools.set(pool, state)
  return pool
}
