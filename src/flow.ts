import { abortReason, snapshot, viewOf } from './options.js'
import type { BinaryInput, ByteStream, ParsedOptions } from './options.js'

/** Applies the same limits, ownership and cancellation rules to all backends. */
export function manageStream(
  pair: ByteStream,
  options: ParsedOptions,
  onFinish: (failed: boolean) => void = () => {},
): ByteStream {
  const reader = pair.readable.getReader()
  const writer = pair.writable.getWriter()
  let inputBytes = 0
  let outputBytes = 0
  let finished = false
  let readDone = false
  let writeDone = false
  let readableController: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>
  let writableController: WritableStreamDefaultController
  let rejectPendingWrite: ((reason: unknown) => void) | undefined
  // A terminated worker may never settle an in-flight write or close.
  const waitForWrite = (pending: Promise<void>) =>
    new Promise<void>((resolve, reject) => {
      rejectPendingWrite = reject
      void pending.then(resolve, reject)
    }).finally(() => {
      rejectPendingWrite = undefined
    })
  const cleanup = (failed = false) => {
    if (finished) {
      return
    }
    finished = true
    options.signal?.removeEventListener('abort', abort)
    onFinish(failed)
  }
  const fail = (reason: unknown) => {
    if (finished) {
      return
    }
    readableController.error(reason)
    writableController.error(reason)
    rejectPendingWrite?.(reason)
    void reader.cancel(reason).catch(() => {})
    void writer.abort(reason).catch(() => {})
    cleanup(true)
  }
  const abort = () => fail(abortReason(options.signal))
  const complete = () => {
    if (readDone && writeDone) {
      cleanup()
    }
  }
  const readable = new ReadableStream<Uint8Array<ArrayBuffer>>(
    {
      start(controller) {
        readableController = controller
      },
      async pull(controller) {
        try {
          const { value, done } = await reader.read()
          if (finished) {
            return
          }
          if (done) {
            readDone = true
            controller.close()
            reader.releaseLock()
            complete()
            return
          }
          outputBytes += value.byteLength
          if (outputBytes > options.maxOutputBytes) {
            throw new RangeError('Output exceeds maxOutputBytes')
          }
          controller.enqueue(value)
        } catch (error) {
          fail(error)
        }
      },
      cancel(reason) {
        fail(reason ?? new DOMException('Compression stream cancelled', 'AbortError'))
      },
    },
    { highWaterMark: 0 },
  )
  const writable = new WritableStream<BinaryInput>(
    {
      start(controller) {
        writableController = controller
        // The sink's abort hook waits for writes. Let the stream register the
        // abort request, then release any write held by backpressure.
        controller.signal.addEventListener(
          'abort',
          () => {
            queueMicrotask(() =>
              fail(
                controller.signal.reason ??
                  new DOMException('Compression stream aborted', 'AbortError'),
              ),
            )
          },
          { once: true },
        )
      },
      async write(input) {
        try {
          const view = viewOf(input)
          inputBytes += view.byteLength
          if (inputBytes > options.maxInputBytes) {
            throw new RangeError('Input exceeds maxInputBytes')
          }
          // A queued write follows standard stream ownership: keep its bytes stable
          // until write() resolves. No caller buffer is ever detached here.
          await waitForWrite(writer.write(snapshot(view)))
        } catch (error) {
          fail(error)
          throw error
        }
      },
      async close() {
        try {
          await waitForWrite(writer.close())
          writeDone = true
          writer.releaseLock()
          complete()
        } catch (error) {
          fail(error)
          throw error
        }
      },
    },
    {
      highWaterMark: options.highWaterMarkBytes,
      // Validate in write(), where failure also tears down the readable/backend.
      size(input) {
        try {
          return viewOf(input).byteLength
        } catch {
          return 1
        }
      },
    },
  )
  // Observe errors even while neither side is being actively consumed.
  void writer.closed.catch(fail)
  void reader.closed.catch(fail)
  options.signal?.addEventListener('abort', abort, { once: true })
  if (options.signal?.aborted) {
    abort()
  }
  return { readable, writable }
}

export interface StepProcessor {
  readonly done: boolean
  start(input: Uint8Array<ArrayBufferLike>, final: boolean): void
  step(): Uint8Array<ArrayBuffer> | undefined
  process(
    input: Uint8Array<ArrayBufferLike>,
    final: boolean,
  ): Generator<Uint8Array<ArrayBuffer> | undefined>
  close(): void
}

const host = globalThis as typeof globalThis & { setImmediate?: (callback: () => void) => unknown }
export function yieldToHost(): Promise<void> {
  return new Promise((resolve) => {
    if (host.setImmediate) {
      host.setImmediate(resolve)
    } else if (typeof MessageChannel === 'undefined') {
      setTimeout(resolve, 0)
    } else {
      const { port1, port2 } = new MessageChannel()
      port1.onmessage = () => {
        port1.close()
        port2.close()
        resolve()
      }
      port2.postMessage(null)
    }
  })
}

/** One managed pair: advance synchronous codec steps only under output demand. */
export function processorStream(processor: StepProcessor, options: ParsedOptions): ByteStream {
  let demand = false
  let stopped = false
  let failed = false
  let failure: unknown
  let inputBytes = 0
  let outputBytes = 0
  let sliceStarted = performance.now()
  let waiting: { resolve(): void; reject(reason: unknown): void } | undefined
  let readableController: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>
  let writableController: WritableStreamDefaultController
  const stop = (reason?: unknown) => {
    if (stopped) {
      return
    }
    stopped = true
    options.signal?.removeEventListener('abort', abort)
    processor.close()
    waiting?.reject(reason)
    waiting = undefined
  }
  const fail = (reason: unknown) => {
    if (stopped) {
      return
    }
    failed = true
    failure = reason
    readableController.error(reason)
    writableController.error(reason)
    stop(reason)
  }
  const abort = () => fail(abortReason(options.signal))
  const checkOpen = () => {
    if (stopped) {
      throw failed ? failure : new Error('Compression stream closed')
    }
  }
  const process = async (input: Uint8Array<ArrayBufferLike>, final: boolean) => {
    try {
      checkOpen()
      for (const output of processor.process(input, final)) {
        if (output) {
          outputBytes += output.byteLength
          if (outputBytes > options.maxOutputBytes) {
            throw new RangeError('Output exceeds maxOutputBytes')
          }
          if (!demand) {
            await new Promise<void>((resolve, reject) => {
              waiting = { resolve, reject }
            })
          }
          checkOpen()
          demand = false
          readableController.enqueue(output)
        }
        if (performance.now() - sliceStarted >= 5) {
          await yieldToHost()
          sliceStarted = performance.now()
        }
        checkOpen()
      }
    } catch (error) {
      fail(error)
      throw failed ? failure : error
    }
  }
  const readable = new ReadableStream<Uint8Array<ArrayBuffer>>(
    {
      start(controller) {
        readableController = controller
      },
      pull() {
        demand = true
        waiting?.resolve()
        waiting = undefined
      },
      cancel(reason) {
        fail(reason ?? new DOMException('Compression stream cancelled', 'AbortError'))
      },
    },
    { highWaterMark: 1 },
  )
  const writable = new WritableStream<BinaryInput>(
    {
      start(controller) {
        writableController = controller
        controller.signal.addEventListener(
          'abort',
          () => {
            // Let WritableStream record its abort before erroring the controller.
            queueMicrotask(() => fail(controller.signal.reason))
          },
          { once: true },
        )
      },
      write(input) {
        try {
          checkOpen()
          const view = viewOf(input)
          inputBytes += view.byteLength
          if (inputBytes > options.maxInputBytes) {
            throw new RangeError('Input exceeds maxInputBytes')
          }
          return process(view, false)
        } catch (error) {
          fail(error)
          throw error
        }
      },
      async close() {
        await process(new Uint8Array(), true)
        readableController.close()
        stop()
      },
      abort(reason) {
        fail(reason)
      },
    },
    {
      highWaterMark: options.highWaterMarkBytes,
      size(input) {
        try {
          return viewOf(input).byteLength
        } catch {
          return 1
        }
      },
    },
  )
  options.signal?.addEventListener('abort', abort, { once: true })
  if (options.signal?.aborted) {
    abort()
  }
  return { readable, writable }
}

/** Source-bound path: the output pull advances the codec without a writable hop. */
export function processorReadable(
  source: ReadableStream<BinaryInput>,
  processor: StepProcessor,
  options: ParsedOptions,
): ReadableStream<Uint8Array<ArrayBuffer>> {
  let reader: ReadableStreamDefaultReader<BinaryInput>
  try {
    reader = source.getReader()
  } catch (error) {
    processor.close()
    throw error
  }
  let stopped = false
  let final = false
  let inputBytes = 0
  let outputBytes = 0
  let sliceStarted = 0
  let active = false
  let controller: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>
  const stop = () => {
    if (stopped) {
      return
    }
    stopped = true
    options.signal?.removeEventListener('abort', abort)
    active = false
    processor.close()
  }
  const cancelSource = (reason: unknown) => {
    stop()
    return reader
      .cancel(reason)
      .catch(() => {})
      .finally(() => reader.releaseLock())
  }
  const fail = (reason: unknown) => {
    if (stopped) {
      return
    }
    controller.error(reason)
    void cancelSource(reason)
  }
  const abort = () => fail(abortReason(options.signal))
  return new ReadableStream<Uint8Array<ArrayBuffer>>(
    {
      start(value) {
        controller = value
        options.signal?.addEventListener('abort', abort, { once: true })
        if (options.signal?.aborted) {
          abort()
        }
      },
      async pull() {
        try {
          while (!stopped) {
            if (sliceStarted && performance.now() - sliceStarted >= 5) {
              await yieldToHost()
              sliceStarted = performance.now()
              if (stopped) {
                return
              }
            }
            if (active) {
              if (!processor.done) {
                const output = processor.step()
                if (!output) {
                  continue
                }
                outputBytes += output.byteLength
                if (outputBytes > options.maxOutputBytes) {
                  throw new RangeError('Output exceeds maxOutputBytes')
                }
                controller.enqueue(output)
                return
              }
              active = false
              if (final) {
                controller.close()
                stop()
                reader.releaseLock()
                return
              }
            }
            const next = await reader.read()
            if (stopped) {
              return
            }
            final = next.done
            const input = next.done ? new Uint8Array() : viewOf(next.value)
            inputBytes += input.byteLength
            if (inputBytes > options.maxInputBytes) {
              throw new RangeError('Input exceeds maxInputBytes')
            }
            processor.start(input, final)
            if (!sliceStarted) {
              sliceStarted = performance.now()
            }
            active = true
          }
        } catch (error) {
          fail(error)
        }
      },
      cancel(reason) {
        return cancelSource(reason)
      },
    },
    { highWaterMark: options.highWaterMarkBytes, size: (chunk) => chunk.byteLength },
  )
}
