import { joinChunks, parseOptions, snapshot, viewOf } from './options.js'
import type {
  BinaryInput,
  ByteStream,
  CompressionOptions,
  DecompressionOptions,
  InputOptions,
  Operation,
  Options,
  ParsedOptions,
} from './options.js'
import { createDirectStream } from './direct.js'
import { createWorkerStream } from './worker-client.js'
import { runZstdBuffer } from './zstd.js'
import { createZstdReadable } from './zstd-processor.js'
import { reserveWorker, type WorkerReservation } from './worker-pool.js'

export {
  createWorkerPool,
  WorkerPoolCapacityError,
  type CompressionWorkerPool,
  type WorkerPoolOptions,
} from './worker-pool.js'
export { createZstdCodec, type ZstdCodec, type ZstdCodecOptions } from './codec.js'

export {
  formats,
  type CompressionFormat,
  type BinaryInput,
  type ByteStream,
  type CompressionOptions,
  type DecompressionOptions,
  type InputOptions,
  type CompressionWorker,
} from './options.js'

function ownedOptions(
  operation: Operation,
  supplied: unknown,
  inputSize?: number,
  buffer = false,
  retainedInputBytes = 0,
): { options: ParsedOptions; reservation?: WorkerReservation } {
  const options = parseOptions(operation, supplied, inputSize, buffer)
  options.signal?.throwIfAborted()
  if (inputSize !== undefined && inputSize > options.maxInputBytes) {
    throw new RangeError('Input exceeds maxInputBytes')
  }
  const reservation = options.workerPool
    ? reserveWorker(options.workerPool, retainedInputBytes + (options.dictionary?.byteLength ?? 0))
    : undefined
  try {
    // Admission must precede dictionary copies, including dictionaries aliasing input.
    if (options.dictionary) {
      options.dictionary = snapshot(options.dictionary)
    }
    return { options, reservation }
  } catch (error) {
    reservation?.release()
    throw error
  }
}

function createStream(
  operation: Operation,
  options: ParsedOptions,
  reservation?: WorkerReservation,
): Promise<ByteStream> {
  return options.execution === 'worker'
    ? createWorkerStream(operation, options, reservation?.release).catch((error) => {
        reservation?.release()
        throw error
      })
    : createDirectStream(operation, options)
}

export async function createCompressionStream(options: CompressionOptions): Promise<ByteStream> {
  const owned = ownedOptions('compress', options)
  return createStream('compress', owned.options, owned.reservation)
}

export async function createDecompressionStream(
  options: DecompressionOptions,
): Promise<ByteStream> {
  const owned = ownedOptions('decompress', options)
  return createStream('decompress', owned.options, owned.reservation)
}

async function collect(
  operation: Operation,
  input: Uint8Array<ArrayBuffer>,
  options: ParsedOptions,
): Promise<Uint8Array<ArrayBuffer>> {
  const pair = await createStream(operation, options)
  const reader = pair.readable.getReader()
  const writer = pair.writable.getWriter()
  // Feed bounded slices even when the caller supplies one very large buffer.
  const writing = (async () => {
    try {
      for (let offset = 0; offset < input.byteLength; offset += 65536) {
        await writer.write(input.subarray(offset, offset + 65536))
      }
      await writer.close()
    } catch (error) {
      await reader.cancel(error).catch(() => {})
      throw error
    } finally {
      writer.releaseLock()
    }
  })()
  void writing.catch(() => {})
  const chunks: Uint8Array<ArrayBuffer>[] = []
  let size = 0
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) {
        break
      }
      chunks.push(value)
      size += value.byteLength
    }
    await writing
    return joinChunks(chunks, size)
  } catch (error) {
    await Promise.allSettled([reader.cancel(error), writer.abort(error), writing])
    throw error
  } finally {
    reader.releaseLock()
  }
}

function run(
  operation: Operation,
  input: BinaryInput,
  options: Options & InputOptions,
): Promise<Uint8Array<ArrayBuffer>> {
  let reservation: WorkerReservation | undefined
  try {
    const { transferInput = false } = options
    if (typeof transferInput !== 'boolean') {
      throw new TypeError('transferInput must be a boolean')
    }
    const view = viewOf(input)
    const owned = ownedOptions(
      operation,
      options,
      view.byteLength,
      true,
      transferInput ? view.buffer.byteLength : view.byteLength,
    )
    const parsed = owned.options
    reservation = owned.reservation
    if (parsed.format === 'zstd' && parsed.execution === 'direct') {
      return runZstdBuffer(operation, view, parsed, transferInput)
    }
    const pending = collect(operation, snapshot(view, transferInput), parsed)
    // Buffer snapshots remain live until collection settles, even after the worker is released.
    return reservation ? pending.finally(reservation.release) : pending
  } catch (error) {
    reservation?.release()
    return Promise.reject(error)
  }
}

export function compress(
  input: BinaryInput,
  options: CompressionOptions & InputOptions,
): Promise<Uint8Array<ArrayBuffer>> {
  return run('compress', input, options)
}

export function decompress(
  input: BinaryInput,
  options: DecompressionOptions & InputOptions,
): Promise<Uint8Array<ArrayBuffer>> {
  return run('decompress', input, options)
}

async function transformSource(
  operation: Operation,
  source: ReadableStream<BinaryInput>,
  options: Options,
): Promise<ReadableStream<Uint8Array<ArrayBuffer>>> {
  if (!(source instanceof ReadableStream) || source.locked) {
    throw new TypeError('Expected an unlocked ReadableStream')
  }
  const owned = ownedOptions(operation, options)
  const parsed = owned.options
  if (parsed.format === 'zstd' && parsed.execution === 'direct') {
    return createZstdReadable(source, operation, parsed)
  }
  const pair = await createStream(operation, parsed, owned.reservation)
  try {
    return source.pipeThrough(pair)
  } catch (error) {
    await Promise.allSettled([pair.readable.cancel(error), pair.writable.abort(error)])
    throw error
  }
}

export function compressStream(
  source: ReadableStream<BinaryInput>,
  options: CompressionOptions,
): Promise<ReadableStream<Uint8Array<ArrayBuffer>>> {
  return transformSource('compress', source, options)
}

export function decompressStream(
  source: ReadableStream<BinaryInput>,
  options: DecompressionOptions,
): Promise<ReadableStream<Uint8Array<ArrayBuffer>>> {
  return transformSource('decompress', source, options)
}
