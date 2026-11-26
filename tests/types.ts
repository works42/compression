import {
  compressStream,
  decompressStream,
  createZstdCodec,
  createWorkerPool,
  WorkerPoolCapacityError,
  compress,
  decompress,
  createCompressionStream,
  createDecompressionStream,
  type CompressionOptions,
  type ZstdCodec,
  type ZstdCodecOptions,
} from '../src/index.js'
async function checkTypes() {
  const input = new Uint8Array(0)
  const readable: ReadableStream<Uint8Array<ArrayBuffer>> = await compressStream(
    new Blob([input]).stream(),
    { format: 'zstd' },
  )
  await decompressStream(readable, { format: 'zstd' })
  // @ts-expect-error The source must be a readable stream.
  await compressStream(input, { format: 'zstd' })
  const preparedOptions: ZstdCodecOptions = { level: 3, contentSize: true, maxWindowLog: 27 }
  const prepared: ZstdCodec = await createZstdCodec(preparedOptions)
  const frame: Uint8Array<ArrayBuffer> = prepared.compress(input)
  const plain: Uint8Array<ArrayBuffer> = prepared.decompress(new DataView(frame.buffer))
  void plain
  // @ts-expect-error Prepared calls return bytes synchronously.
  const pending: Promise<Uint8Array<ArrayBuffer>> = prepared.compress(input)
  void pending
  // @ts-expect-error The prepared codec is Zstd only and takes no format selector.
  await createZstdCodec({ format: 'gzip' })
  // @ts-expect-error Prepared calls execute synchronously on the calling thread.
  await createZstdCodec({ execution: 'worker' })
  await compress(input, { format: 'zstd', level: 10 })
  await compress(input.buffer, { format: 'gzip' })
  await compress(new DataView(input.buffer), { format: 'deflate-raw' })
  await decompress(input, { format: 'zstd', maxWindowLog: 27 })
  await createCompressionStream({ format: 'zstd', dictionary: input })
  await createDecompressionStream({ format: 'gzip', maxOutputBytes: 100 })
  const options: CompressionOptions = { format: 'deflate' }
  await compress(input, options)
  await compress(input, {
    format: 'gzip',
    execution: 'worker',
    workerFactory: () => new Worker('worker.js'),
  })
  const pool = createWorkerPool({
    size: 2,
    maxQueuedOperations: 8,
    maxPendingBytes: 32 * 1024 ** 2,
  })
  const capacityError = new WorkerPoolCapacityError('maxPendingBytes')
  const limit: 'maxPendingBytes' | 'maxQueuedOperations' = capacityError.limit
  void limit
  await compress(input, {
    format: 'zstd',
    contentSize: true,
    execution: 'worker',
    workerPool: pool,
  })
  await createCompressionStream({ format: 'zstd', contentSize: input.byteLength })
  await pool.close()
  // @ts-expect-error Decompression cannot write a content size.
  await decompress(input, { format: 'zstd', contentSize: true })
  // @ts-expect-error Native formats cannot write a Zstd content size.
  await compress(input, { format: 'gzip', contentSize: true })
  // @ts-expect-error A pool must be created by createWorkerPool.
  await compress(input, { format: 'zstd', workerPool: {} })
  // @ts-expect-error Native formats do not expose a compression level.
  await compress(input, { format: 'gzip', level: 1 })
  // @ts-expect-error Unknown formats are rejected.
  await compress(input, { format: 'zip' })
  // @ts-expect-error Format is explicit.
  await compress(input, {})
  // @ts-expect-error Text encoding belongs to the caller.
  await compress('text', { format: 'gzip' })
  // @ts-expect-error Streams preserve ownership; transferInput is one-shot only.
  await createCompressionStream({ format: 'zstd', transferInput: true })
  // @ts-expect-error A decompressor has no compression level.
  await decompress(input, { format: 'zstd', level: 3 })
}

void checkTypes
