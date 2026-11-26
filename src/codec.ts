import {
  isRecord,
  parseOptions,
  snapshot,
  type BinaryInput,
  type CompressionOptions,
  type DecompressionOptions,
} from './options.js'
import { prepareZstdBuffers } from './zstd.js'

/** Fixed settings for synchronous, in-memory Zstd operations. */
export type ZstdCodecOptions = Omit<
  Extract<CompressionOptions, { format: 'zstd' }>,
  'format' | 'execution' | 'workerFactory' | 'workerPool' | 'highWaterMarkBytes'
> & {
  maxWindowLog?: Extract<DecompressionOptions, { format: 'zstd' }>['maxWindowLog']
}

export interface ZstdCodec {
  compress(input: BinaryInput): Uint8Array<ArrayBuffer>
  decompress(input: BinaryInput): Uint8Array<ArrayBuffer>
}

/** Load WASM and snapshot settings once; subsequent calls complete synchronously. */
export async function createZstdCodec(options: ZstdCodecOptions = {}): Promise<ZstdCodec> {
  if (!isRecord(options)) {
    throw new TypeError('Expected a Zstd codec options object')
  }
  const { level, checksum, contentSize, maxWindowLog, ...common } = options
  // Reuse direction-specific validation, resolving `true` per call below.
  const compression = parseOptions('compress', {
    ...common,
    format: 'zstd',
    level,
    checksum,
    contentSize: contentSize === true ? undefined : contentSize,
  })
  const decompression = parseOptions('decompress', { ...common, format: 'zstd', maxWindowLog })
  for (const key of ['format', 'execution', 'workerFactory', 'workerPool', 'highWaterMarkBytes']) {
    if (key in options) {
      throw new TypeError(`Unexpected Zstd codec option: ${key}`)
    }
  }
  compression.contentSize = contentSize
  // Capture dictionary bytes before awaiting initialization, once per codec.
  if (compression.dictionary) {
    compression.dictionary = snapshot(compression.dictionary)
  }
  decompression.dictionary = compression.dictionary
  return prepareZstdBuffers(compression, decompression)
}
