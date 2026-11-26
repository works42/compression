import { manageStream, processorStream } from './flow.js'
import type { ByteStream, CompressionFormat, Operation, ParsedOptions } from './options.js'
import { createZstdStepProcessor } from './zstd-processor.js'

export async function createDirectStream(
  operation: Operation,
  options: ParsedOptions,
): Promise<ByteStream> {
  options.signal?.throwIfAborted()
  if (options.format === 'zstd') {
    return processorStream(await createZstdStepProcessor(operation, options), options)
  }
  return manageStream(createNativeStream(operation, options.format), options)
}

/** Raw native backend; its caller owns limits, input snapshots and teardown. */
export function createNativeStream(
  operation: Operation,
  format: Exclude<CompressionFormat, 'zstd'>,
): ByteStream {
  let pair: ByteStream
  const Constructor =
    operation === 'compress' ? globalThis.CompressionStream : globalThis.DecompressionStream
  try {
    pair = new Constructor(format)
  } catch {
    throw new DOMException(
      `Compression format ${format} is not supported in this environment`,
      'NotSupportedError',
    )
  }
  return { readable: pair.readable, writable: pair.writable }
}
