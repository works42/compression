import {
  joinChunks,
  snapshot,
  viewOf,
  type BinaryInput,
  type Operation,
  type ParsedOptions,
} from './options.js'
import { yieldToHost } from './flow.js'
import { loadZstdModule, type ZstdModule } from './zstd-module.js'
import { ZstdProcessor, compressionLevel, includeContentSize } from './zstd-processor.js'
import { KnownBufferOutput } from './buffer-output.js'

const chunkBytes = 65536

function bufferOutput(
  module: ZstdModule,
  operation: Operation,
  input: Uint8Array<ArrayBufferLike>,
  options: ParsedOptions,
) {
  if (operation !== 'decompress') {
    return
  }
  const prefix = input.subarray(0, 18)
  const pointer = module._codec_buffer_input()
  module.HEAPU8.set(prefix, pointer)
  const knownSize = module._codec_frame_content_size(pointer, prefix.byteLength)
  if (
    Number.isSafeInteger(knownSize) &&
    knownSize >= 1024 * 1024 &&
    knownSize <= Math.min(options.maxOutputBytes, 256 * 1024 * 1024)
  ) {
    return new KnownBufferOutput(knownSize, options.maxOutputBytes)
  }
}

/** One bounded synchronous call. Larger/complex frames use the processor below. */
function smallBuffer(
  module: ZstdModule,
  operation: Operation,
  input: Uint8Array<ArrayBufferLike>,
  options: ParsedOptions,
): Uint8Array<ArrayBuffer> | undefined {
  const inputCapacity = operation === 'compress' ? module._codec_buffer_capacity() : chunkBytes
  if (input.byteLength > inputCapacity) {
    return
  }
  const level = options.level ?? 3
  const source = module._codec_buffer_input()
  const output = module._codec_buffer_output()
  let dictionary = 0
  try {
    module.HEAPU8.set(input, source)
    let capacity: number
    if (operation === 'compress') {
      capacity = module._codec_buffer_bound(input.byteLength)
    } else {
      const size = module._codec_buffer_size(source, input.byteLength, options.maxWindowLog ?? 27)
      if (size === -2) {
        return
      }
      if (size >= 0 && size > options.maxOutputBytes) {
        throw new RangeError('Output exceeds maxOutputBytes')
      }
      capacity = Math.min(size < 0 ? chunkBytes : size, options.maxOutputBytes)
    }
    const dictionarySize = options.dictionary?.byteLength ?? 0
    if (dictionarySize) {
      dictionary = module._malloc(dictionarySize)
      if (!dictionary) {
        throw new Error('Zstd dictionary allocation failed')
      }
      module.HEAPU8.set(options.dictionary!, dictionary)
    }
    const size =
      operation === 'compress'
        ? module._codec_buffer_compress(
            output,
            capacity,
            source,
            input.byteLength,
            level,
            Number(options.checksum ?? true),
            Number(includeContentSize(options, input.byteLength)),
            dictionary,
            dictionarySize,
          )
        : module._codec_buffer_decompress(
            output,
            capacity,
            source,
            input.byteLength,
            dictionary,
            dictionarySize,
          )
    if (size === -2) {
      return
    } // An unknown-size frame may expand beyond the bounded output buffer.
    if (size < 0) {
      throw new Error(module.UTF8ToString(module._codec_error()))
    }
    if (size > options.maxOutputBytes) {
      throw new RangeError('Output exceeds maxOutputBytes')
    }
    return module.HEAPU8.slice(output, output + size)
  } finally {
    if (dictionary) {
      module._free(dictionary)
    }
  }
}

/** Own asynchronous inputs immediately, and attempt the bounded kernel once. */
export function runZstdBuffer(
  operation: Operation,
  input: Uint8Array<ArrayBufferLike>,
  options: ParsedOptions,
  transferInput: boolean,
): Promise<Uint8Array<ArrayBuffer>> {
  try {
    const transferred = transferInput ? snapshot(input, true) : undefined
    const source = transferred ?? input
    const module = loadZstdModule(options)
    if (!(module instanceof Promise)) {
      if (operation === 'compress') {
        compressionLevel(module, options)
      }
      // Allocation can grow/detach an input that aliases this module's heap.
      const fast =
        source.buffer === module.HEAPU8.buffer
          ? undefined
          : smallBuffer(module, operation, source, options)
      if (fast !== undefined) {
        return options.signal
          ? Promise.resolve().then(() => {
              options.signal!.throwIfAborted()
              return fast
            })
          : Promise.resolve(fast)
      }
    }
    return collectZstdBuffer(module, operation, transferred ?? new Uint8Array(source), options)
  } catch (error) {
    return options.signal
      ? Promise.resolve().then(() => {
          options.signal!.throwIfAborted()
          throw error
        })
      : Promise.reject(error)
  }
}

async function collectZstdBuffer(
  loading: ZstdModule | Promise<ZstdModule>,
  operation: Operation,
  input: Uint8Array<ArrayBuffer>,
  options: ParsedOptions,
): Promise<Uint8Array<ArrayBuffer>> {
  const module = await loading
  options.signal?.throwIfAborted()
  // Ready modules already tried the bounded kernel before this handoff.
  if (loading instanceof Promise) {
    if (operation === 'compress') {
      compressionLevel(module, options)
    }
    const fast = smallBuffer(module, operation, input, options)
    if (fast !== undefined) {
      return fast
    }
  }
  const output = bufferOutput(module, operation, input, options)
  const processor = new ZstdProcessor(
    module,
    operation,
    options,
    operation === 'compress' ? input.byteLength : undefined,
    true,
    !output,
  )
  const chunks: Uint8Array<ArrayBuffer>[] = []
  let size = 0
  let sliceStarted = performance.now()
  try {
    for (const outputs of [
      processor.process(input, false),
      processor.process(new Uint8Array(), true),
    ]) {
      for (const chunk of outputs) {
        if (chunk) {
          size += chunk.byteLength
          if (size > options.maxOutputBytes) {
            throw new RangeError('Output exceeds maxOutputBytes')
          }
          if (output) {
            output.append(chunk)
          } else {
            chunks.push(chunk)
          }
        }
        if (performance.now() - sliceStarted >= 5) {
          await yieldToHost()
          sliceStarted = performance.now()
        }
      }
    }
    options.signal?.throwIfAborted()
    return output ? output.finish() : joinChunks(chunks, size)
  } finally {
    output?.clear()
    processor.close()
  }
}

export async function prepareZstdBuffers(compression: ParsedOptions, decompression: ParsedOptions) {
  const module = await loadZstdModule(compression)
  compression.signal?.throwIfAborted()
  compression.level = compressionLevel(module, compression)
  function run(
    operation: Operation,
    input: BinaryInput,
    options: ParsedOptions,
  ): Uint8Array<ArrayBuffer> {
    options.signal?.throwIfAborted()
    const supplied = viewOf(input)
    // WASM allocation can grow/detach its old heap, including caller views.
    const view = supplied.buffer === module.HEAPU8.buffer ? new Uint8Array(supplied) : supplied
    if (view.byteLength > options.maxInputBytes) {
      throw new RangeError('Input exceeds maxInputBytes')
    }
    if (
      operation === 'compress' &&
      typeof options.contentSize === 'number' &&
      options.contentSize !== view.byteLength
    ) {
      throw new RangeError('Input length does not match contentSize')
    }
    const fast = smallBuffer(module, operation, view, options)
    if (fast !== undefined) {
      return fast
    }
    const output = bufferOutput(module, operation, view, options)
    const processor = new ZstdProcessor(
      module,
      operation,
      options,
      operation === 'compress' ? view.byteLength : undefined,
      true,
      !output,
    )
    const chunks: Uint8Array<ArrayBuffer>[] = []
    let size = 0
    try {
      for (const outputs of [
        processor.process(view, false),
        processor.process(new Uint8Array(), true),
      ]) {
        for (const chunk of outputs) {
          if (!chunk) {
            continue
          }
          size += chunk.byteLength
          if (size > options.maxOutputBytes) {
            throw new RangeError('Output exceeds maxOutputBytes')
          }
          if (output) {
            output.append(chunk)
          } else {
            chunks.push(chunk)
          }
        }
      }
      return output ? output.finish() : joinChunks(chunks, size)
    } finally {
      output?.clear()
      processor.close()
    }
  }
  return {
    compress: (input: BinaryInput) => run('compress', input, compression),
    decompress: (input: BinaryInput) => run('decompress', input, decompression),
  }
}
