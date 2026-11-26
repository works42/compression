import { processorReadable, type StepProcessor } from './flow.js'
import type { BinaryInput, Operation, ParsedOptions } from './options.js'
import { loadZstdModule, type ZstdModule } from './zstd-module.js'

export function compressionLevel(module: ZstdModule, options: ParsedOptions): number {
  const level = options.level ?? 3
  if (level < module._codec_min_level() || level > module._codec_max_level()) {
    throw new RangeError(
      `Zstd level must be ${module._codec_min_level()} through ${module._codec_max_level()}`,
    )
  }
  return level
}

export function includeContentSize(options: ParsedOptions, inputSize: number): boolean {
  return (
    options.contentSize !== false &&
    (options.contentSize === true || typeof options.contentSize === 'number' || inputSize === 0)
  )
}

export class ZstdProcessor implements StepProcessor {
  done = true
  private handle = 0
  private inputPointer: number
  private outputPointer: number
  private inputCapacity: number
  private outputCapacity: number
  private inputBytes = 0
  private lastStatus = 1
  private input: Uint8Array<ArrayBufferLike> = new Uint8Array()
  private final = false
  private offset = 0
  private stagedFrom = 0
  private stagedEnd = 0

  constructor(
    private module: ZstdModule,
    private operation: Operation,
    private options: ParsedOptions,
    inputSize?: number,
    private reusable = false,
    private copyOutput = true,
  ) {
    options.signal?.throwIfAborted()
    const level = compressionLevel(module, options)
    let dictionary = 0
    try {
      const bytes = options.dictionary?.byteLength ?? 0
      if (bytes) {
        dictionary = module._malloc(bytes)
        if (!dictionary) {
          throw new Error('Zstd dictionary allocation failed')
        }
        module.HEAPU8.set(options.dictionary!, dictionary)
      }
      const create = reusable ? module._codec_buffer_acquire : module._codec_create
      this.handle = create(
        Number(operation === 'compress'),
        level,
        Number(options.checksum ?? true),
        options.maxWindowLog ?? 27,
        dictionary,
        bytes,
      )
      if (!this.handle) {
        throw this.error()
      }
      if (
        operation === 'compress' &&
        (inputSize !== undefined || options.contentSize !== undefined)
      ) {
        const pledged =
          inputSize ?? (typeof options.contentSize === 'number' ? options.contentSize : -1)
        const includeSize =
          inputSize === undefined
            ? options.contentSize !== false
            : includeContentSize(options, inputSize)
        if (module._codec_set_pledged_size(this.handle, pledged, Number(includeSize)) < 0) {
          throw this.error()
        }
      }
      this.inputCapacity = module._codec_input_capacity()
      this.outputCapacity = Math.min(
        module._codec_output_capacity(this.handle),
        options.maxOutputBytes + 1,
      )
      this.inputPointer = module._codec_input(this.handle)
      this.outputPointer = module._codec_output(this.handle)
    } catch (error) {
      this.close()
      throw error
    } finally {
      if (dictionary) {
        module._free(dictionary)
      }
    }
  }

  private error() {
    return new Error(this.module.UTF8ToString(this.module._codec_error()))
  }

  close() {
    if (!this.handle) {
      return
    }
    if (this.reusable) {
      this.module._codec_buffer_release(this.handle)
    } else {
      this.module._codec_free(this.handle)
    }
    this.handle = 0
    this.input = new Uint8Array()
  }

  start(input: Uint8Array<ArrayBufferLike>, final: boolean) {
    try {
      if (!this.handle) {
        throw new Error('Zstd stream closed')
      }
      if (this.operation === 'decompress' && final) {
        if (this.lastStatus !== 0) {
          throw new Error('Truncated Zstd stream')
        }
        this.done = true
        return
      }
      if (this.operation === 'compress' && typeof this.options.contentSize === 'number') {
        this.inputBytes += input.byteLength
        if (
          this.inputBytes > this.options.contentSize ||
          (final && this.inputBytes !== this.options.contentSize)
        ) {
          throw new RangeError('Input length does not match contentSize')
        }
      }
      this.final = final
      this.offset = this.stagedFrom = this.stagedEnd = 0
      this.done = !input.byteLength && !final
      this.input = this.done ? new Uint8Array() : input
    } catch (error) {
      this.close()
      throw error
    }
  }

  step(): Uint8Array<ArrayBuffer> | undefined {
    if (this.done) {
      return
    }
    try {
      if (!this.handle) {
        throw new Error('Zstd stream closed')
      }
      this.options.signal?.throwIfAborted()
      const module = this.module
      const input = this.input
      if (this.offset === this.stagedEnd) {
        this.stagedFrom = this.offset
        this.stagedEnd = Math.min(this.offset + this.inputCapacity, input.byteLength)
        if (this.stagedEnd > this.offset) {
          module.HEAPU8.set(input.subarray(this.offset, this.stagedEnd), this.inputPointer)
        }
      }
      const status = module._codec_process_at(
        this.handle,
        this.stagedEnd - this.offset,
        Number(this.final),
        this.offset - this.stagedFrom,
        this.outputCapacity,
      )
      if (status < 0) {
        throw this.error()
      }
      const consumed = module._codec_consumed(this.handle)
      const produced = module._codec_produced(this.handle)
      this.lastStatus = status
      this.offset += consumed
      const drain =
        this.operation === 'decompress' && produced === this.outputCapacity && status !== 0
      this.done =
        this.final && this.operation === 'compress'
          ? status === 0
          : this.offset === input.byteLength && !drain
      if (this.done) {
        this.input = new Uint8Array()
      }
      if (!consumed && !produced && this.offset < input.byteLength) {
        throw new Error('Zstd made no progress')
      }
      if (!produced) {
        return
      }
      const output = module.HEAPU8.subarray(this.outputPointer, this.outputPointer + produced)
      // Buffer collectors copy this borrowed view before another codec step or yield.
      return this.copyOutput ? new Uint8Array(output) : output
    } catch (error) {
      this.close()
      throw error
    }
  }

  *process(input: Uint8Array<ArrayBufferLike>, final: boolean) {
    this.start(input, final)
    while (!this.done) {
      yield this.step()
    }
  }
}

export async function createZstdReadable(
  source: ReadableStream<BinaryInput>,
  operation: Operation,
  options: ParsedOptions,
) {
  return processorReadable(source, await createZstdStepProcessor(operation, options), options)
}

export async function createZstdStepProcessor(
  operation: Operation,
  options: ParsedOptions,
): Promise<StepProcessor> {
  return new ZstdProcessor(await loadZstdModule(options), operation, options)
}
