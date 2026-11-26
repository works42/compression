import { isWorkerPool, type CompressionWorkerPool } from './worker-pool.js'

export const formats = ['zstd', 'gzip', 'deflate', 'deflate-raw'] as const
export type CompressionFormat = (typeof formats)[number]

export type BinaryInput = ArrayBuffer | ArrayBufferView<ArrayBufferLike>

export type ByteStream = ReadableWritablePair<Uint8Array<ArrayBuffer>, BinaryInput>

/** @internal */
export type Operation = 'compress' | 'decompress'

/** A worker owned by one operation, or by an explicit worker pool. */
export interface CompressionWorker {
  postMessage(message: unknown, transfer?: Transferable[]): void
  terminate(): unknown
  addEventListener?(event: string, listener: (event: MessageEvent | ErrorEvent) => void): void
  removeEventListener?(event: string, listener: (event: MessageEvent | ErrorEvent) => void): void
  on?(event: string, listener: (value: unknown) => void): unknown
  off?(event: string, listener: (value: unknown) => void): unknown
}

interface CommonOptions {
  signal?: AbortSignal
  /** Total input/output bytes, independent of internal chunk sizes. Default: Infinity. */
  maxInputBytes?: number
  maxOutputBytes?: number
  /** Writable queue backpressure threshold in bytes. Default: 64 KiB. */
  highWaterMarkBytes?: number
  initializationTimeoutMs?: number
  execution?: 'direct' | 'worker'
  workerFactory?: () => CompressionWorker
  /** Explicit reusable workers; requires worker execution and excludes workerFactory. */
  workerPool?: CompressionWorkerPool
}

interface ZstdAssets {
  loaderUrl?: string | URL
  wasmUrl?: string | URL
  dictionary?: Uint8Array<ArrayBufferLike>
}

type NativeOptions = {
  format: Exclude<CompressionFormat, 'zstd'>
  level?: never
  contentSize?: never
  checksum?: never
  dictionary?: never
  maxWindowLog?: never
  loaderUrl?: never
  wasmUrl?: never
}

export type CompressionOptions = CommonOptions &
  (
    | NativeOptions
    | (ZstdAssets & {
        format: 'zstd'
        level?: number
        checksum?: boolean
        contentSize?: boolean | number
      })
  )

export type DecompressionOptions = CommonOptions &
  (NativeOptions | (ZstdAssets & { format: 'zstd'; maxWindowLog?: number }))

export type InputOptions = { transferInput?: boolean }

/** @internal */
export type Options = CompressionOptions | DecompressionOptions

/** @internal */
export type ParsedOptions = CommonOptions &
  ZstdAssets & {
    format: CompressionFormat
    level?: number
    checksum?: boolean
    contentSize?: boolean | number
    maxWindowLog?: number
    maxInputBytes: number
    maxOutputBytes: number
    highWaterMarkBytes: number
    initializationTimeoutMs: number
    execution: 'direct' | 'worker'
  }

const supportedFormats = [...formats]

/** @internal */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** @internal */
export function hasOnlyKeys(record: Record<string, unknown>, ...keys: string[]): boolean {
  for (const key in record) {
    if (!keys.includes(key)) {
      return false
    }
  }
  return true
}

function integerOption(
  name: string,
  value: unknown,
  min: number,
  max = Number.MAX_SAFE_INTEGER,
): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new TypeError(`${name} must be a safe integer between ${min} and ${max}`)
  }
  return value
}

function byteLimit(name: string, value: unknown): number {
  if (
    value === Infinity ||
    (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0)
  ) {
    return value
  }
  throw new TypeError(`${name} must be a nonnegative safe integer or Infinity`)
}

function assetUrl(name: string, value: unknown): string | undefined {
  if (value instanceof URL) {
    return value.href
  }
  if (value === undefined || (typeof value === 'string' && value.length > 0)) {
    return value
  }
  throw new TypeError(`${name} must be a nonempty string or URL`)
}

/** @internal */
export function parseOptions(
  operation: Operation,
  options: unknown,
  inputSize?: number,
  buffer = false,
): ParsedOptions {
  if (!isRecord(options)) {
    throw new TypeError('Expected a compression options object')
  }
  const {
    format: requestedFormat,
    signal,
    execution = 'direct',
    workerFactory,
    workerPool,
    maxInputBytes = Infinity,
    maxOutputBytes = Infinity,
    highWaterMarkBytes = 65536,
    initializationTimeoutMs = 15000,
  } = options
  const format = supportedFormats.find((value) => value === requestedFormat)
  if (format === undefined) {
    throw new TypeError('format must be zstd, gzip, deflate, or deflate-raw')
  }
  if (signal !== undefined && !(signal instanceof AbortSignal)) {
    throw new TypeError('signal must be an AbortSignal')
  }
  if (execution !== 'direct' && execution !== 'worker') {
    throw new TypeError('execution must be direct or worker')
  }
  if (workerFactory !== undefined && typeof workerFactory !== 'function') {
    throw new TypeError('workerFactory must be a function')
  }
  if (workerFactory && execution !== 'worker') {
    throw new TypeError('workerFactory requires execution: worker')
  }

  if (workerPool !== undefined && !isWorkerPool(workerPool)) {
    throw new TypeError('workerPool must be created by createWorkerPool')
  }
  if (workerPool && execution !== 'worker') {
    throw new TypeError('workerPool requires execution: worker')
  }
  if (workerPool && workerFactory) {
    throw new TypeError('workerPool and workerFactory are mutually exclusive')
  }

  const parsed: ParsedOptions = {
    format,
    signal,
    execution,
    workerPool,
    workerFactory: workerFactory as ParsedOptions['workerFactory'],
    maxInputBytes: byteLimit('maxInputBytes', maxInputBytes),
    maxOutputBytes: byteLimit('maxOutputBytes', maxOutputBytes),
    highWaterMarkBytes: integerOption('highWaterMarkBytes', highWaterMarkBytes, 1),
    initializationTimeoutMs: integerOption(
      'initializationTimeoutMs',
      initializationTimeoutMs,
      1,
      2_147_483_647,
    ),
  }
  if (format === 'zstd') {
    const { dictionary, loaderUrl, wasmUrl } = options
    if (dictionary !== undefined && !(dictionary instanceof Uint8Array)) {
      throw new TypeError('dictionary must be a Uint8Array')
    }
    parsed.dictionary = dictionary
    parsed.loaderUrl = assetUrl('loaderUrl', loaderUrl)
    parsed.wasmUrl = assetUrl('wasmUrl', wasmUrl)
    if (operation === 'compress') {
      const { level, checksum, contentSize } = options
      if (checksum !== undefined && typeof checksum !== 'boolean') {
        throw new TypeError('checksum must be a boolean')
      }
      parsed.level =
        level === undefined ? undefined : integerOption('level', level, Number.MIN_SAFE_INTEGER)
      parsed.checksum = checksum
      if (contentSize === true) {
        if (inputSize === undefined) {
          throw new TypeError(
            'contentSize: true requires compress(); streams require a numeric contentSize',
          )
        }
        parsed.contentSize = inputSize
      } else if (contentSize === undefined || contentSize === false) {
        parsed.contentSize = contentSize
      } else {
        parsed.contentSize = integerOption('contentSize', contentSize, 0)
      }
      if (
        typeof parsed.contentSize === 'number' &&
        inputSize !== undefined &&
        parsed.contentSize !== inputSize
      ) {
        throw new RangeError('Input length does not match contentSize')
      }
    } else {
      const { maxWindowLog } = options
      parsed.maxWindowLog =
        maxWindowLog === undefined ? undefined : integerOption('maxWindowLog', maxWindowLog, 10, 30)
    }
  }
  for (const key in options) {
    if (buffer && key === 'transferInput') {
      continue
    }
    if (!Object.hasOwn(parsed, key)) {
      throw new TypeError(`Unexpected compression option: ${key}`)
    }
  }
  return parsed
}

/** @internal */
export function abortReason(signal?: AbortSignal): unknown {
  return signal?.aborted
    ? signal.reason
    : new DOMException('Compression operation aborted', 'AbortError')
}

/** @internal */
export function viewOf(input: BinaryInput): Uint8Array<ArrayBufferLike> {
  // Both constructors reject detached storage, including zero-length views.
  if (input instanceof ArrayBuffer) {
    return new Uint8Array(input)
  }
  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength)
  }
  throw new TypeError('Expected an ArrayBuffer or an ArrayBuffer view')
}

/** @internal */
export function snapshot(input: BinaryInput, transfer = false): Uint8Array<ArrayBuffer> {
  const view = viewOf(input)
  if (!transfer) {
    return new Uint8Array(view)
  }
  if (!(view.buffer instanceof ArrayBuffer)) {
    throw new TypeError('SharedArrayBuffer cannot transfer ownership')
  }
  return structuredClone(view, { transfer: [view.buffer] }) as Uint8Array<ArrayBuffer>
}

/** @internal */
export function joinChunks(
  chunks: Uint8Array<ArrayBuffer>[],
  size: number,
): Uint8Array<ArrayBuffer> {
  if (chunks.length === 1) {
    return chunks[0]
  }
  const output = new Uint8Array(size)
  let offset = 0
  for (const chunk of chunks) {
    output.set(chunk, offset)
    offset += chunk.byteLength
  }
  return output
}
