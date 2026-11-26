import { importRuntime } from './runtime-import.js'
import { abortReason, type ParsedOptions } from './options.js'

export interface ZstdModule {
  HEAPU8: Uint8Array<ArrayBuffer>
  UTF8ToString(pointer: number): string
  _malloc(bytes: number): number
  _free(pointer: number): void
  _codec_error(): number
  _codec_min_level(): number
  _codec_max_level(): number
  _codec_buffer_input(): number
  _codec_buffer_capacity(): number
  _codec_buffer_output(): number
  _codec_buffer_bound(size: number): number
  _codec_buffer_compress(
    output: number,
    capacity: number,
    input: number,
    size: number,
    level: number,
    checksum: number,
    includeSize: number,
    dictionary: number,
    dictionarySize: number,
  ): number
  _codec_buffer_size(input: number, size: number, windowLog: number): number
  _codec_frame_content_size(input: number, size: number): number
  _codec_buffer_decompress(
    output: number,
    capacity: number,
    input: number,
    size: number,
    dictionary: number,
    dictionarySize: number,
  ): number
  _codec_create(
    compress: number,
    level: number,
    checksum: number,
    windowLog: number,
    dictionary: number,
    bytes: number,
  ): number
  _codec_set_pledged_size(codec: number, size: number, includeSize: number): number
  _codec_free(codec: number): void
  _codec_buffer_acquire: ZstdModule['_codec_create']
  _codec_buffer_release(codec: number): void
  _codec_input_capacity(): number
  _codec_output_capacity(codec: number): number
  _codec_input(codec: number): number
  _codec_output(codec: number): number
  _codec_consumed(codec: number): number
  _codec_produced(codec: number): number
  _codec_process_at(
    codec: number,
    bytes: number,
    final: number,
    offset: number,
    capacity: number,
  ): number
}

const modules = new Map<string, { pending: Promise<ZstdModule>; ready?: ZstdModule }>()
let defaultModule: ZstdModule | undefined
export function zstdUrls(options: ParsedOptions): { loaderUrl: string; wasmUrl: string } {
  return {
    loaderUrl: options.loaderUrl
      ? new URL(options.loaderUrl, import.meta.url).href
      : new URL('./wasm/zstd.mjs', import.meta.url).href,
    wasmUrl: options.wasmUrl
      ? new URL(options.wasmUrl, import.meta.url).href
      : new URL('./wasm/zstd.wasm', import.meta.url).href,
  }
}

// A shared loader can outlive the caller; do not capture dictionaries or input options here.
function waitFor<T>(
  pending: Promise<T>,
  signal: AbortSignal | undefined,
  initializationTimeoutMs: number,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const finish = (action: () => void) => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', abort)
      action()
      signal = undefined
    }
    const abort = () => finish(() => reject(abortReason(signal)))
    const timer = setTimeout(
      () =>
        finish(() =>
          reject(new DOMException('Compression initialization timed out', 'TimeoutError')),
        ),
      initializationTimeoutMs,
    )
    signal?.addEventListener('abort', abort, { once: true })
    if (signal?.aborted) {
      abort()
    }
    pending.then(
      (value) => finish(() => resolve(value)),
      (error) => finish(() => reject(error)),
    )
  })
}

export function loadZstdModule(options: ParsedOptions): ZstdModule | Promise<ZstdModule> {
  options.signal?.throwIfAborted()
  const useDefault = options.loaderUrl === undefined && options.wasmUrl === undefined
  if (useDefault && defaultModule) {
    return defaultModule
  }
  const assets = useDefault ? undefined : zstdUrls(options)
  const key = assets ? JSON.stringify([assets.loaderUrl, assets.wasmUrl]) : 'embedded'
  let entry = modules.get(key)
  if (entry?.ready) {
    return entry.ready
  }
  if (!entry) {
    const pending = (async () => {
      if (!assets) {
        const { createEmbeddedModule } = await import('./embedded.js')
        return createEmbeddedModule()
      }
      const { loaderUrl, wasmUrl } = assets
      const { default: createModule } = (await importRuntime(loaderUrl)) as {
        default(options: { locateFile(path: string): string }): Promise<ZstdModule>
      }
      return createModule({
        locateFile: (path) => (path.endsWith('.wasm') ? wasmUrl : new URL(path, loaderUrl).href),
      })
    })()
    const current = (entry = { pending } as { pending: Promise<ZstdModule>; ready?: ZstdModule })
    modules.set(key, current)
    void pending.then(
      (module) => {
        current.ready = module
        if (useDefault) {
          defaultModule = module
        }
      },
      () => {
        if (modules.get(key) === current) {
          modules.delete(key)
        }
      },
    )
  }
  return waitFor(entry.pending, options.signal, options.initializationTimeoutMs)
}
