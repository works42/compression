# API Reference

ESM exports from `@works42/compression`:

| Export                               | Returns                                      |
| ------------------------------------ | -------------------------------------------- |
| `compress(input, options)`           | `Promise<Uint8Array>`                        |
| `decompress(input, options)`         | `Promise<Uint8Array>`                        |
| `compressStream(source, options)`    | `Promise<ReadableStream<Uint8Array>>`        |
| `decompressStream(source, options)`  | `Promise<ReadableStream<Uint8Array>>`        |
| `createCompressionStream(options)`   | `Promise<{ readable, writable }>`            |
| `createDecompressionStream(options)` | `Promise<{ readable, writable }>`            |
| `createZstdCodec(options?)`          | `Promise<ZstdCodec>` (sync methods)          |
| `createWorkerPool(options?)`         | `CompressionWorkerPool`                      |
| `WorkerPoolCapacityError`            | Pool capacity error class                    |
| `formats`                            | `['zstd', 'gzip', 'deflate', 'deflate-raw']` |

- Buffer inputs accept `ArrayBuffer` and its views (`Uint8Array`, `DataView`, Node `Buffer`).
- Outputs own their backing bytes and never alias inputs.
- `await` stream factories before passing `{ readable, writable }` to `pipeThrough()`.

## Common Options

| Option                    | Default    | Description                                                     |
| ------------------------- | ---------- | --------------------------------------------------------------- |
| `format`                  | Required   | One of `formats`; non-`zstd` formats accept common options only |
| `execution`               | `'direct'` | `'direct'` or `'worker'`                                        |
| `signal`                  | None       | `AbortSignal`                                                   |
| `maxInputBytes`           | `Infinity` | Max total input bytes (non-negative safe integer or `Infinity`) |
| `maxOutputBytes`          | `Infinity` | Max total output bytes                                          |
| `highWaterMarkBytes`      | `65536`    | Writable queue byte threshold (positive safe integer)           |
| `initializationTimeoutMs` | `15000`    | Initialization timeout in ms (`1` to `2147483647`)              |
| `workerFactory`           | Built-in   | Factory returning a fresh Worker (`'worker'` only; no pool)     |
| `workerPool`              | None       | Pool from `createWorkerPool()` (`'worker'` only)                |

- Without `workerPool`, each `'worker'` call spawns and terminates its own Worker. `workerFactory` must return a fresh Worker per call.
- Use `'direct'` inside an application-managed Worker. Browser `'worker'` mode requires transferable Web Streams.
- Default Node Workers launch with `execArgv: []`; supply `workerFactory` to customize Worker options.
- If serving without a bundler, keep the published JS files together so relative imports resolve.

## Source-Bound Streams

`compressStream(source, options)` and `decompressStream(source, options)` take an unlocked `ReadableStream<BinaryInput>` and resolve to an output `ReadableStream`.

- Accepts the same options as stream factories, except `contentSize: true` is rejected on compression streams (pass a byte count or `false`).
- Keep `source` unlocked until initialization resolves, and consume or cancel every output stream to release locks and workers.
- Output cancellation, errors, byte-limit violations, and `signal` aborts cancel the source and release resources.

## Prepared Zstd Codec

`await createZstdCodec(options = {})` initializes WASM once and returns a synchronous codec (`compress(input)` and `decompress(input)`).

```js
const codec = await createZstdCodec({ level: 3, checksum: true, contentSize: true })
const frame = codec.compress(input)
const restored = codec.decompress(frame)
```

- **Options**: `level`, `checksum`, `contentSize` (`compress`); `maxWindowLog` (`decompress`); `dictionary`, `maxInputBytes`, `maxOutputBytes`, `signal`, `initializationTimeoutMs`, `loaderUrl`, `wasmUrl`. Rejects `format`, `execution`, Worker options, `highWaterMarkBytes`, and `transferInput`.
- Options and `dictionary` are snapshotted at creation; byte limits apply per call.
- `contentSize: true` writes each input's length; a number requires every input to match that exact length.
- Aborting `signal` fails subsequent calls with its reason (running sync calls are not interrupted mid-call).

## Worker Pool

`createWorkerPool({ size = 1, maxQueuedOperations = 64, maxPendingBytes = 67108864, workerFactory } = {})` creates a lazily populated pool for `execution: 'worker'`.

```js
import { createWorkerPool, compress, decompress } from '@works42/compression'

const pool = createWorkerPool({ size: 2, maxQueuedOperations: 8 })
try {
  const options = { format: 'zstd', execution: 'worker', workerPool: pool }
  const packed = await compress(input, options)
  const restored = await decompress(packed, options)
} finally {
  await pool.close()
}
```

- **Queue & budget**: Workers run one task at a time, up to `size + maxQueuedOperations` in flight (`0` disables queueing). `initializationTimeoutMs` includes queue wait time.
- **`maxPendingBytes`**: Enforced at admission over retained inputs and dictionaries (for streams, counts dictionaries only and holds a slot until both ends settle or cancel). Transferring a subview counts its full backing `ArrayBuffer`.
- **Errors & recovery**: Exceeding limits rejects with `WorkerPoolCapacityError` (`limit` is `'maxQueuedOperations'` or `'maxPendingBytes'`). Failed or cancelled Workers are replaced lazily.
- **Shutdown**: `await pool.close()` is idempotent, aborts active/queued work, and terminates Workers. New submissions to a closed pool reject with `AbortError`.

## Zstd Options & Errors

| Option         | Direction             | Default     | Description                                                        |
| -------------- | --------------------- | ----------- | ------------------------------------------------------------------ |
| `level`        | Compress              | `3`         | Integer supported by the linked Zstd codec (negative levels valid) |
| `checksum`     | Compress              | `true`      | Write frame content checksum                                       |
| `contentSize`  | Compress              | Unspecified | `true` (buffer only), explicit byte length, or `false`             |
| `maxWindowLog` | Decompress            | `27`        | Max window exponent (`10`–`30`), enforced across all frames        |
| `dictionary`   | Compress & Decompress | None        | `Uint8Array`, snapshotted for async calls                          |
| `loaderUrl`    | Compress & Decompress | Bundled     | URL for a version-matched JS loader                                |
| `wasmUrl`      | Compress & Decompress | Bundled     | URL for a version-matched WASM binary                              |

- **Frames**: Non-empty Zstd frames omit `contentSize` by default; length mismatches throw `RangeError`. Decompression supports concatenated and skippable frames and rejects malformed trailing bytes.
- **Self-hosting & CSP**: Bundled WASM needs no extra hosting; CSP must allow `wasm-unsafe-eval`, module scripts, and Workers. Custom `loaderUrl`/`wasmUrl` resolve relative to the library module (prefer absolute URLs).
- **Errors**: Unknown or inapplicable options reject with `TypeError`; byte-limit/level violations with `RangeError`; unsupported native formats with `NotSupportedError`; init timeouts with `TimeoutError`. Worker init errors preserve `name` and `message` for `RangeError`, `TypeError`, and `DOMException`.

## Ownership & TypeScript

- **Buffer ownership**: Inputs to `compress()`/`decompress()` are reusable immediately unless `transferInput: true` is set (which detaches the backing `ArrayBuffer`, cannot be undone on error, and rejects `SharedArrayBuffer`).
- **Streams**: Do not mutate chunks passed to `writer.write()` until the write settles. Treat streamed decompression output as provisional until `pipeTo()` resolves.
- **TypeScript**: Requires TypeScript 5.7+ and the `DOM` lib (e.g. `"lib": ["ES2023", "DOM"]`), because declarations use generic typed arrays like `Uint8Array<ArrayBuffer>`.
