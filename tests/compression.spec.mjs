import { test, expect } from '@playwright/test'
import { createServer } from 'node:http'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve, extname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { execFileSync } from 'node:child_process'
import * as zlib from 'node:zlib'
import { build, createServer as createViteServer, preview } from 'vite'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const formats = ['zstd', 'gzip', 'deflate', 'deflate-raw']
const encode = {
  zstd: zlib.zstdCompressSync,
  gzip: zlib.gzipSync,
  deflate: zlib.deflateSync,
  'deflate-raw': zlib.deflateRawSync,
}
const decode = {
  zstd: zlib.zstdDecompressSync,
  gzip: zlib.gunzipSync,
  deflate: zlib.inflateSync,
  'deflate-raw': zlib.inflateRawSync,
}
let directory
let server
let origin

test('pooled admission preserves rejected ownership and recovers after queued cancellation', async ({
  page,
}) => {
  await page.goto(origin)
  await page.waitForFunction(() => window.library)
  expect(
    await page.evaluate(async () => {
      const lib = window.library
      const pool = lib.createWorkerPool({ size: 1, maxQueuedOperations: 1, maxPendingBytes: 32 })
      const options = { format: 'zstd', execution: 'worker', workerPool: pool }
      try {
        const held = await lib.createCompressionStream(options)
        const controller = new AbortController()
        const queued = lib.compress(new Uint8Array(32), { ...options, signal: controller.signal })
        const cancelled = queued.then(
          () => false,
          (error) => error.name === 'AbortError',
        )
        const input = new Uint8Array([1])
        const full = await lib.compress(input, { ...options, transferInput: true }).then(
          () => false,
          (error) => error instanceof lib.WorkerPoolCapacityError,
        )
        controller.abort()
        const aborted = await cancelled
        const storage = new Uint8Array(33)
        const bytesFull = await lib
          .compress(storage.subarray(0, 1), { ...options, transferInput: true })
          .then(
            () => false,
            (error) => error.limit === 'maxPendingBytes',
          )
        const pending = lib.compress(input, options)
        const reading = new Response(held.readable).arrayBuffer()
        await held.writable.close()
        await reading
        const restored = await lib.decompress(await pending, options)
        return (
          full &&
          aborted &&
          bytesFull &&
          !input.buffer.detached &&
          !storage.buffer.detached &&
          restored[0] === 1
        )
      } finally {
        await pool.close()
      }
    }),
  ).toBe(true)
})

test.beforeAll(async () => {
  test.setTimeout(120_000)
  directory = await mkdtemp(join(tmpdir(), 'compression-test-'))
  try {
    const [packed] = Object.values(
      JSON.parse(
        execFileSync(
          'npm',
          ['pack', '--ignore-scripts', '--json', '--pack-destination', directory],
          { cwd: root, encoding: 'utf8' },
        ),
      ),
    )
    expect(packed.files.map((file) => file.path)).toEqual(
      expect.arrayContaining([
        'dist/index.js',
        'dist/index.d.ts',
        'dist/worker.js',
        'dist/embedded.js',
        'dist/wasm/zstd.mjs',
        'dist/wasm/zstd.wasm',
        'LICENSE.zstd',
      ]),
    )
    await writeFile(join(directory, 'package.json'), '{"private":true,"type":"module"}')
    execFileSync(
      'npm',
      ['install', '--ignore-scripts', '--no-audit', '--no-fund', join(directory, packed.filename)],
      { cwd: directory, stdio: 'pipe' },
    )
    // Resolve types from the packed artifact, without checkout source modules.
    await writeFile(
      join(directory, 'consumer.ts'),
      (await readFile(join(root, 'tests/types.ts'), 'utf8')).replace(
        '../src/index.js',
        '@works42/compression',
      ),
    )
    for (const resolution of ['NodeNext', 'Bundler']) {
      execFileSync(
        process.execPath,
        [
          join(root, 'node_modules/typescript/bin/tsc'),
          '--noEmit',
          '--strict',
          '--target',
          'ES2023',
          '--lib',
          'ES2023,DOM',
          '--module',
          resolution === 'NodeNext' ? 'NodeNext' : 'ESNext',
          '--moduleResolution',
          resolution,
          'consumer.ts',
        ],
        { cwd: directory, stdio: 'pipe' },
      )
    }
    // Test Node resolution from the installed artifact too, including eval/stdin
    // flags that must not leak into file-based worker entry points.
    execFileSync(
      process.execPath,
      [
        '--input-type=module',
        '-e',
        `
      import { compress, decompress, formats } from '@works42/compression';
      for (const format of formats) for (const execution of ['direct', 'worker']) {
        const bytes = await decompress(await compress(new Uint8Array([1,2,3]), {format, execution}), {format, execution});
        if (bytes.join(',') !== '1,2,3') throw new Error('Installed Node package mismatch');
      }
      const loaderUrl = import.meta.resolve('@works42/compression/wasm/zstd.mjs');
      const wasmUrl = import.meta.resolve('@works42/compression/wasm/zstd.wasm');
      for (const execution of ['direct', 'worker']) {
        for (const assets of [{loaderUrl}, {wasmUrl}, {loaderUrl, wasmUrl}]) {
          const options = {format: 'zstd', execution, ...assets};
          const bytes = await decompress(await compress(new Uint8Array([4,5,6]), options), options);
          if (bytes.join(',') !== '4,5,6') throw new Error('Installed WASM export mismatch');
        }
      }
    `,
      ],
      { cwd: directory, stdio: 'pipe' },
    )
    const packageRoot = join(directory, 'node_modules/@works42/compression')
    server = createServer(async (request, response) => {
      try {
        const path = new URL(request.url, 'http://localhost').pathname
        if (path === '/') {
          response.setHeader('Content-Type', 'text/html')
          response.end(
            '<script type="module">window.library = await import("/dist/index.js")</script>',
          )
          return
        }
        if (path === '/embedded-only') {
          response.setHeader('Content-Type', 'text/html')
          response.setHeader(
            'Content-Security-Policy',
            "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'; connect-src 'none'",
          )
          response.end('<script type="module" src="/embedded-main.js"></script>')
          return
        }
        if (path === '/scenarios.mjs') {
          response.setHeader('Content-Type', 'text/javascript')
          response.end(await readFile(join(root, 'tests/scenarios.mjs')))
          return
        }
        const fixtures = {
          '/embedded-main.js': 'window.library = await import("/dist/index.js")',
          '/hang-worker.js': '',
          '/bad-worker.js': 'postMessage({error:"worker failed", unexpected:true})',
          '/crash-worker.js': 'throw new Error("worker crashed")',
          '/no-native-worker.js':
            'import "/dist/worker.js"; self.CompressionStream = self.DecompressionStream = undefined',
          '/no-stream-transfer-worker.js':
            'import "/dist/worker.js"; const post = self.postMessage.bind(self); self.postMessage = (value, transfer) => { if (transfer.length) throw new DOMException("Stream transfer unavailable", "DataCloneError"); post(value, transfer) }',
          '/failure.mjs':
            'const params = new URL(import.meta.url).searchParams; export default () => { const error = params.has("ordinary") ? new Error("loader failed") : new DOMException("loader failed", params.get("name")); if (params.has("ordinary")) error.name = params.get("name"); throw error }',
          '/plain-worker.js':
            'import * as lib from "/dist/index.js"; import { streaming } from "/scenarios.mjs"; self.Worker = undefined; self.onmessage = async () => { try { for (const format of lib.formats) await streaming(lib, format, "direct"); postMessage({ok:true}) } catch(error) { postMessage({error:String(error)}) } }',
        }
        if (path in fixtures) {
          response.setHeader('Content-Type', 'text/javascript')
          response.end(fixtures[path])
          return
        }
        const file = resolve(packageRoot, `.${path}`)
        if (!file.startsWith(`${packageRoot}/`)) {
          response.writeHead(403).end()
          return
        }
        response.setHeader(
          'Content-Type',
          extname(file) === '.wasm' ? 'application/wasm' : 'text/javascript',
        )
        response.end(await readFile(file))
      } catch {
        response.writeHead(404).end()
      }
    })
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
    origin = `http://127.0.0.1:${server.address().port}`
  } catch (error) {
    if (server?.listening) {
      await new Promise((resolve) => server.close(resolve))
    }
    await rm(directory, { recursive: true, force: true })
    throw error
  }
})

test.afterAll(async () => {
  try {
    if (server?.listening) {
      await new Promise((resolve) => server.close(resolve))
    }
  } finally {
    if (directory) {
      await rm(directory, { recursive: true, force: true })
    }
  }
})

test.beforeEach(async ({ page }) => {
  await page.goto(origin)
  await page.waitForFunction(() => window.library)
})

for (const format of formats) {
  for (const execution of ['direct', 'worker']) {
    for (const scenario of [
      'roundTrip',
      'streaming',
      'limitsAndErrors',
      'cancellation',
      'sourceStreams',
    ]) {
      test(`${format} ${execution}: ${scenario}`, async ({ page }) => {
        await page.evaluate(
          async ({ scenario, format, execution }) => {
            const scenarios = await import('/scenarios.mjs')
            await scenarios[scenario](window.library, format, execution)
          },
          { scenario, format, execution },
        )
      })
    }
    test(`${format} ${execution}: independent Node interoperability`, async ({ page }) => {
      const plain = Buffer.alloc(1024 * 1024 + 19, 'cross-impl')
      const compressed = encode[format](plain)
      const result = await page.evaluate(
        async ({ format, execution, compressed }) => {
          const options = { format, execution }
          const decoded = await window.library.decompress(Uint8Array.from(compressed), options)
          const packed = await window.library.compress(decoded, options)
          return { size: decoded.length, packed: Array.from(packed) }
        },
        { format, execution, compressed: Array.from(compressed) },
      )
      expect(result.size).toBe(plain.length)
      expect(decode[format](Buffer.from(result.packed))).toEqual(plain)
    })
  }
}

for (const format of ['zstd', 'gzip']) {
  for (const execution of ['direct', 'worker']) {
    test(`${format} ${execution}: large Buffer results retain fixed storage and Web API compatibility`, async ({
      page,
    }) => {
      await page.evaluate(
        async ({ format, execution }) => {
          const { largeBufferOutput } = await import('/scenarios.mjs')
          await largeBufferOutput(window.library, format, execution)
        },
        { format, execution },
      )
    })
  }
}

for (const execution of ['direct', 'worker']) {
  test(`Zstd ${execution}: frame window limits hold across APIs and input splits`, async ({
    page,
  }) => {
    await page.evaluate(async (execution) => {
      const { zstdWindowLimits } = await import('/scenarios.mjs')
      await zstdWindowLimits(window.library, execution)
    }, execution)
  })
}

test('option validation and snapshot ownership', async ({ page }) => {
  await page.evaluate(async () => {
    await (await import('/scenarios.mjs')).validation(window.library)
  })
})

test('worker setup rejects locked responses and cleans up partially registered listeners', async ({
  page,
}) => {
  await page.evaluate(async () => {
    await (await import('/scenarios.mjs')).workerSetupFailures(window.library)
  })
})
test('prepared Zstd synchronous codecs preserve limits, ownership and frame validation', async ({
  page,
}) => {
  await page.evaluate(async () => {
    await (await import('/scenarios.mjs')).preparedZstd(window.library)
  })
})
test('WASM decoding retains native matches beyond 64 MiB', async ({ page }) => {
  test.setTimeout(60000)
  const MiB = 1024 ** 2
  const tail = 65 * MiB + 65536
  const head = Buffer.alloc(MiB)
  let state = 123456789
  for (let index = 0; index < head.length; index++) {
    state ^= state << 13
    state ^= state >>> 17
    state ^= state << 5
    head[index] = state & 255
  }
  const plain = Buffer.alloc(tail + head.length)
  head.copy(plain)
  head.copy(plain, tail)
  const frame = zlib.zstdCompressSync(plain, {
    params: {
      [zlib.constants.ZSTD_c_compressionLevel]: 3,
      [zlib.constants.ZSTD_c_windowLog]: 27,
      [zlib.constants.ZSTD_c_enableLongDistanceMatching]: 1,
      [zlib.constants.ZSTD_c_checksumFlag]: 1,
    },
  })
  expect(frame.length).toBeLessThan(head.length * 1.25)
  await page.evaluate(
    async ({ frame, head, tail }) => {
      const bytes = Uint8Array.from(frame)
      for (const execution of ['direct', 'worker']) {
        let inputBytes = 0
        let outputBytes = 0
        const source = new ReadableStream({
          pull(controller) {
            if (inputBytes === bytes.length) {
              return controller.close()
            }
            const end = Math.min(inputBytes + 65537, bytes.length)
            controller.enqueue(bytes.subarray(inputBytes, end))
            inputBytes = end
          },
        })
        const output = await window.library.decompressStream(source, {
          format: 'zstd',
          execution,
          maxWindowLog: 27,
          maxInputBytes: bytes.length,
          maxOutputBytes: tail + head.length,
        })
        await output.pipeTo(
          new WritableStream({
            write(chunk) {
              for (let index = 0; index < chunk.length; index++) {
                const position = outputBytes + index
                const expected =
                  position < head.length
                    ? head[position]
                    : position >= tail
                      ? head[position - tail]
                      : 0
                if (chunk[index] !== expected) {
                  throw new Error('Distant native match differs')
                }
              }
              outputBytes += chunk.length
            },
          }),
        )
        if (outputBytes !== tail + head.length) {
          throw new Error('Distant native frame was truncated')
        }
      }
    },
    { frame: Array.from(frame), head: Array.from(head), tail },
  )
})
for (const execution of ['direct', 'worker']) {
  test(`zstd ${execution}: scratch buffers and interleaved streams`, async ({ page }) => {
    await page.evaluate(async (execution) => {
      await (await import('/scenarios.mjs')).zstdScratchBuffers(window.library, execution)
    }, execution)
  })
  test(`zstd ${execution}: self-hosted matching assets`, async ({ page }) => {
    expect(
      await page.evaluate(async (execution) => {
        const input = new Uint8Array(2 * 1024 * 1024 + 11).fill(73)
        const options = {
          format: 'zstd',
          execution,
          loaderUrl: '/dist/wasm/zstd.mjs',
          wasmUrl: '/dist/wasm/zstd.wasm',
        }
        const result = await window.library.decompress(
          await window.library.compress(input, options),
          options,
        )
        return result.length === input.length && result.every((byte) => byte === 73)
      }, execution),
    ).toBe(true)
  })
}
test('native formats do not fetch Zstd assets', async ({ page }) => {
  const assets = []
  page.on('request', (request) => {
    if (/embedded\.js|zstd\.(mjs|wasm)/.test(request.url())) {
      assets.push(request.url())
    }
  })
  await page.evaluate(async () => {
    for (const format of ['gzip', 'deflate', 'deflate-raw']) {
      for (const execution of ['direct', 'worker']) {
        await window.library.compress(new Uint8Array(), { format, execution })
      }
    }
  })
  expect(assets).toEqual([])
})
test('embedded Zstd works with script-only CSP and no separately hosted codec assets', async ({
  page,
  context,
}) => {
  const codecRequests = []
  context.on('request', (request) => {
    if (/\/wasm\/|blob:|data:/.test(request.url())) {
      codecRequests.push(request.url())
    }
  })
  await context.route('**/wasm/**', (route) => route.abort())
  await page.goto(origin + '/embedded-only')
  await page.waitForFunction(() => window.library)
  expect(
    await page.evaluate(async () => {
      const saved = globalThis.DecompressionStream
      const savedFromBase64 = Uint8Array.fromBase64
      // Embedded binary decoding does not require native gzip decompression.
      globalThis.DecompressionStream = undefined
      Uint8Array.fromBase64 = undefined
      try {
        for (const execution of ['direct', 'worker']) {
          const options = { format: 'zstd', execution }
          const input = new Uint8Array(262157).fill(67)
          const output = await window.library.decompress(
            await window.library.compress(input, options),
            options,
          )
          if (output.length !== input.length || !output.every((byte) => byte === 67)) {
            return false
          }
        }
        return true
      } finally {
        globalThis.DecompressionStream = saved
        Uint8Array.fromBase64 = savedFromBase64
      }
    }),
  ).toBe(true)
  expect(codecRequests).toEqual([])
})
test('can execute directly inside an existing worker', async ({ page }) => {
  expect(
    await page.evaluate(
      () =>
        new Promise((resolve, reject) => {
          const worker = new Worker('/plain-worker.js', { type: 'module' })
          worker.onmessage = (event) => {
            worker.terminate()
            event.data.ok ? resolve(true) : reject(new Error(event.data.error))
          }
          worker.onerror = (event) => {
            worker.terminate()
            reject(new Error(event.message))
          }
          worker.postMessage({})
        }),
    ),
  ).toBe(true)
})
test('initialization timeout, cancellation, malformed response, worker crash and unsupported native API', async ({
  page,
}) => {
  expect(
    await page.evaluate(async () => {
      const failures = []
      for (const [url, pattern, name] of [
        ['/hang-worker.js', 'timed out', 'TimeoutError'],
        ['/bad-worker.js', 'Invalid', 'Error'],
        ['/crash-worker.js', 'worker crashed', 'Error'],
      ]) {
        failures.push(
          await window.library
            .compress(new Uint8Array(), {
              format: 'gzip',
              execution: 'worker',
              workerFactory: () => new Worker(url, { type: 'module' }),
              initializationTimeoutMs: 1000,
            })
            .then(
              () => false,
              (error) => error.name === name && error.message.includes(pattern),
            ),
        )
      }
      const controller = new AbortController()
      const pending = window.library
        .compress(new Uint8Array(), {
          format: 'gzip',
          execution: 'worker',
          signal: controller.signal,
          workerFactory: () => new Worker('/hang-worker.js', { type: 'module' }),
        })
        .then(
          () => false,
          (error) => error.message === 'cancel init',
        )
      controller.abort(new Error('cancel init'))
      failures.push(await pending)
      const { CompressionStream, DecompressionStream } = globalThis
      try {
        globalThis.CompressionStream = globalThis.DecompressionStream = undefined
        const worker = {
          execution: 'worker',
          workerFactory: () => new Worker('/no-native-worker.js', { type: 'module' }),
        }
        for (const open of [
          window.library.createCompressionStream,
          window.library.createDecompressionStream,
        ]) {
          for (const options of [{}, worker]) {
            failures.push(
              await open({ format: 'gzip', ...options }).then(
                () => false,
                (error) => error instanceof DOMException && error.name === 'NotSupportedError',
              ),
            )
          }
        }
      } finally {
        Object.assign(globalThis, { CompressionStream, DecompressionStream })
      }
      return failures.every(Boolean)
    }),
  ).toBe(true)
})
test('worker initialization preserves DOMExceptions and named ordinary errors', async ({
  page,
}) => {
  await page.evaluate(async () => {
    for (const open of [
      window.library.createCompressionStream,
      window.library.createDecompressionStream,
    ]) {
      for (const name of [
        'DataCloneError',
        'AbortError',
        'SecurityError',
        'OperationError',
        'CustomSetupError',
        'TypeError',
      ]) {
        const loaderUrl = new URL('/failure.mjs', location.href)
        loaderUrl.searchParams.set('name', name)
        try {
          await open({ format: 'zstd', execution: 'worker', loaderUrl })
          throw new Error('Expected initialization failure')
        } catch (error) {
          if (
            !(error instanceof DOMException) ||
            error.name !== name ||
            error.message !== 'loader failed'
          ) {
            throw error
          }
        }
      }
      const loaderUrl = new URL('/failure.mjs', location.href)
      loaderUrl.searchParams.set('name', 'TimeoutError')
      loaderUrl.searchParams.set('ordinary', 'true')
      try {
        await open({ format: 'zstd', execution: 'worker', loaderUrl })
        throw new Error('Expected initialization failure')
      } catch (error) {
        if (
          !(error instanceof Error) ||
          error instanceof DOMException ||
          error.name !== 'TimeoutError' ||
          error.message !== 'loader failed'
        ) {
          throw error
        }
      }
    }
  })
})
test('worker stream transfer failure preserves DataCloneError and terminates the worker', async ({
  page,
}) => {
  await page.evaluate(async () => {
    let terminated = 0
    const workerFactory = () => {
      const worker = new Worker('/no-stream-transfer-worker.js', { type: 'module' })
      const terminate = worker.terminate.bind(worker)
      worker.terminate = () => {
        terminated++
        terminate()
      }
      return worker
    }
    try {
      await window.library.createCompressionStream({
        format: 'gzip',
        execution: 'worker',
        workerFactory,
      })
      throw new Error('Expected stream transfer failure')
    } catch (error) {
      if (
        !(error instanceof DOMException) ||
        error.name !== 'DataCloneError' ||
        error.message !== 'Stream transfer unavailable'
      ) {
        throw error
      }
    }
    if (terminated !== 1) {
      throw new Error(`Worker terminated ${terminated} times`)
    }
  })
})
test('installed package works in Vite development and production under a subpath', async ({
  page,
}) => {
  test.setTimeout(120_000)
  await writeFile(join(directory, 'index.html'), '<script type="module" src="/main.js"></script>')
  await writeFile(
    join(directory, 'main.js'),
    await readFile(join(root, 'tests/integration-fixture.mjs')),
  )
  const config = { configFile: false, root: directory, base: '/nested/', logLevel: 'warn' }
  const dev = await createViteServer({ ...config, server: { port: 0, host: '127.0.0.1' } })
  let production
  const verify = async (address) => {
    await page.goto(address)
    await page.waitForFunction(() => globalThis.integrationComplete === true)
  }
  try {
    await dev.listen()
    await verify(`http://127.0.0.1:${dev.httpServer.address().port}/nested/`)
    await dev.close()
    await build(config)
    production = await preview({ ...config, preview: { port: 0, host: '127.0.0.1' } })
    await verify(`http://127.0.0.1:${production.httpServer.address().port}/nested/`)
  } finally {
    await dev.close()
    if (production) {
      await new Promise((resolve) => production.httpServer.close(resolve))
    }
  }
})
