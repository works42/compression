import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as zlib from 'node:zlib'
import { Worker } from 'node:worker_threads'
import { createHash, randomFillSync } from 'node:crypto'
import { cp, mkdtemp, writeFile, rm } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import * as lib from '../dist/index.js'
import * as scenarios from './scenarios.mjs'

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

test(
  'WASM decoders retain offsets beyond 64 MiB with exact limits and native frames',
  { timeout: 30000 },
  async () => {
    const MiB = 1024 ** 2
    const tail = 65 * MiB + 65536
    const head = randomFillSync(new Uint8Array(MiB))
    const plain = new Uint8Array(tail + head.length)
    plain.set(head)
    plain.set(head, tail)
    const frame = zlib.zstdCompressSync(plain, {
      params: {
        [zlib.constants.ZSTD_c_compressionLevel]: 3,
        [zlib.constants.ZSTD_c_windowLog]: 27,
        [zlib.constants.ZSTD_c_enableLongDistanceMatching]: 1,
        [zlib.constants.ZSTD_c_checksumFlag]: 1,
      },
    })
    // The random prefix appears again only beyond the 32-bit decoder's short-offset range.
    assert(
      frame.length < head.length * 1.25,
      'Expected a distant match rather than a second literal prefix',
    )
    const expected = createHash('sha256').update(plain).digest('hex')
    for (const execution of ['direct', 'worker']) {
      const hash = createHash('sha256')
      let offset = 0
      let outputBytes = 0
      const source = new ReadableStream({
        pull(controller) {
          if (offset === frame.length) {
            return controller.close()
          }
          const end = Math.min(offset + 65537, frame.length)
          controller.enqueue(frame.subarray(offset, end))
          offset = end
        },
      })
      const output = await lib.decompressStream(source, {
        format: 'zstd',
        execution,
        maxWindowLog: 27,
        maxInputBytes: frame.length,
        maxOutputBytes: plain.length,
      })
      await output.pipeTo(
        new WritableStream({
          write(chunk) {
            outputBytes += chunk.length
            hash.update(chunk)
          },
        }),
      )
      assert.equal(outputBytes, plain.length)
      assert.equal(hash.digest('hex'), expected)
      await assert.rejects(lib.decompress(frame, { format: 'zstd', execution, maxWindowLog: 26 }))
    }
  },
)

test('WASM decoder variants read native frames across levels, literals and concatenated boundaries', async () => {
  const text = Buffer.from('timestamp=2026-11-25 route=/items/7 status=200 user '.repeat(1700))
  const entropy = randomFillSync(Buffer.alloc(131073))
  const codec = await lib.createZstdCodec()
  for (const level of [-3, 1, 3, 9, 19, 22]) {
    const frames = [text, entropy].map((input) =>
      zlib.zstdCompressSync(input, {
        params: {
          [zlib.constants.ZSTD_c_compressionLevel]: level,
          [zlib.constants.ZSTD_c_checksumFlag]: 1,
        },
      }),
    )
    assert.deepEqual(
      Buffer.from(codec.decompress(Buffer.concat(frames))),
      Buffer.concat([text, entropy]),
    )
  }
})
for (const execution of ['direct', 'worker']) {
  test(
    `Zstd ${execution}: frame window limits hold across APIs and input splits`,
    { timeout: 30000 },
    () => scenarios.zstdWindowLimits(lib, execution),
  )
}
for (const format of lib.formats) {
  for (const execution of ['direct', 'worker']) {
    for (const scenario of [
      'roundTrip',
      'streaming',
      'limitsAndErrors',
      'cancellation',
      'sourceStreams',
    ]) {
      test(`${format} ${execution}: ${scenario}`, { timeout: 30000 }, () =>
        scenarios[scenario](lib, format, execution),
      )
    }
    test(`${format} ${execution}: independent Node interoperability`, async () => {
      const input = Buffer.alloc(2 * 1024 * 1024 + 13, 'cross-impl')
      assert.deepEqual(
        Buffer.from(await lib.decompress(encode[format](input), { format, execution })),
        input,
      )
      assert.deepEqual(decode[format](await lib.compress(input, { format, execution })), input)
      const buffer = Buffer.from([99, 1, 2, 99])
      assert.deepEqual(
        Array.from(
          await lib.decompress(await lib.compress(buffer.subarray(1, 3), { format, execution }), {
            format,
            execution,
          }),
        ),
        [1, 2],
      )
      assert.deepEqual(Array.from(buffer), [99, 1, 2, 99])
    })
  }
}
test('validates options and ownership', () => scenarios.validation(lib))

test('embedded default works without the separate WASM distribution', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'compression-embedded-'))
  try {
    await cp(new URL('../dist', import.meta.url), join(directory, 'dist'), { recursive: true })
    await rm(join(directory, 'dist/wasm'), { recursive: true, force: true })
    await writeFile(join(directory, 'package.json'), '{"type":"module"}')
    execFileSync(
      process.execPath,
      [
        '--input-type=module',
        '-e',
        `
      import assert from 'node:assert/strict';
      import * as lib from './dist/index.js';
      for (const execution of ['direct', 'worker']) {
        const input = new Uint8Array(262157).fill(53);
        const options = { format: 'zstd', execution };
        assert.deepEqual(await lib.decompress(await lib.compress(input, options), options), input);
      }
    `,
      ],
      { cwd: directory, stdio: 'pipe', timeout: 15000 },
    )
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('embedded initialization shares pending work and retries after failure', async () => {
  // A distinct entry gives this test a cold module cache without changing production state.
  const fresh = await import('../dist/index.js?embedded-initialization')
  const instantiate = WebAssembly.instantiate
  let calls = 0
  WebAssembly.instantiate = async (...args) => {
    calls++
    if (calls === 1) {
      throw new Error('test initialization failure')
    }
    return instantiate(...args)
  }
  try {
    await assert.rejects(
      fresh.createCompressionStream({ format: 'zstd' }),
      /test initialization failure/,
    )
    const controller = new AbortController()
    const reason = new Error('cancel one initializer')
    const aborted = fresh.createCompressionStream({ format: 'zstd', signal: controller.signal })
    const successful = fresh.createDecompressionStream({ format: 'zstd' })
    controller.abort(reason)
    await assert.rejects(aborted, (error) => error === reason)
    await (await successful).readable.cancel()
    assert.equal(calls, 2)
  } finally {
    WebAssembly.instantiate = instantiate
  }
})

test('a single custom asset override retains the external loading path', async () => {
  for (const execution of ['direct', 'worker']) {
    for (const override of [
      { loaderUrl: new URL('../dist/wasm/zstd.mjs', import.meta.url) },
      { wasmUrl: new URL('../dist/wasm/zstd.wasm', import.meta.url) },
    ]) {
      const options = { format: 'zstd', execution, ...override }
      const input = new Uint8Array([1, 2, 3])
      assert.deepEqual(await lib.decompress(await lib.compress(input, options), options), input)
    }
  }
})

test('direct Zstd buffers preserve limits, ownership and frame compatibility', async () => {
  const options = { format: 'zstd' }
  const sample = Buffer.from('a short record '.repeat(20))
  const input = Uint8Array.from(sample)
  const pending = lib.compress(input, options)
  input.fill(0)
  const frame = await pending
  assert(frame[4] & 4)
  assert.deepEqual(zlib.zstdDecompressSync(frame), sample)
  assert.deepEqual(
    Buffer.from(await lib.decompress(frame, { ...options, maxOutputBytes: sample.length })),
    sample,
  )
  await assert.rejects(
    lib.decompress(frame, { ...options, maxOutputBytes: sample.length - 1 }),
    RangeError,
  )

  const corrupted = frame.slice()
  corrupted[corrupted.length - 1] ^= 1
  await assert.rejects(lib.decompress(corrupted, options))
  await assert.rejects(lib.decompress(frame.subarray(0, -1), options))
  const skippable = Buffer.alloc(8)
  skippable.writeUInt32LE(0x184d2a50)
  const frames = Buffer.concat([frame, skippable, zlib.zstdCompressSync(sample)])
  assert.deepEqual(
    Buffer.from(await lib.decompress(frames, options)),
    Buffer.concat([sample, sample]),
  )

  const large = Buffer.alloc(1024 * 1024 + 21, 'unknown-size expansion')
  const largeFrame = await lib.compress(large, options)
  assert(largeFrame.length < 32 * 1024)
  assert.equal(largeFrame[4] & 0xe0, 0)
  assert.deepEqual(Buffer.from(await lib.decompress(largeFrame, options)), large)
  await assert.rejects(
    lib.decompress(largeFrame, { ...options, maxOutputBytes: large.length - 1 }),
    RangeError,
  )
  await assert.rejects(lib.decompress(largeFrame, { ...options, maxWindowLog: 10 }))

  const controller = new AbortController()
  const reason = new Error('cancel buffer')
  const cancelled = lib.compress(sample, { ...options, signal: controller.signal })
  controller.abort(reason)
  await assert.rejects(cancelled, (error) => error === reason)

  await Promise.all(
    [1, 3, 9, 1].map(async (level, index) => {
      const checksum = index % 2 === 0
      const encoded = await lib.compress(sample, { ...options, level, checksum })
      assert.equal(Boolean(encoded[4] & 4), checksum)
      assert.deepEqual(Buffer.from(await lib.decompress(encoded, options)), sample)
    }),
  )
})

test('incremental direct Zstd buffers finalize every frame and release cancelled operations', async () => {
  const options = { format: 'zstd' }
  const plain = Buffer.alloc(2 * 1024 * 1024 + 19, 'large buffer sample')
  const first = zlib.zstdCompressSync(plain)
  const frames = Buffer.concat([first, first])
  const size = 2 * plain.length
  assert.deepEqual(
    await lib.decompress(frames, { ...options, maxOutputBytes: size }),
    Uint8Array.from(Buffer.concat([plain, plain])),
  )
  await assert.rejects(lib.decompress(frames, { ...options, maxOutputBytes: size - 1 }), RangeError)
  await assert.rejects(lib.decompress(frames.subarray(0, -1), options), /truncated/i)

  const controller = new AbortController()
  const reason = new Error('cancel during codec work')
  const timer = setTimeout(() => controller.abort(reason), 0)
  try {
    const manyFrames = Buffer.concat(Array(32).fill(first))
    await assert.rejects(
      lib.decompress(manyFrames, { ...options, signal: controller.signal }),
      (error) => error === reason,
    )
  } finally {
    clearTimeout(timer)
  }
  assert.deepEqual(Buffer.from(await lib.decompress(first, options)), plain)
})

for (const execution of ['direct', 'worker']) {
  test(`zstd ${execution}: scratch buffers and interleaved streams`, { timeout: 30000 }, () =>
    scenarios.zstdScratchBuffers(lib, execution),
  )
  test(`zstd ${execution}: dictionaries, checksums, levels, continuous frames and window limits`, async () => {
    const dictionary = new TextEncoder().encode('dictionary sample text '.repeat(100))
    const input = dictionary.slice(0, 2000)
    const suppliedDictionary = dictionary.slice()
    const pending = lib.compress(input, {
      format: 'zstd',
      dictionary: suppliedDictionary,
      level: 10,
      execution,
    })
    suppliedDictionary.fill(0)
    const packed = await pending
    assert.deepEqual(await lib.decompress(packed, { format: 'zstd', dictionary, execution }), input)
    await assert.rejects(lib.decompress(packed, { format: 'zstd', execution }))
    const plain = new Uint8Array(3 * 1024 * 1024).fill(42)
    const frame = await lib.compress(plain, { format: 'zstd', execution })
    // Streaming encoder deliberately doesn't require/pledge a total input size.
    assert.equal(frame[4] >>> 6, 0)
    assert.equal(frame[4] & 32, 0)
    await assert.rejects(lib.decompress(frame, { format: 'zstd', execution, maxWindowLog: 10 }))
    const two = Buffer.concat([
      zlib.zstdCompressSync(Buffer.from('one')),
      zlib.zstdCompressSync(Buffer.from('two')),
    ])
    assert.equal(
      new TextDecoder().decode(await lib.decompress(two, { format: 'zstd', execution })),
      'onetwo',
    )
    const skip = Buffer.alloc(11)
    skip.writeUInt32LE(0x184d2a50, 0)
    skip.writeUInt32LE(3, 4)
    skip.set([1, 2, 3], 8)
    assert.equal(
      new TextDecoder().decode(
        await lib.decompress(Buffer.concat([skip, two, skip]), { format: 'zstd', execution }),
      ),
      'onetwo',
    )
    frame[frame.length - 1] ^= 1
    await assert.rejects(lib.decompress(frame, { format: 'zstd', execution }))
    await assert.rejects(lib.compress(input, { format: 'zstd', level: 23, execution }), /level/)
    const negative = await lib.compress(input, {
      format: 'zstd',
      level: -10,
      execution,
      checksum: false,
    })
    assert.deepEqual(await lib.decompress(negative, { format: 'zstd', execution }), input)
  })
}
test('Node worker factory, initialization failure, timeout and termination', async () => {
  let worker
  let exited
  const workerFactory = () => {
    worker = new Worker(new URL('../dist/worker.js', import.meta.url))
    exited = new Promise((resolve) => worker.once('exit', resolve))
    return worker
  }
  await lib.compress(new Uint8Array([1]), { format: 'gzip', execution: 'worker', workerFactory })
  await exited
  assert.equal(worker.threadId, -1)
  await assert.rejects(
    lib.compress(new Uint8Array(), {
      format: 'gzip',
      execution: 'worker',
      initializationTimeoutMs: 50,
      workerFactory: () => new Worker('setInterval(()=>{},1000)', { eval: true }),
    }),
    /timed out/,
  )
  await assert.rejects(
    lib.compress(new Uint8Array(), {
      format: 'gzip',
      execution: 'worker',
      workerFactory: () => new Worker('throw new Error("crashed")', { eval: true }),
    }),
    /crashed/,
  )
})

test('custom worker cleanup errors do not prevent termination', async () => {
  let worker
  let exited
  const workerFactory = () => {
    worker = new Worker(new URL('../dist/worker.js', import.meta.url))
    exited = new Promise((resolve) => worker.once('exit', resolve))
    worker.off = () => {
      throw new Error('custom listener removal failed')
    }
    return worker
  }
  const input = new Uint8Array([1, 2, 3])
  const frame = await lib.compress(input, { format: 'zstd', execution: 'worker', workerFactory })
  await exited
  assert.equal(worker.threadId, -1)
  assert.deepEqual(await lib.decompress(frame, { format: 'zstd' }), input)
})

test(
  'worker setup rejects locked responses and cleans up partially registered listeners',
  { timeout: 5000 },
  () => scenarios.workerSetupFailures(lib),
)

test('WASM setup releases handles if a custom ABI fails after allocation', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'compression-abi-setup-'))
  try {
    const path = join(directory, 'failure.mjs')
    await writeFile(
      path,
      `
      import createModule from ${JSON.stringify(new URL('../dist/wasm/zstd.mjs', import.meta.url).href)};
      export const live = new Set();
      export default async options => {
        const module = await createModule(options);
        const create = module._codec_create, free = module._codec_free;
        module._codec_create = (...args) => { const handle = create(...args); live.add(handle); return handle; };
        module._codec_free = handle => { live.delete(handle); free(handle); };
        module._codec_input_capacity = () => { throw new Error('ABI setup failed'); };
        return module;
      };
    `,
    )
    const loaderUrl = pathToFileURL(path)
    await assert.rejects(
      lib.createCompressionStream({ format: 'zstd', loaderUrl }),
      /ABI setup failed/,
    )
    assert.equal((await import(loaderUrl)).live.size, 0)
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('timed out initialization and completed stream writes release input storage', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'compression-setup-memory-'))
  try {
    const path = join(directory, 'pending.mjs')
    await writeFile(path, 'export default () => new Promise(() => {})')
    const script = join(directory, 'verify.mjs')
    await writeFile(
      script,
      `
      import assert from 'node:assert/strict';
      import { compress, createCompressionStream } from ${JSON.stringify(new URL('../dist/index.js', import.meta.url).href)};
      const options = {
        format: 'zstd', loaderUrl: ${JSON.stringify(pathToFileURL(path).href)},
        dictionary: new Uint8Array(8 * 1024 * 1024), initializationTimeoutMs: 5,
      };
      const measure = async () => {
        for (let i = 0; i < 3; i++) {
          await new Promise(resolve => setImmediate(resolve));
          global.gc();
        }
        return process.memoryUsage().arrayBuffers;
      };
      const before = await measure();
      for (let i = 0; i < 5; i++) {
        await assert.rejects(compress(new Uint8Array(1), options), { name: 'TimeoutError' });
      }
      const retained = await measure() - before;
      assert(retained < 1024 * 1024, 'Timed out calls retained dictionary snapshots: ' + retained);
      async function timedOutSignal() {
        const controller = new AbortController();
        const reference = new WeakRef(controller.signal);
        await assert.rejects(compress(new Uint8Array(1), { ...options, signal: controller.signal }), { name: 'TimeoutError' });
        return reference;
      }
      const signal = await timedOutSignal();
      await measure();
      assert.equal(signal.deref(), undefined, 'Timed out initialization retained its signal');
      const pair = await createCompressionStream({ format: 'zstd' });
      const writer = pair.writable.getWriter();
      const reading = pair.readable.pipeTo(new WritableStream());
      async function write() {
        const bytes = new Uint8Array(16 * 1024 * 1024).fill(73);
        const reference = new WeakRef(bytes.buffer);
        await writer.write(bytes);
        return reference;
      }
      const reference = await write();
      await measure();
      assert.equal(reference.deref(), undefined, 'Completed write retained its input');
      await writer.close();
      writer.releaseLock();
      await reading;
    `,
    )
    execFileSync(process.execPath, ['--expose-gc', script], { stdio: 'pipe' })
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('worker initialization preserves DOMExceptions and named ordinary errors', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'compression-errors-'))
  try {
    const path = join(directory, 'failure.mjs')
    await writeFile(
      path,
      `
      const params = new URL(import.meta.url).searchParams;
      export default () => {
        const error = params.has('ordinary') ? new Error('loader failed')
          : new DOMException('loader failed', params.get('name'));
        if (params.has('ordinary')) error.name = params.get('name');
        throw error;
      };
    `,
    )
    for (const open of [lib.createCompressionStream, lib.createDecompressionStream]) {
      for (const name of [
        'DataCloneError',
        'AbortError',
        'SecurityError',
        'OperationError',
        'CustomSetupError',
        'TypeError',
      ]) {
        const loaderUrl = pathToFileURL(path)
        loaderUrl.searchParams.set('name', name)
        await assert.rejects(
          open({ format: 'zstd', execution: 'worker', loaderUrl }),
          (error) =>
            error instanceof DOMException &&
            error.name === name &&
            error.message === 'loader failed',
        )
      }
      const loaderUrl = pathToFileURL(path)
      loaderUrl.searchParams.set('name', 'TimeoutError')
      loaderUrl.searchParams.set('ordinary', 'true')
      await assert.rejects(
        open({ format: 'zstd', execution: 'worker', loaderUrl }),
        (error) =>
          error instanceof Error &&
          !(error instanceof DOMException) &&
          error.name === 'TimeoutError' &&
          error.message === 'loader failed',
      )
    }
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('worker stream transfer failure preserves DataCloneError and terminates the worker', async () => {
  let worker
  let exited
  const workerFactory = () => {
    worker = new Worker(
      `
      const { parentPort } = require('node:worker_threads');
      const post = parentPort.postMessage.bind(parentPort);
      parentPort.postMessage = (value, transfer) => {
        if (transfer.length) throw new DOMException('Stream transfer unavailable', 'DataCloneError');
        post(value, transfer);
      };
      void import(${JSON.stringify(new URL('../dist/worker.js', import.meta.url).href)});
    `,
      { eval: true },
    )
    exited = new Promise((resolve) => worker.once('exit', resolve))
    return worker
  }
  await assert.rejects(
    lib.createCompressionStream({ format: 'gzip', execution: 'worker', workerFactory }),
    (error) =>
      error instanceof DOMException &&
      error.name === 'DataCloneError' &&
      error.message === 'Stream transfer unavailable',
  )
  await exited
  assert.equal(worker.threadId, -1)
})

test('Zstd scratch offset ABI rejects out-of-range input', async () => {
  const { default: createModule } = await import('../dist/wasm/zstd.mjs')
  const module = await createModule()
  const handle = module._codec_create(0, 3, 1, 27, 0, 0)
  assert(handle)
  try {
    const capacity = module._codec_input_capacity()
    const outputCapacity = module._codec_output_capacity(handle)
    assert(capacity >= 65536)
    for (const [size, offset] of [
      [1, capacity],
      [0, capacity + 1],
      [capacity + 1, 0],
    ]) {
      assert.equal(module._codec_process_at(handle, size, 0, offset, outputCapacity), -1)
    }
    for (const bytes of [0, outputCapacity + 1]) {
      assert.equal(module._codec_process_at(handle, 0, 0, 0, bytes), -1)
    }
    const plain = Buffer.from('offset bounds')
    const frame = zlib.zstdCompressSync(plain)
    // A valid nonzero offset must still work after rejected ranges.
    module.HEAPU8.set(frame, module._codec_input(handle) + 7)
    assert.equal(module._codec_process_at(handle, frame.length, 0, 7, outputCapacity), 0)
    assert.equal(module._codec_consumed(handle), frame.length)
    assert.equal(module._codec_produced(handle), plain.length)
    const start = module._codec_output(handle)
    assert.deepEqual(Buffer.from(module.HEAPU8.slice(start, start + plain.length)), plain)
  } finally {
    module._codec_free(handle)
  }
})

test('Zstd releases every context once after success, errors, limits and cancellation', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'compression-lifecycle-'))
  try {
    const path = join(directory, 'tracked.mjs')
    const loader = new URL('../dist/wasm/zstd.mjs', import.meta.url).href
    await writeFile(
      path,
      `
      import createModule from ${JSON.stringify(loader)};
      export const stats = { created: 0, freed: 0, acquired: 0, released: 0, doubleFree: 0, live: new Set(), leases: new Set(), buffers: new Set() };
      export default async options => {
        const module = await createModule(options);
        const create = module._codec_create, free = module._codec_free;
        const acquire = module._codec_buffer_acquire, recycle = module._codec_buffer_release;
        const malloc = module._malloc, release = module._free;
        module._malloc = bytes => { const pointer = malloc(bytes); if (pointer) stats.buffers.add(pointer); return pointer; };
        module._free = pointer => { if (!stats.buffers.delete(pointer)) stats.doubleFree++; release(pointer); };
        module._codec_create = (...args) => {
          const handle = create(...args);
          if (handle) { stats.created++; stats.live.add(handle); }
          return handle;
        };
        module._codec_free = handle => {
          if (!stats.live.delete(handle)) stats.doubleFree++;
          stats.freed++; free(handle);
        };
        module._codec_buffer_acquire = (...args) => {
          const handle = acquire(...args);
          if (handle) { if (stats.leases.has(handle)) stats.doubleFree++; stats.acquired++; stats.leases.add(handle); }
          return handle;
        };
        module._codec_buffer_release = handle => {
          if (!stats.leases.delete(handle)) stats.doubleFree++;
          stats.released++; recycle(handle);
        };
        return module;
      };
    `,
    )
    const options = {
      format: 'zstd',
      loaderUrl: pathToFileURL(path),
      wasmUrl: new URL('../dist/wasm/zstd.wasm', import.meta.url),
    }
    const input = Buffer.alloc(2 * 1024 * 1024 + 13, 'lifecycle')
    const frame = await lib.compress(input, options)
    const { format, ...preparedOptions } = options
    const prepared = await lib.createZstdCodec(preparedOptions)
    for (const bytes of [input.subarray(0, 100), input]) {
      const packed = prepared.compress(bytes)
      assert.deepEqual(Buffer.from(prepared.decompress(packed)), bytes)
      assert.throws(() => prepared.decompress(packed.subarray(0, -1)))
    }
    const limited = await lib.createZstdCodec({ ...preparedOptions, maxOutputBytes: 1 })
    assert.throws(() => limited.decompress(frame), RangeError)
    assert.throws(() => limited.compress(input), RangeError)
    const sourceOptions = { ...options, maxOutputBytes: 1 }
    await assert.rejects(
      (await lib.decompressStream(new Blob([frame]).stream(), sourceOptions)).pipeTo(
        new WritableStream(),
      ),
      RangeError,
    )
    const fast = await lib.compressStream(new Blob([input]).stream(), options)
    assert.equal(
      zlib.zstdDecompressSync(new Uint8Array(await new Response(fast).arrayBuffer())).length,
      input.length,
    )
    assert.deepEqual(Buffer.from(await lib.decompress(frame, options)), input)
    const bufferAbort = new AbortController()
    const bufferReason = new Error('host cancelled incremental buffer')
    const pendingBuffer = lib.decompress(Buffer.concat(Array(32).fill(frame)), {
      ...options,
      signal: bufferAbort.signal,
    })
    const timer = setTimeout(() => bufferAbort.abort(bufferReason), 0)
    try {
      await assert.rejects(pendingBuffer, (error) => error === bufferReason)
    } finally {
      clearTimeout(timer)
    }
    await assert.rejects(lib.decompress(new Uint8Array([255, 0, 255]), options))
    await assert.rejects(lib.decompress(frame, { ...options, maxOutputBytes: 1 }), RangeError)
    const aborted = new AbortController()
    const pair = await lib.createDecompressionStream({ ...options, signal: aborted.signal })
    aborted.abort(new Error('stop initialized codec'))
    await pair.readable.cancel().catch(() => {})
    await (await lib.createCompressionStream(options)).readable.cancel(new Error('no consumer'))

    const blocked = await lib.createDecompressionStream(options)
    const writer = blocked.writable.getWriter()
    const reader = blocked.readable.getReader()
    const writing = writer.write(frame).catch((error) => error)
    assert.equal((await reader.read()).done, false)
    const reason = new Error('abort blocked decoder')
    const closed = reader.closed.catch((error) => error)
    await writer.abort(reason)
    assert.equal(await closed, reason)
    await writing
    writer.releaseLock()
    reader.releaseLock()
    await new Promise((resolve) => setImmediate(resolve))
    const { stats } = await import(pathToFileURL(path).href)
    assert(stats.created > 0)
    assert.equal(stats.live.size, 0)
    assert.equal(stats.freed, stats.created)
    assert(stats.acquired > 0)
    assert.equal(stats.leases.size, 0)
    assert.equal(stats.released, stats.acquired)
    assert.equal(stats.doubleFree, 0)
    assert.equal(stats.buffers.size, 0)
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('prepared initialization observes cancellation and timeout while sharing a pending module', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'compression-prepared-init-'))
  try {
    const path = join(directory, 'pending.mjs')
    await writeFile(path, 'export default async () => new Promise(() => {})')
    const options = { loaderUrl: pathToFileURL(path) }
    const controller = new AbortController()
    const reason = new Error('stop prepared initialization')
    const pending = lib.createZstdCodec({ ...options, signal: controller.signal })
    controller.abort(reason)
    await assert.rejects(pending, (error) => error === reason)
    await assert.rejects(
      lib.createZstdCodec({ ...options, initializationTimeoutMs: 5 }),
      (error) => error instanceof DOMException && error.name === 'TimeoutError',
    )
    const source = new ReadableStream()
    await assert.rejects(
      lib.compressStream(source, { ...options, format: 'zstd', initializationTimeoutMs: 5 }),
      (error) => error.name === 'TimeoutError',
    )
    assert.equal(source.locked, false)
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('source-bound Zstd yields to host timers and releases a cancelled processor', async () => {
  const controller = new AbortController()
  const reason = new Error('host timer cancelled codec')
  let cancelled = false
  let timerRan = false
  const input = new Uint8Array(1024 * 1024).fill(73)
  const source = new ReadableStream({
    pull(c) {
      c.enqueue(input)
    },
    cancel() {
      cancelled = true
    },
  })
  const output = await lib.compressStream(source, { format: 'zstd', signal: controller.signal })
  const timer = setTimeout(() => {
    timerRan = true
    controller.abort(reason)
  }, 10)
  try {
    await assert.rejects(output.pipeTo(new WritableStream()), (error) => error === reason)
  } finally {
    clearTimeout(timer)
  }
  assert(timerRan && cancelled)
  assert.equal(source.locked, false)
})

test('custom assets attempt bounded buffers once and retain warm synchronous execution', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'compression-warm-assets-'))
  try {
    const path = join(directory, 'tracked.mjs')
    const loader = new URL('../dist/wasm/zstd.mjs', import.meta.url).href
    await writeFile(
      path,
      `
      import createModule from ${JSON.stringify(loader)};
      export let module;
      export const stats = { calls: 0, decodeCalls: 0 };
      export default async options => {
        module = await createModule(options);
        const compress = module._codec_buffer_compress;
        const decompress = module._codec_buffer_decompress;
        module._codec_buffer_compress = (...args) => { stats.calls++; return compress(...args) };
        module._codec_buffer_decompress = (...args) => { stats.decodeCalls++; return decompress(...args) };
        return module;
      };
    `,
    )
    const settings = {
      format: 'zstd',
      loaderUrl: pathToFileURL(path),
      wasmUrl: new URL('../dist/wasm/zstd.wasm', import.meta.url),
    }
    const expanded = Buffer.alloc(128 * 1024, 'unknown-size bounded fallback')
    const reference = zlib.zstdCompressSync(expanded, {
      params: {
        [zlib.constants.ZSTD_c_contentSizeFlag]: 0,
        [zlib.constants.ZSTD_c_checksumFlag]: 1,
      },
    })
    assert.deepEqual(Buffer.from(await lib.decompress(reference, settings)), expanded)
    const tracked = await import(pathToFileURL(path).href)
    assert.equal(tracked.stats.decodeCalls, 1)
    const changing = reference.slice()
    const beforeFallback = tracked.stats.decodeCalls
    const fallback = lib.decompress(changing, settings)
    assert.equal(tracked.stats.decodeCalls, beforeFallback + 1)
    changing.fill(0)
    assert.deepEqual(Buffer.from(await fallback), expanded)
    assert.equal(
      tracked.stats.decodeCalls,
      beforeFallback + 1,
      'Warm fallback repeated the bounded decode',
    )
    const previous = tracked.stats.calls
    const pending = lib.compress(new Uint8Array([2]), settings)
    assert(pending instanceof Promise)
    assert.equal(tracked.stats.calls, previous + 1)
    assert.deepEqual(await lib.decompress(await pending, settings), new Uint8Array([2]))
    const stream = await lib.compressStream(
      new Blob([new Uint8Array(100).fill(73)]).stream(),
      settings,
    )
    const wideWindow = new Uint8Array(await new Response(stream).arrayBuffer())
    assert((wideWindow[5] >>> 3) + 10 > 16, 'Expected an unpledged stream window')
    const previousDecode = tracked.stats.decodeCalls
    const decoded = lib.decompress(wideWindow, settings)
    assert.equal(tracked.stats.decodeCalls, previousDecode + 1)
    assert.deepEqual(await decoded, new Uint8Array(100).fill(73))
    await assert.rejects(lib.decompress(wideWindow, { ...settings, maxWindowLog: 10 }))
    const pointer = tracked.module._malloc(100)
    try {
      const view = tracked.module.HEAPU8.subarray(pointer, pointer + 100)
      view.fill(73)
      const codec = await lib.createZstdCodec({
        loaderUrl: settings.loaderUrl,
        wasmUrl: settings.wasmUrl,
      })
      assert.deepEqual(codec.decompress(codec.compress(view)), new Uint8Array(100).fill(73))
      const stable = tracked.module.HEAPU8.subarray(pointer, pointer + 100)
      const allocate = tracked.module._malloc
      let growth = 0
      tracked.module._malloc = (bytes) => {
        if (!growth) {
          growth = allocate(tracked.module.HEAPU8.byteLength * 2)
        }
        return allocate(bytes)
      }
      try {
        // A dictionary allocation forces growth while the input aliases the heap.
        const withDictionary = await lib.createZstdCodec({
          loaderUrl: settings.loaderUrl,
          wasmUrl: settings.wasmUrl,
          dictionary: new Uint8Array(100).fill(42),
        })
        const frame = withDictionary.compress(stable)
        assert(stable.buffer.detached)
        assert.deepEqual(withDictionary.decompress(frame), new Uint8Array(100).fill(73))
      } finally {
        tracked.module._malloc = allocate
        if (growth) {
          tracked.module._free(growth)
        }
      }
    } finally {
      tracked.module._free(pointer)
    }
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})
