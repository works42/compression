import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Worker } from 'node:worker_threads'
import * as lib from '../dist/index.js'

const input = new TextEncoder().encode('reusable worker compress '.repeat(4000))
function trackedPool(size = 1, factory, limits = {}) {
  const workers = []
  const pool = lib.createWorkerPool({
    size,
    ...limits,
    workerFactory: () => {
      const worker = factory
        ? factory(workers.length)
        : new Worker(new URL('../dist/worker.js', import.meta.url))
      workers.push(worker)
      return worker
    },
  })
  return { pool, workers, options: { format: 'zstd', execution: 'worker', workerPool: pool } }
}

test(
  'a single reusable worker handles sequential buffers, streams and native formats',
  { timeout: 30000 },
  async () => {
    const { pool, workers, options } = trackedPool()
    try {
      for (let i = 0; i < 10; i++) {
        const frame = await lib.compress(input, { ...options, contentSize: true })
        assert.deepEqual(await lib.decompress(frame, options), input)
      }
      for (const format of lib.formats) {
        const codec = await lib.createCompressionStream({ ...options, format })
        const packed = new Response(new Blob([input]).stream().pipeThrough(codec)).arrayBuffer()
        assert.deepEqual(await lib.decompress(await packed, { ...options, format }), input)
      }
      assert.equal(workers.length, 1)
      assert(workers[0].threadId > 0)
      assert.equal(workers[0].listenerCount('message'), 0)
      assert.equal(workers[0].listenerCount('error'), 1)
    } finally {
      await pool.close()
    }
    assert.equal(workers[0].threadId, -1)
    await pool.close()
    await assert.rejects(lib.compress(input, options), { name: 'AbortError' })
  },
)

test('pool bounds concurrency and drains queued operations', { timeout: 30000 }, async () => {
  const { pool, workers, options } = trackedPool(2)
  try {
    const results = await Promise.all(
      Array.from({ length: 12 }, async (_, index) => {
        const bytes = new Uint8Array(100000 + index).fill(index)
        const frame = await lib.compress(bytes, options)
        return assert.deepEqual(await lib.decompress(frame, options), bytes)
      }),
    )
    assert.equal(results.length, 12)
    assert.equal(workers.length, 2)
  } finally {
    await pool.close()
  }
})

test(
  'queued cancellation and timeouts remove waiters without disturbing an active worker',
  { timeout: 30000 },
  async () => {
    const { pool, workers, options } = trackedPool()
    try {
      const held = await lib.createCompressionStream(options)
      const controller = new AbortController()
      const reason = new Error('cancel queued')
      const waiting = lib.compress(input, { ...options, signal: controller.signal })
      const rejected = assert.rejects(waiting, (error) => error === reason)
      controller.abort(reason)
      await rejected
      await assert.rejects(lib.compress(input, { ...options, initializationTimeoutMs: 20 }), {
        name: 'TimeoutError',
      })
      // Finish the held operation successfully; queued failures must not evict it.
      const reading = new Response(held.readable).arrayBuffer()
      const writer = held.writable.getWriter()
      await writer.write(input)
      await writer.close()
      await reading
      assert.deepEqual(await lib.decompress(await lib.compress(input, options), options), input)
      assert.equal(workers.length, 1)
    } finally {
      await pool.close()
    }
  },
)

test(
  'pool replaces workers after stream cancellation, codec errors and initialization errors',
  { timeout: 30000 },
  async () => {
    const { pool, workers, options } = trackedPool()
    try {
      const held = await lib.createCompressionStream(options)
      await held.readable.cancel()
      await assert.rejects(lib.decompress(new Uint8Array([255]), options))
      await assert.rejects(lib.compress(input, { ...options, level: 100 }), RangeError)
      assert.deepEqual(await lib.decompress(await lib.compress(input, options), options), input)
      assert.equal(workers.length, 4)
    } finally {
      await pool.close()
    }
  },
)

test('pool close aborts active streams and queued operations', { timeout: 30000 }, async () => {
  const { pool, workers, options } = trackedPool()
  const held = await lib.createCompressionStream(options)
  const writer = held.writable.getWriter()
  const reader = held.readable.getReader()
  const rejected = [
    assert.rejects(reader.read(), { name: 'AbortError' }),
    assert.rejects(writer.closed, { name: 'AbortError' }),
    assert.rejects(lib.compress(input, options), { name: 'AbortError' }),
  ]
  await pool.close()
  await Promise.all(rejected)
  assert.equal(workers[0].threadId, -1)
})

test('an idle worker exit is observed and replaced', { timeout: 30000 }, async () => {
  const { pool, workers, options } = trackedPool()
  try {
    await lib.compress(input, options)
    await workers[0].terminate()
    assert.deepEqual(await lib.decompress(await lib.compress(input, options), options), input)
    assert.equal(workers.length, 2)
  } finally {
    await pool.close()
  }
})

test('message deserialization errors replace active and idle Node workers', async () => {
  const { pool, workers, options } = trackedPool()
  try {
    const pair = await lib.createCompressionStream(options)
    const reader = pair.readable.getReader()
    const rejected = assert.rejects(reader.read(), /could not decode message/)
    workers[0].emit('messageerror', new Error('could not decode message'))
    await rejected
    reader.releaseLock()
    await lib.compress(input, options)
    assert.equal(workers.length, 2)
    workers[1].emit('messageerror', new Error('idle decode error'))
    assert.deepEqual(await lib.decompress(await lib.compress(input, options), options), input)
    assert.equal(workers.length, 3)
  } finally {
    await pool.close()
  }
})

test('queued worker operations capture mutable asset URLs before initialization', async () => {
  const { pool, options } = trackedPool()
  try {
    const held = await lib.createCompressionStream(options)
    const loaderUrl = new URL('../dist/wasm/zstd.mjs', import.meta.url)
    const wasmUrl = new URL('../dist/wasm/zstd.wasm', import.meta.url)
    const pending = lib.compress(input, { ...options, loaderUrl, wasmUrl })
    loaderUrl.pathname += '.changed'
    wasmUrl.pathname += '.changed'
    const collecting = new Response(held.readable).arrayBuffer()
    const writer = held.writable.getWriter()
    await writer.close()
    writer.releaseLock()
    await collecting
    assert.deepEqual(await lib.decompress(await pending, options), input)
  } finally {
    await pool.close()
  }
})

test(
  'pool recovers from worker startup failure and initialization timeout',
  { timeout: 30000 },
  async () => {
    for (const code of ['throw new Error("pool crash")', 'setInterval(() => {}, 1000)']) {
      const { pool, options } = trackedPool(1, (index) =>
        index === 0
          ? new Worker(code, { eval: true })
          : new Worker(new URL('../dist/worker.js', import.meta.url)),
      )
      try {
        await assert.rejects(
          lib.compress(input, { ...options, initializationTimeoutMs: 300 }),
          /pool crash|timed out/,
        )
        assert.deepEqual(await lib.decompress(await lib.compress(input, options), options), input)
      } finally {
        await pool.close()
      }
    }
  },
)

test('validates pool options before detaching input', async () => {
  for (const options of [
    { size: 0 },
    { size: 1.5 },
    { workerFactory: 42 },
    { unknown: true },
    { maxQueuedOperations: -1 },
    { maxQueuedOperations: 1.5 },
    { maxPendingBytes: NaN },
    { maxPendingBytes: '64' },
    null,
  ]) {
    assert.throws(() => lib.createWorkerPool(options), TypeError)
  }
  const pool = lib.createWorkerPool()
  try {
    for (const extra of [
      { workerPool: {} },
      { workerPool: pool },
      { execution: 'worker', workerPool: pool, workerFactory() {} },
    ]) {
      const bytes = new Uint8Array([1])
      await assert.rejects(
        lib.compress(bytes, { format: 'zstd', transferInput: true, ...extra }),
        TypeError,
      )
      assert.equal(bytes.buffer.detached, false)
    }
  } finally {
    await pool.close()
  }
})

test('pool admission rejects before transfer and queued tasks preserve their input snapshot', async () => {
  const { pool, workers, options } = trackedPool(1, undefined, { maxQueuedOperations: 1 })
  try {
    const held = await lib.createCompressionStream(options)
    const bytes = new Uint8Array([1, 2, 3])
    const expected = bytes.slice()
    const pending = lib.compress(bytes, options)
    bytes.fill(99)
    const rejectedInput = new Uint8Array([42])
    await assert.rejects(
      lib.compress(rejectedInput, { ...options, transferInput: true }),
      (error) =>
        error instanceof lib.WorkerPoolCapacityError && error.limit === 'maxQueuedOperations',
    )
    assert.equal(rejectedInput.buffer.detached, false)
    const detachedDictionary = new Uint8Array([1])
    structuredClone(detachedDictionary, { transfer: [detachedDictionary.buffer] })
    // Copying this dictionary would throw TypeError; admission must fail first.
    await assert.rejects(
      lib.compress(rejectedInput, { ...options, dictionary: detachedDictionary }),
      lib.WorkerPoolCapacityError,
    )
    await assert.rejects(lib.createDecompressionStream(options), lib.WorkerPoolCapacityError)
    assert.equal(workers.length, 1)
    const output = new Response(held.readable).arrayBuffer()
    await held.writable.close()
    await output
    const frame = await pending
    assert.deepEqual(await lib.decompress(frame, options), expected)
  } finally {
    await pool.close()
  }
})

test('pending bytes include dictionaries and transferred backing buffers until completion', async () => {
  const { pool, options } = trackedPool(1, undefined, { maxPendingBytes: 32 })
  try {
    const held = await lib.createCompressionStream({ ...options, dictionary: new Uint8Array(8) })
    const controller = new AbortController()
    const queued = lib.compress(new Uint8Array(16), {
      ...options,
      dictionary: new Uint8Array(8),
      signal: controller.signal,
    })
    const cancelled = assert.rejects(queued, { name: 'AbortError' })
    await assert.rejects(
      lib.compress(new Uint8Array(1), options),
      (error) => error instanceof lib.WorkerPoolCapacityError && error.limit === 'maxPendingBytes',
    )
    controller.abort()
    await cancelled
    const storage = new Uint8Array(25)
    await assert.rejects(
      lib.compress(storage.subarray(0, 1), { ...options, transferInput: true }),
      lib.WorkerPoolCapacityError,
    )
    assert.equal(storage.buffer.detached, false)
    // An ordinary subview only copies its visible bytes.
    const pending = lib.compress(storage.subarray(0, 1), options)
    const output = new Response(held.readable).arrayBuffer()
    await held.writable.close()
    await output
    assert.deepEqual(await lib.decompress(await pending, options), new Uint8Array(1))
    // A failure before a worker is acquired must also return the reservation.
    await assert.rejects(
      lib.compress(new Uint8Array(new SharedArrayBuffer(32)), {
        ...options,
        transferInput: true,
      }),
      TypeError,
    )
    const detachedDictionary = new Uint8Array([1])
    structuredClone(detachedDictionary, { transfer: [detachedDictionary.buffer] })
    await assert.rejects(
      lib.createCompressionStream({ ...options, dictionary: detachedDictionary }),
      TypeError,
    )
    const frame = await lib.compress(new Uint8Array(32), options)
    assert.deepEqual(await lib.decompress(frame, options), new Uint8Array(32))
  } finally {
    await pool.close()
  }
})

test('zero queue capacity applies during startup, and completion restores capacity', async () => {
  const { pool, options } = trackedPool(2, undefined, { maxQueuedOperations: 0 })
  try {
    const first = lib.compress(new Uint8Array([1]), options)
    const second = lib.compress(new Uint8Array([2]), options)
    await assert.rejects(lib.compress(new Uint8Array([3]), options), lib.WorkerPoolCapacityError)
    await Promise.all([first, second])
    assert.deepEqual(
      await lib.decompress(await lib.compress(new Uint8Array([3]), options), options),
      new Uint8Array([3]),
    )
  } finally {
    await pool.close()
  }
})

test('timeouts, initialization errors, cancellation and close return admission capacity', async () => {
  const { pool, options } = trackedPool(1, undefined, {
    maxQueuedOperations: 1,
    maxPendingBytes: 32,
  })
  try {
    const held = await lib.createCompressionStream(options)
    await assert.rejects(
      lib.compress(new Uint8Array(32), {
        ...options,
        initializationTimeoutMs: 20,
      }),
      { name: 'TimeoutError' },
    )
    const abort = new AbortController()
    const pending = lib.compress(new Uint8Array(32), { ...options, signal: abort.signal })
    const rejected = assert.rejects(pending, { name: 'AbortError' })
    abort.abort()
    await rejected
    await held.readable.cancel()
    await assert.rejects(lib.compress(new Uint8Array(32), { ...options, level: 100 }), RangeError)
    const frame = await lib.compress(new Uint8Array(32), options)
    assert.deepEqual(await lib.decompress(frame, options), new Uint8Array(32))
    await pool.close()
    const input = new Uint8Array([1])
    await assert.rejects(lib.compress(input, { ...options, transferInput: true }), {
      name: 'AbortError',
    })
    assert.equal(input.buffer.detached, false)
  } finally {
    await pool.close()
  }
})

test('source helpers share admission limits, and rejected sources stay unlocked', async () => {
  const { pool, options } = trackedPool(1, undefined, {
    maxQueuedOperations: 0,
    maxPendingBytes: 0,
  })
  try {
    const held = await lib.createDecompressionStream(options)
    const source = new Blob([new Uint8Array([1])]).stream()
    await assert.rejects(lib.compressStream(source, options), lib.WorkerPoolCapacityError)
    assert.equal(source.locked, false)
    await held.readable.cancel()
    const stream = await lib.compressStream(source, options)
    assert((await new Response(stream).arrayBuffer()).byteLength > 0)
    await assert.rejects(
      lib.createCompressionStream({ ...options, dictionary: new Uint8Array(1) }),
      lib.WorkerPoolCapacityError,
    )
    await (await lib.createCompressionStream(options)).readable.cancel()
  } finally {
    await pool.close()
  }
})

test('unbounded admission is an explicit opt-in and zero-byte inputs fit a zero-byte budget', async () => {
  const pool = lib.createWorkerPool({ maxQueuedOperations: Infinity, maxPendingBytes: 0 })
  const options = { format: 'zstd', execution: 'worker', workerPool: pool }
  try {
    assert((await lib.compress(new Uint8Array(), options)).length > 0)
    await assert.rejects(lib.compress(new Uint8Array(1), options), lib.WorkerPoolCapacityError)
  } finally {
    await pool.close()
  }
  await lib.createWorkerPool({ maxPendingBytes: Infinity }).close()
})
