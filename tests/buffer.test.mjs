import { test } from 'node:test'
import assert from 'node:assert/strict'
import { constants, zstdCompressSync, zstdDecompressSync } from 'node:zlib'
import { randomFillSync } from 'node:crypto'
import * as lib from '../dist/index.js'
import { preparedZstd } from './scenarios.mjs'

const options = { format: 'zstd' }

test('warm async buffer calls retain Promise, input ownership and immediate cancellation semantics', async () => {
  await lib.compress(new Uint8Array([1]), options)
  for (const transferInput of [false, true]) {
    const input = new Uint8Array([99, 1, 2, 3, 99])
    const expected = input.slice(1, 4)
    const pending = lib.compress(input.subarray(1, 4), { ...options, transferInput })
    assert(pending instanceof Promise)
    if (transferInput) {
      assert.equal(input.buffer.detached, true)
    } else {
      input.fill(0)
    }
    const frame = await pending
    assert.deepEqual(await lib.decompress(frame, options), expected)
    const controller = new AbortController()
    const reason = new Error('abort warm buffer delivery')
    const result = lib.decompress(frame, { ...options, signal: controller.signal })
    controller.abort(reason)
    await assert.rejects(result, (error) => error === reason)
    const invalid = new AbortController()
    const failure = lib.compress(expected, {
      ...options,
      maxOutputBytes: 0,
      signal: invalid.signal,
    })
    invalid.abort(reason)
    await assert.rejects(failure, (error) => error === reason)
  }
})

test('prepared Zstd codecs preserve synchronous ownership, limits, validation and recovery', async () => {
  await preparedZstd(lib)
})

test('prepared Zstd works without Web Streams and interoperates with the native codec', async () => {
  const codec = await lib.createZstdCodec({ contentSize: true })
  const original = {
    ReadableStream: globalThis.ReadableStream,
    WritableStream: globalThis.WritableStream,
  }
  const unavailable = class {
    constructor() {
      throw new Error('Unexpected Web Streams dependency')
    }
  }
  Object.assign(globalThis, { ReadableStream: unavailable, WritableStream: unavailable })
  try {
    for (const size of [0, 64, 65536, 65537, 2 * 1024 * 1024 + 13]) {
      const input = randomFillSync(Buffer.alloc(size))
      const frame = codec.compress(input)
      assert.deepEqual(Uint8Array.from(zstdDecompressSync(frame)), Uint8Array.from(input))
      assert.deepEqual(codec.decompress(zstdCompressSync(input)), Uint8Array.from(input))
    }
  } finally {
    Object.assign(globalThis, original)
  }
})

test('prepared codecs preserve the existing async API options and metadata at every size', async () => {
  for (const contentSize of [undefined, false, true, 65537]) {
    const codec = await lib.createZstdCodec({ contentSize })
    for (const size of typeof contentSize === 'number' ? [contentSize] : [0, 1, 65536, 65537]) {
      const input = new Uint8Array(size).fill(73)
      assert.deepEqual(
        codec.compress(input),
        await lib.compress(input, { ...options, contentSize }),
      )
    }
    if (typeof contentSize === 'number') {
      assert.throws(() => codec.compress(new Uint8Array(1)), RangeError)
    }
  }
})

test('prepared codecs remain isolated from suspended async and streaming operations', async () => {
  const codec = await lib.createZstdCodec({ level: -3, checksum: false, contentSize: true })
  const input = randomFillSync(new Uint8Array(2 * 1024 * 1024))
  const pending = lib.compress(input, options)
  const syncFrame = codec.compress(new Uint8Array(90000).fill(73))
  assert.equal(codec.decompress(syncFrame).length, 90000)
  assert.deepEqual(await lib.decompress(await pending, options), input)
  const stream = await lib.createCompressionStream({ ...options, level: 7 })
  const reader = stream.readable.getReader()
  const writer = stream.writable.getWriter()
  const reading = (async () => {
    const chunks = []
    for (;;) {
      const { value, done } = await reader.read()
      if (done) {
        return Buffer.concat(chunks)
      }
      chunks.push(value)
    }
  })()
  await writer.write(input.subarray(0, 65536))
  codec.compress(input.subarray(0, 90000))
  await writer.write(input.subarray(65536))
  await writer.close()
  assert.deepEqual(Uint8Array.from(zstdDecompressSync(await reading)), input)
})

test('direct Zstd buffer APIs handle bounded and incremental data without Web Streams', async () => {
  await lib.compress(new Uint8Array([1]), options)
  const original = {
    ReadableStream: globalThis.ReadableStream,
    WritableStream: globalThis.WritableStream,
  }
  const unavailable = class {
    constructor() {
      throw new Error('Unexpected Web Streams dependency')
    }
  }
  Object.assign(globalThis, { ReadableStream: unavailable, WritableStream: unavailable })
  try {
    for (const size of [0, 64, 65536, 65537, 2 * 1024 * 1024 + 13]) {
      const plain = randomFillSync(new Uint8Array(size))
      const frame = await lib.compress(plain, options)
      assert.deepEqual(Uint8Array.from(zstdDecompressSync(frame)), plain)
      assert.deepEqual(await lib.decompress(frame, options), plain)
    }
    const frame = zstdCompressSync(Buffer.alloc(70000, 'concatenated'))
    assert.equal((await lib.decompress(Buffer.concat([frame, frame]), options)).length, 140000)
    await assert.rejects(lib.decompress(frame.subarray(0, -1), options), /truncated/i)
  } finally {
    Object.assign(globalThis, original)
  }
})

test('small buffers reset cached levels, checksum, size and dictionary state after failures', async () => {
  const dictionary = new TextEncoder().encode(
    'shared record keys value timestamp route '.repeat(80),
  )
  const input = dictionary.slice(17, 1117)
  for (const level of [0, 1, 7, -3, 3]) {
    for (const contentSize of [true, false]) {
      const settings = { ...options, level, checksum: contentSize, contentSize, dictionary }
      const frame = await lib.compress(input, settings)
      assert.deepEqual(await lib.decompress(frame, { ...options, dictionary }), input)
      const plainFrame = await lib.compress(input, options)
      assert.deepEqual(Uint8Array.from(zstdDecompressSync(plainFrame)), input)
      assert.deepEqual(await lib.decompress(plainFrame, options), input)
      const damaged = plainFrame.slice()
      damaged[damaged.length - 1] ^= 1
      await assert.rejects(lib.decompress(damaged, options))
      await assert.rejects(lib.compress(input, { ...options, maxOutputBytes: 0 }), RangeError)
      assert.deepEqual(await lib.decompress(await lib.compress(input, options), options), input)
    }
  }
})

test('buffer dictionary snapshots precede input transfer, even with shared backing storage', async () => {
  const storage = new TextEncoder().encode('shared dictionary and payload '.repeat(100))
  const expected = storage.slice(19, 700)
  const dictionary = storage.subarray(0, 900)
  const savedDictionary = dictionary.slice()
  const pending = lib.compress(storage.subarray(19, 700), {
    ...options,
    dictionary,
    transferInput: true,
  })
  assert.equal(storage.buffer.detached, true)
  const frame = await pending
  assert.deepEqual(
    await lib.decompress(frame, { ...options, dictionary: savedDictionary }),
    expected,
  )

  const changing = savedDictionary.slice()
  const compressing = lib.compress(expected, { ...options, dictionary: changing })
  changing.fill(0)
  assert.deepEqual(
    await lib.decompress(await compressing, { ...options, dictionary: savedDictionary }),
    expected,
  )
})

test('small frame limits are exact for known/unknown sizes, empty frames and decoder windows', async () => {
  for (const size of [0, 1, 64, 65536]) {
    const input = new Uint8Array(size).fill(73)
    for (const contentSize of [true, false]) {
      const frame = await lib.compress(input, { ...options, contentSize })
      assert.deepEqual(await lib.decompress(frame, { ...options, maxOutputBytes: size }), input)
      assert.deepEqual(
        await lib.compress(input, { ...options, contentSize, maxOutputBytes: frame.length }),
        frame,
      )
      await assert.rejects(
        lib.compress(input, { ...options, contentSize, maxOutputBytes: frame.length - 1 }),
        RangeError,
      )
      if (size) {
        await assert.rejects(
          lib.decompress(frame, { ...options, maxOutputBytes: size - 1 }),
          RangeError,
        )
      }
    }
  }
  const input = new Uint8Array(65536).fill(73)
  const frame = await lib.compress(input, options)
  await assert.rejects(lib.decompress(frame, { ...options, maxWindowLog: 10 }))
  assert.deepEqual(await lib.decompress(frame, { ...options, maxWindowLog: 16 }), input)
})

test('buffer work pledges its length while leaving the default frame size field omitted', async () => {
  for (const size of [64, 65536, 65537, 2 * 1024 * 1024]) {
    const frame = await lib.compress(new Uint8Array(size).fill(73), options)
    assert.equal(frame[4] & 0xe0, 0, 'Default content size must remain omitted')
    const windowDescriptor = frame[5]
    const exponent = 10 + (windowDescriptor >>> 3)
    const window = 2 ** exponent + (2 ** exponent / 8) * (windowDescriptor & 7)
    // The encoder can choose a smaller window than the complete input.
    assert(
      window <= Math.max(1024, 2 ** Math.ceil(Math.log2(size))),
      `Unexpected oversized window ${window} for ${size} bytes`,
    )
    assert.equal(zstdDecompressSync(frame).length, size)
  }
})

test('64/128/256 KiB boundaries preserve native interop, output limits and owned results', async () => {
  for (const contentSize of [true, false]) {
    const codec = await lib.createZstdCodec({ contentSize })
    for (const size of [65535, 65536, 65537, 131071, 131072, 131073, 262143, 262144, 262145]) {
      for (const random of [false, true]) {
        const input = new Uint8Array(size).fill(73)
        if (random) randomFillSync(input)
        const frame = await lib.compress(input, { ...options, contentSize })
        assert.deepEqual(Uint8Array.from(zstdDecompressSync(frame)), input)
        assert.deepEqual(codec.compress(input), frame)
        assert.deepEqual(codec.decompress(frame), input)
        assert.deepEqual(await lib.decompress(frame, { ...options, maxOutputBytes: size }), input)
        await assert.rejects(
          lib.decompress(frame, { ...options, maxOutputBytes: size - 1 }),
          RangeError,
        )
        await assert.rejects(
          lib.compress(input, {
            ...options,
            contentSize,
            maxOutputBytes: frame.length - 1,
          }),
          RangeError,
        )
        const saved = frame.slice()
        codec.compress(input)
        assert.deepEqual(frame, saved)
      }
    }
  }
})

test('reused incremental buffers clear dictionary, window, size and checksum state after errors', async () => {
  const dictionary = new TextEncoder().encode('request path timestamp response status '.repeat(100))
  const input = new Uint8Array(192 * 1024 + 1)
  for (let i = 0; i < input.length; i++) input[i] = dictionary[i % dictionary.length]
  for (const level of [7, -3, 3]) {
    const codec = await lib.createZstdCodec({
      dictionary,
      level,
      contentSize: true,
      checksum: false,
    })
    const frame = codec.compress(input)
    assert.deepEqual(codec.decompress(frame), input)
    assert.throws(() => codec.decompress(frame.subarray(0, -1)))
    const plain = await lib.compress(input, options)
    assert.deepEqual(Uint8Array.from(zstdDecompressSync(plain)), input)
    assert.equal(plain[4] & 4, 4)
    await assert.rejects(lib.decompress(plain, { ...options, maxWindowLog: 10 }))
    const damaged = plain.slice()
    damaged[damaged.length - 1] ^= 1
    await assert.rejects(lib.decompress(damaged, options))
    assert.deepEqual(await lib.decompress(plain, options), input)
    assert.deepEqual(codec.decompress(codec.compress(input)), input)
  }
})

test('cached incremental handles are leased exclusively and reset after partial work', async () => {
  const { default: createModule } = await import('../dist/wasm/zstd.mjs')
  const module = await createModule()
  const first = module._codec_buffer_acquire(1, 3, 1, 27, 0, 0)
  const second = module._codec_buffer_acquire(1, 7, 0, 27, 0, 0)
  assert(first && second && first !== second)
  // Return a partially processed frame, as cancellation would do.
  const pointer = module._codec_input(first)
  module.HEAPU8.fill(73, pointer, pointer + 100)
  assert(module._codec_process_at(first, 100, 0, 0, 65536) >= 0)
  module._codec_buffer_release(first)
  const reused = module._codec_buffer_acquire(1, 3, 1, 27, 0, 0)
  assert.equal(reused, first)
  assert.notEqual(reused, second)
  module.HEAPU8.fill(42, pointer, pointer + 100)
  assert.equal(module._codec_set_pledged_size(reused, 100, 1), 0)
  assert.equal(module._codec_process_at(reused, 100, 1, 0, 65536), 0)
  const output = module._codec_output(reused)
  const frame = module.HEAPU8.slice(output, output + module._codec_produced(reused))
  assert.deepEqual(Uint8Array.from(zstdDecompressSync(frame)), new Uint8Array(100).fill(42))
  module._codec_buffer_release(reused)
  module._codec_buffer_release(second)
})

test('large known/unknown/concatenated frames preserve exact fixed owned bytes and limits', async () => {
  const plain = new Uint8Array(8 * 1024 ** 2 + 17)
  for (let i = 0; i < plain.length; i++) plain[i] = i % 251
  const prepared = await lib.createZstdCodec()
  for (const contentSize of [true, false]) {
    const frame = zstdCompressSync(plain, {
      params: { [constants.ZSTD_c_contentSizeFlag]: Number(contentSize) },
    })
    const output = await lib.decompress(frame, { ...options, maxOutputBytes: plain.length })
    assert.deepEqual(output, plain)
    assert.equal(output.byteOffset, 0)
    assert.equal(output.buffer.byteLength, output.byteLength)
    assert.equal(output.buffer.resizable, false)
    assert.deepEqual(prepared.decompress(frame), plain)
    const saved = output.slice()
    await lib.decompress(frame, options)
    assert.deepEqual(output, saved)
    const roundTrip = prepared.compress(plain)
    assert.deepEqual(Uint8Array.from(zstdDecompressSync(roundTrip)), plain)
    await assert.rejects(
      lib.decompress(frame, { ...options, maxOutputBytes: plain.length - 1 }),
      RangeError,
    )
    await assert.rejects(lib.decompress(frame.subarray(0, -1), options))
    assert.deepEqual(prepared.decompress(frame), plain)
    const concatenated = new Uint8Array(frame.length * 2)
    concatenated.set(frame)
    concatenated.set(frame, frame.length)
    const doubled = new Uint8Array(plain.length * 2)
    doubled.set(plain)
    doubled.set(plain, plain.length)
    assert.deepEqual(await lib.decompress(concatenated, options), doubled)
    assert.deepEqual(prepared.decompress(concatenated), doubled)
  }
})

test('native and Worker buffer collectors retain complete output and byte limits', async () => {
  const plain = new Uint8Array(2 * 1024 ** 2 + 11)
  randomFillSync(plain)
  for (const format of ['gzip', 'zstd']) {
    for (const execution of ['direct', 'worker']) {
      const settings = { format, execution }
      const frame = await lib.compress(plain, settings)
      const output = await lib.decompress(frame, { ...settings, maxOutputBytes: plain.length })
      assert.deepEqual(output, plain)
      assert.equal(output.buffer.byteLength, output.byteLength)
      await assert.rejects(
        lib.decompress(frame, { ...settings, maxOutputBytes: plain.length - 1 }),
        RangeError,
      )
    }
  }
})
