import { test } from 'node:test'
import assert from 'node:assert/strict'
import { zstdDecompressSync } from 'node:zlib'
import * as lib from '../dist/index.js'

// Parse the wire header independently of the WASM encoder.
function frameContentSize(frame) {
  const descriptor = frame[4]
  const single = Boolean(descriptor & 32)
  const flag = descriptor >>> 6
  let offset = 5 + Number(!single) + [0, 1, 2, 4][descriptor & 3]
  const bytes = flag ? 2 ** flag : Number(single)
  if (!bytes) {
    return undefined
  }
  let size = 0n
  for (let index = 0; index < bytes; index++) {
    size |= BigInt(frame[offset++]) << BigInt(index * 8)
  }
  return Number(size) + (flag === 1 ? 256 : 0)
}

for (const execution of ['direct', 'worker']) {
  test(
    `${execution}: optional Zstd content size covers empty, short, boundary and large buffers`,
    { timeout: 30000 },
    async () => {
      for (const size of [
        0,
        1,
        255,
        256,
        32768,
        32769,
        65535,
        65536,
        65537,
        65791,
        65792,
        2 * 1024 * 1024 + 19,
      ]) {
        const input = new Uint8Array(size).fill(73)
        for (const contentSize of [undefined, true, size, false]) {
          const frame = await lib.compress(input, { format: 'zstd', execution, contentSize })
          const omitted = contentSize === false || (contentSize === undefined && size > 0)
          assert.equal(frameContentSize(frame), omitted ? undefined : size)
          assert.deepEqual(Uint8Array.from(zstdDecompressSync(frame)), input)
        }
      }
    },
  )
  test(
    `${execution}: numeric content size works with streaming and dictionaries`,
    { timeout: 30000 },
    async () => {
      const dictionary = new TextEncoder().encode('dictionary sample content '.repeat(100))
      const input = dictionary.slice(0, 2000)
      const options = { format: 'zstd', execution, dictionary }
      const codec = await lib.createCompressionStream({ ...options, contentSize: input.length })
      const frame = new Uint8Array(
        await new Response(new Blob([input]).stream().pipeThrough(codec)).arrayBuffer(),
      )
      assert.equal(frameContentSize(frame), input.length)
      assert.deepEqual(await lib.decompress(frame, options), input)
      for (const size of [input.length - 1, input.length + 1]) {
        const codec = await lib.createCompressionStream({ ...options, contentSize: size })
        await assert.rejects(
          new Blob([input]).stream().pipeThrough(codec).pipeTo(new WritableStream()),
          /contentSize/,
        )
      }
    },
  )
}
test('content size validates direction, type and known length before transferring ownership', async () => {
  for (const contentSize of [null, 'true', -1, 1.5, Infinity, 4]) {
    const input = new Uint8Array(3)
    await assert.rejects(lib.compress(input, { format: 'zstd', contentSize, transferInput: true }))
    assert.equal(input.buffer.detached, false)
  }
  await assert.rejects(
    lib.createCompressionStream({ format: 'zstd', contentSize: true }),
    TypeError,
  )
  await assert.rejects(
    lib.compress(new Uint8Array(), { format: 'gzip', contentSize: true }),
    TypeError,
  )
  await assert.rejects(
    lib.decompress(new Uint8Array(), { format: 'zstd', contentSize: false }),
    TypeError,
  )
  const input = new Uint8Array([99, 1, 2, 99])
  assert.equal(
    frameContentSize(
      await lib.compress(input.subarray(1, 3), { format: 'zstd', contentSize: true }),
    ),
    2,
  )
})

test('the content size ABI preserves pledged sizes above the WASM32 range', async () => {
  const { default: createModule } = await import('../dist/wasm/zstd.mjs')
  const module = await createModule()
  for (const size of [2 ** 32 + 17, Number.MAX_SAFE_INTEGER]) {
    const handle = module._codec_create(1, 3, 1, 27, 0, 0)
    assert(handle)
    try {
      assert.equal(module._codec_set_pledged_size(handle, size, 1), 0)
      module.HEAPU8.fill(73, module._codec_input(handle), module._codec_input(handle) + 65536)
      let frame
      for (let index = 0; index < 4 && !frame; index++) {
        assert(
          module._codec_process_at(handle, 65536, 0, 0, module._codec_output_capacity(handle)) >= 0,
        )
        const bytes = module._codec_produced(handle)
        if (bytes) {
          frame = module.HEAPU8.slice(
            module._codec_output(handle),
            module._codec_output(handle) + bytes,
          )
        }
      }
      assert(frame)
      assert.equal(frameContentSize(frame), size)
      const pointer = module._codec_buffer_input()
      module.HEAPU8.set(frame.subarray(0, 18), pointer)
      assert.equal(module._codec_frame_content_size(pointer, Math.min(frame.length, 18)), size)
      assert.equal(module._codec_frame_content_size(pointer, 0), -1)
    } finally {
      module._codec_free(handle)
    }
  }
})
