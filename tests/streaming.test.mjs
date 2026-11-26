import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { Duplex } from 'node:stream'
import { createZstdDecompress, zstdCompressSync, constants } from 'node:zlib'
import { Worker } from 'node:worker_threads'
import * as lib from '../dist/index.js'

const MiB = 1024 ** 2
const lengthMiB = Number(process.env.COMPRESSION_LONG_MIB ?? 64)
if (!Number.isSafeInteger(lengthMiB) || lengthMiB < 1) {
  throw new Error('COMPRESSION_LONG_MIB must be a positive integer')
}
const sample = new Uint8Array(MiB)
let seed = 1234
for (let i = 0; i < sample.length; i++) {
  seed ^= seed << 13
  seed ^= seed >>> 17
  seed ^= seed << 5
  sample[i] = i < MiB / 2 ? seed & 255 : i % 251
}
const frame = zstdCompressSync(sample, { params: { [constants.ZSTD_c_checksumFlag]: 1 } })
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const settle = (pending) =>
  pending.then(
    (value) => ({ value }),
    (error) => ({ error }),
  )

for (const execution of ['direct', 'worker']) {
  if (lengthMiB >= 4096) {
    test(
      `${execution}: skips a valid payload above 2 GiB before decoding the next frame`,
      { timeout: 120000 },
      async () => {
        const bytes = 2 ** 31 + 65536
        const header = new Uint8Array(8)
        const view = new DataView(header.buffer)
        view.setUint32(0, 0x184d2a50, true)
        view.setUint32(4, bytes, true)
        const tail = zstdCompressSync(new Uint8Array([1, 2, 3]))
        let sentHeader = false
        let skipped = 0
        let sentTail = false
        const zeroes = new Uint8Array(65536)
        const output = []
        const source = new ReadableStream(
          {
            pull(controller) {
              if (!sentHeader) {
                sentHeader = true
                controller.enqueue(header)
                return
              }
              if (skipped < bytes) {
                skipped += zeroes.length
                controller.enqueue(zeroes.slice())
                return
              }
              if (!sentTail) {
                sentTail = true
                controller.enqueue(tail)
                return
              }
              controller.close()
            },
          },
          { highWaterMark: 1 },
        )
        await source
          .pipeThrough(
            await lib.createDecompressionStream({
              format: 'zstd',
              execution,
              maxInputBytes: 8 + bytes + tail.length,
              maxOutputBytes: 3,
            }),
          )
          .pipeTo(
            new WritableStream({
              write(chunk) {
                output.push(...chunk)
              },
            }),
          )
        assert.equal(skipped, bytes)
        assert.deepEqual(output, [1, 2, 3])
      },
    )
  }
  for (const api of ['pair', 'source']) {
    test(
      `${execution}/${api}: continuous ${lengthMiB} MiB stream interoperates without collecting input or output`,
      { timeout: 120000 },
      async () => {
        const expected = createHash('sha256')
        const actual = createHash('sha256')
        let produced = 0
        let consumed = 0
        const total = lengthMiB * MiB
        const source = new ReadableStream(
          {
            pull(controller) {
              if (produced === total) {
                return controller.close()
              }
              const offset = produced % MiB
              const chunk = sample.slice(offset, offset + Math.min(65536, total - produced))
              produced += chunk.length
              expected.update(chunk)
              controller.enqueue(chunk)
            },
          },
          { highWaterMark: 1 },
        )
        const compressed =
          api === 'pair'
            ? source.pipeThrough(await lib.createCompressionStream({ format: 'zstd', execution }))
            : await lib.compressStream(source, { format: 'zstd', execution })
        await compressed.pipeThrough(Duplex.toWeb(createZstdDecompress())).pipeTo(
          new WritableStream({
            write(chunk) {
              consumed += chunk.length
              actual.update(chunk)
            },
          }),
        )
        assert.equal(consumed, total)
        assert.equal(actual.digest('hex'), expected.digest('hex'))
      },
    )
  }

  for (const operation of ['compress', 'decompress']) {
    test(
      `${execution} ${operation}: backpressure stops input and cancellation releases the worker`,
      { timeout: 15000 },
      async () => {
        let exited
        let worker
        let pulls = 0
        let cancelled = false
        const options = { format: 'zstd', execution }
        if (execution === 'worker') {
          options.workerFactory = () => {
            worker = new Worker(new URL('../dist/worker.js', import.meta.url))
            exited = new Promise((resolve) => worker.once('exit', resolve))
            return worker
          }
        }
        const pair =
          operation === 'compress'
            ? await lib.createCompressionStream(options)
            : await lib.createDecompressionStream(options)
        const input = operation === 'compress' ? sample : frame
        const source = new ReadableStream(
          {
            pull(controller) {
              pulls++
              controller.enqueue(Uint8Array.from(input))
            },
            cancel() {
              cancelled = true
            },
          },
          { highWaterMark: 1 },
        )
        const piping = settle(source.pipeTo(pair.writable))
        const reader = pair.readable.getReader()
        try {
          assert.equal((await reader.read()).done, false)
          // Give transferable streams time to fill their bounded protocol queues.
          await sleep(100)
          const stalled = pulls
          await sleep(200)
          assert.equal(pulls, stalled, 'Input continued while the output consumer was blocked')
          const reason = new Error('stop slow consumer')
          await reader.cancel(reason)
          assert.equal((await piping).error, reason)
          assert(cancelled)
          if (exited) {
            await exited
            assert.equal(worker.threadId, -1)
          }
        } finally {
          await reader.cancel().catch(() => {})
          reader.releaseLock()
          if (worker && worker.threadId !== -1) {
            await worker.terminate()
          }
        }
      },
    )
  }
  test(
    `${execution}/source: decodes ${lengthMiB} MiB of concatenated native frames with exact total limits`,
    { timeout: 120000 },
    async () => {
      let produced = 0
      let consumed = 0
      const expected = createHash('sha256')
      const actual = createHash('sha256')
      for (let index = 0; index < lengthMiB; index++) {
        expected.update(sample)
      }
      const source = new ReadableStream(
        {
          pull(controller) {
            if (produced === lengthMiB) {
              return controller.close()
            }
            produced++
            controller.enqueue(frame)
          },
        },
        { highWaterMark: 1 },
      )
      const decoded = await lib.decompressStream(source, {
        format: 'zstd',
        execution,
        maxInputBytes: frame.length * lengthMiB,
        maxOutputBytes: MiB * lengthMiB,
      })
      await decoded.pipeTo(
        new WritableStream({
          write(chunk) {
            consumed += chunk.length
            actual.update(chunk)
          },
        }),
      )
      assert.equal(consumed, MiB * lengthMiB)
      assert.equal(actual.digest('hex'), expected.digest('hex'))
    },
  )
}
