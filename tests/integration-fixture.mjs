import * as lib from '@works42/compression'

function assert(value, message) {
  if (!value) {
    throw new Error(message)
  }
}

const equal = (actual, expected) =>
  actual.length === expected.length && actual.every((byte, i) => byte === expected[i])
const input = new TextEncoder().encode('installed package runtime test '.repeat(3000))
const prepared = await lib.createZstdCodec({ contentSize: true })
assert(equal(prepared.decompress(prepared.compress(input)), input), 'Prepared codec mismatch')
const sourcePacked = await lib.compressStream(new Blob([input]).stream(), { format: 'zstd' })
const sourcePlain = await lib.decompressStream(sourcePacked, { format: 'zstd' })
assert(
  equal(new Uint8Array(await new Response(sourcePlain).arrayBuffer()), input),
  'Source-bound stream mismatch',
)
const pool = lib.createWorkerPool({ size: 2 })
try {
  for (const format of lib.formats) {
    for (const mode of ['direct', 'worker', 'pool']) {
      const options = {
        format,
        execution: mode === 'direct' ? 'direct' : 'worker',
        ...(mode === 'pool' ? { workerPool: pool } : {}),
      }
      const frame = await lib.compress(input, {
        ...options,
        ...(format === 'zstd' ? { contentSize: true } : {}),
      })
      assert(
        equal(await lib.decompress(frame, options), input),
        `${format}/${mode}: buffer mismatch`,
      )
    }
  }
  const streamingOptions = { format: 'zstd', execution: 'worker', workerPool: pool }
  await Promise.all(
    Array.from({ length: 6 }, async () => {
      const options = streamingOptions
      const codec = await lib.createCompressionStream({ ...options, contentSize: input.length })
      const frame = await new Response(new Blob([input]).stream().pipeThrough(codec)).arrayBuffer()
      assert(equal(await lib.decompress(frame, options), input), 'Pooled stream mismatch')
    }),
  )
  const controller = new AbortController()
  const held = await lib.createCompressionStream({ ...streamingOptions, signal: controller.signal })
  const reading = held.readable
    .getReader()
    .read()
    .then(
      () => {
        throw new Error('Abort was ignored')
      },
      (error) => assert(error === controller.signal.reason, 'Abort reason changed'),
    )
  controller.abort(new Error('cancel integration operation'))
  await reading
} finally {
  await pool.close()
}
globalThis.integrationComplete = true
console.log('Installed package integration passed')
