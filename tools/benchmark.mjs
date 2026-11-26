import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises'
import { availableParallelism, cpus, platform, release, arch } from 'node:os'
import { resolve } from 'node:path'
import { performance } from 'node:perf_hooks'
import { setImmediate } from 'node:timers/promises'
import { gunzipSync, zstdDecompressSync } from 'node:zlib'
import {
  compress,
  decompress,
  compressStream,
  decompressStream,
  createZstdCodec,
  createWorkerPool,
} from '../dist/index.js'
import { renderReport } from './render-benchmark.mjs'

const outputDirectory = resolve(process.argv[2] ?? 'docs/performance')
const samples = Number(process.env.BENCH_SAMPLES ?? 9)
const sampleMs = Number(process.env.BENCH_SAMPLE_MS ?? 100)
assert(
  Number.isSafeInteger(samples) && samples >= 3 && samples <= 100,
  'BENCH_SAMPLES must be 3–100',
)
assert(
  Number.isFinite(sampleMs) && sampleMs >= 10 && sampleMs <= 10000,
  'BENCH_SAMPLE_MS must be 10–10000',
)
const mib = 1024 ** 2
const chunkBytes = 64 * 1024
const zstd = { format: 'zstd', level: 3, checksum: true, contentSize: false }
const zstdDecode = { format: 'zstd' }
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex')

// Fixed seeds keep both compressible and incompressible inputs reproducible.
function random(seed) {
  return () => {
    seed ^= seed << 13
    seed ^= seed >>> 17
    seed ^= seed << 5
    return seed >>> 0
  }
}

function jsonInput(size) {
  const next = random(0x42c0ffee)
  const rows = []
  let bytes = 2
  while (true) {
    const row = JSON.stringify({
      id: rows.length,
      timestamp: 1700000000000 + rows.length * 1000,
      route: ['/api/jobs', '/api/files', '/api/events', '/health'][next() % 4],
      status: next() % 10 ? 200 : 503,
      durationMs: next() % 1000,
      userId: next() % 10000,
    })
    const addedBytes = row.length + (rows.length ? 1 : 0)
    if (bytes + addedBytes > size) break
    rows.push(row)
    bytes += addedBytes
  }
  // Pad with valid JSON whitespace to make the byte count exact.
  const input = Buffer.alloc(size, 32)
  input.write(`[${rows.join(',')}]`)
  return input
}

const nextByte = random(0x12345678)
const corpora = [
  { id: 'json-4k', label: 'JSON records · 4 KiB', input: jsonInput(4096) },
  { id: 'json-1m', label: 'JSON records · 1 MiB', input: jsonInput(mib) },
  {
    id: 'random-1m',
    label: 'Seeded random · 1 MiB',
    input: Buffer.from(Uint8Array.from({ length: mib }, () => nextByte() & 255)),
  },
]

async function stream(operation, input, options) {
  let offset = 0
  const source = new ReadableStream({
    pull(controller) {
      if (offset === input.byteLength) {
        controller.close()
      } else {
        const end = Math.min(offset + chunkBytes, input.byteLength)
        controller.enqueue(input.subarray(offset, end))
        offset = end
      }
    },
  })
  const output = await operation(source, options)
  return new Uint8Array(await new Response(output).arrayBuffer())
}

// Run synchronous codecs without a Promise/microtask per operation.
let sink = 0
async function batch(run, synchronous, iterations) {
  const start = performance.now()
  if (synchronous) {
    for (let i = 0; i < iterations; i++) sink ^= run().byteLength
  } else {
    for (let i = 0; i < iterations; i++) sink ^= (await run()).byteLength
  }
  return performance.now() - start
}

async function calibrate(run, synchronous) {
  let iterations = 1
  let elapsedMs
  do {
    elapsedMs = await batch(run, synchronous, iterations)
    if (elapsedMs >= sampleMs / 2 || iterations >= 100000) break
    iterations *= 2
  } while (true)
  return Math.max(1, Math.min(100000, Math.ceil((iterations * sampleMs) / elapsedMs)))
}

function quantile(values, q) {
  const sorted = [...values].sort((a, b) => a - b)
  const index = (sorted.length - 1) * q
  const lower = Math.floor(index)
  return sorted[lower] + (sorted[Math.ceil(index)] - sorted[lower]) * (index - lower)
}

function summarize(measurements, inputBytes) {
  const ms = measurements.map(({ iterations, elapsedMs }) => elapsedMs / iterations)
  const rates = ms.map((duration) => inputBytes / mib / (duration / 1000))
  return {
    medianMs: quantile(ms, 0.5),
    medianMiBPerSecond: quantile(rates, 0.5),
    p25MiBPerSecond: quantile(rates, 0.25),
    p75MiBPerSecond: quantile(rates, 0.75),
    samples: measurements,
  }
}

const codec = await createZstdCodec({ level: 3, checksum: true, contentSize: false })
const pool = createWorkerPool({ size: 1 })
const workerCompress = { ...zstd, execution: 'worker', workerPool: pool }
const workerDecompress = { ...zstdDecode, execution: 'worker', workerPool: pool }
const methods = [
  {
    id: 'prepared',
    label: 'Zstd · prepared',
    synchronous: true,
    compress: codec.compress,
    decompress: codec.decompress,
  },
  {
    id: 'buffer',
    label: 'Zstd · buffer',
    compress: (input) => compress(input, zstd),
    decompress: (input) => decompress(input, zstdDecode),
  },
  {
    id: 'stream',
    label: 'Zstd · stream',
    compress: (input) => stream(compressStream, input, zstd),
    decompress: (input) => stream(decompressStream, input, zstdDecode),
  },
  {
    id: 'worker',
    label: 'Zstd · worker pool',
    compress: (input) => compress(input, workerCompress),
    decompress: (input) => decompress(input, workerDecompress),
  },
  {
    id: 'gzip',
    label: 'gzip · native buffer',
    compress: (input) => compress(input, { format: 'gzip' }),
    decompress: (input) => decompress(input, { format: 'gzip' }),
  },
]

const startedAt = new Date().toISOString()
const cases = []
try {
  for (const corpus of corpora) {
    for (const method of methods) {
      const frame = await method.compress(corpus.input)
      assert.deepEqual(Buffer.from(await method.decompress(frame)), corpus.input)
      const nativeDecode = method.id === 'gzip' ? gunzipSync : zstdDecompressSync
      assert.deepEqual(nativeDecode(frame), corpus.input)
      const entry = { corpus, method, frame, operations: {} }
      for (const operation of ['compress', 'decompress']) {
        const input = operation === 'compress' ? corpus.input : frame
        const run = () => method[operation](input)
        const iterations = await calibrate(run, method.synchronous)
        entry.operations[operation] = { run, iterations, measurements: [] }
      }
      cases.push(entry)
      console.log(`Warmed ${corpus.id} / ${method.id}`)
    }
  }
  for (let round = 0; round < samples; round++) {
    // Rotate the starting case and alternate direction order to spread timing drift.
    const order = [...cases.slice(round % cases.length), ...cases.slice(0, round % cases.length)]
    for (const entry of order) {
      for (const operation of round % 2 ? ['decompress', 'compress'] : ['compress', 'decompress']) {
        const { run, iterations, measurements } = entry.operations[operation]
        const elapsedMs = await batch(run, entry.method.synchronous, iterations)
        measurements.push({ iterations, elapsedMs })
        await setImmediate()
      }
    }
    console.log(`Measured round ${round + 1}/${samples}`)
  }
  for (const { corpus, method, frame } of cases) {
    const finalFrame = await method.compress(corpus.input)
    assert.equal(finalFrame.byteLength, frame.byteLength)
    assert.deepEqual(Buffer.from(await method.decompress(finalFrame)), corpus.input)
  }
} finally {
  await pool.close()
}

const sourceFiles = [
  'CMakeLists.txt',
  'Makefile',
  'package.json',
  'package-lock.json',
  'tools/Dockerfile',
  'tools/build-js.mjs',
  'tools/benchmark.mjs',
  'tools/render-benchmark.mjs',
]
for (const directory of ['src', 'c']) {
  for (const name of (await readdir(directory)).sort()) sourceFiles.push(`${directory}/${name}`)
}
const artifactFiles = [
  'dist/index.js',
  'dist/worker.js',
  'dist/embedded.js',
  'dist/runtime-import.js',
  'dist/wasm/zstd.wasm',
  'dist/wasm/zstd.mjs',
]
async function hashes(files) {
  return Object.fromEntries(
    await Promise.all(files.sort().map(async (path) => [path, sha256(await readFile(path))])),
  )
}
const manifest = JSON.parse(await readFile('package.json', 'utf8'))
const zstdVersion = /set\(ZSTD_VERSION ([^)]+)\)/.exec(await readFile('CMakeLists.txt', 'utf8'))[1]
const report = {
  schemaVersion: 1,
  startedAt,
  completedAt: new Date().toISOString(),
  package: { name: manifest.name, version: manifest.version },
  environment: {
    node: process.version,
    v8: process.versions.v8,
    nativeZstd: process.versions.zstd,
    platform: platform(),
    release: release(),
    arch: arch(),
    cpu: cpus()[0]?.model ?? 'unknown',
    logicalCpus: cpus().length,
    availableParallelism: availableParallelism(),
  },
  configuration: {
    samples,
    targetSampleMs: sampleMs,
    warmup: 'adaptive calibration, at least half target sample duration per operation',
    chunkBytes,
    concurrency: 1,
    workerPoolSize: 1,
    zstd: { version: zstdVersion, level: 3, checksum: true, contentSize: false },
    gzip: 'runtime defaults',
    transferInput: false,
  },
  sources: await hashes(sourceFiles),
  artifacts: await hashes(artifactFiles),
  corpora: corpora.map(({ id, label, input }) => ({
    id,
    label,
    bytes: input.byteLength,
    sha256: sha256(input),
  })),
  methods: methods.map(({ id, label, synchronous }) => ({
    id,
    label,
    synchronous: synchronous === true,
  })),
  results: cases.map(({ corpus, method, frame, operations }) => ({
    corpus: corpus.id,
    method: method.id,
    compressedBytes: frame.byteLength,
    compressedPercent: (frame.byteLength / corpus.input.byteLength) * 100,
    compress: summarize(operations.compress.measurements, corpus.input.byteLength),
    decompress: summarize(operations.decompress.measurements, corpus.input.byteLength),
  })),
}
await mkdir(outputDirectory, { recursive: true })
await writeFile(resolve(outputDirectory, 'results.json'), `${JSON.stringify(report, null, 2)}\n`)
await renderReport(report, outputDirectory)
console.log(`Report written to ${outputDirectory} (sink ${sink})`)
