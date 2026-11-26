import { createReadStream, createWriteStream } from 'node:fs'
import { link, mkdtemp, rm } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { Readable, Writable } from 'node:stream'
import { createCompressionStream, createWorkerPool } from '../dist/index.js'

const files = process.argv.slice(2)
if (files.length === 0) {
  throw new Error('Usage: node examples/node-worker-pool.mjs INPUT [INPUT ...]')
}

const pool = createWorkerPool({ size: 2, maxQueuedOperations: 0 })

async function compressFile(input) {
  const output = `${input}.zst`
  const staging = await mkdtemp(join(dirname(output), '.compression-'))
  const temporary = join(staging, 'output.zst')
  try {
    const pair = await createCompressionStream({
      format: 'zstd',
      execution: 'worker',
      workerPool: pool,
    })
    await Readable.toWeb(createReadStream(input))
      .pipeThrough(pair)
      .pipeTo(Writable.toWeb(createWriteStream(temporary)))

    // Publish the completed frame only if the destination does not exist.
    await link(temporary, output)
    console.log(`${input} → ${output}`)
  } finally {
    await rm(staging, { recursive: true, force: true })
  }
}

try {
  // Submit at most one file per worker, then reuse the pool for the next batch.
  for (let start = 0; start < files.length; start += pool.size) {
    const batch = files.slice(start, start + pool.size)
    const results = await Promise.allSettled(batch.map(compressFile))
    for (const [index, result] of results.entries()) {
      if (result.status === 'rejected') {
        console.error(`${batch[index]}: ${result.reason.message}`)
        process.exitCode = 1
      }
    }
  }
} finally {
  await pool.close()
}
