import { createReadStream, createWriteStream } from 'node:fs'
import { Readable, Writable } from 'node:stream'
import { createCompressionStream, createDecompressionStream } from '../dist/index.js'

const [operation, input, output, execution = 'direct'] = process.argv.slice(2)
if (
  !['compress', 'decompress'].includes(operation) ||
  !input ||
  !output ||
  !['direct', 'worker'].includes(execution)
) {
  throw new Error(
    'Usage: node examples/node-stream.mjs compress|decompress INPUT OUTPUT [direct|worker]',
  )
}
const controller = new AbortController()
const interrupt = () => controller.abort(new DOMException('Interrupted', 'AbortError'))
process.once('SIGINT', interrupt)
try {
  const options = { format: 'zstd', execution, signal: controller.signal }
  const codec =
    operation === 'compress'
      ? await createCompressionStream(options)
      : await createDecompressionStream(options)
  await Readable.toWeb(createReadStream(input, { highWaterMark: 65536 }))
    .pipeThrough(codec)
    .pipeTo(Writable.toWeb(createWriteStream(output, { flags: 'wx' })), {
      signal: controller.signal,
    })
} finally {
  process.removeListener('SIGINT', interrupt)
}
