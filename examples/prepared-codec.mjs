import assert from 'node:assert/strict'
import { createZstdCodec } from '../dist/index.js'

const codec = await createZstdCodec({ level: 3, contentSize: true })
const encoder = new TextEncoder()
const decoder = new TextDecoder()

const events = [
  { event: 'job.started', id: 42 },
  { event: 'job.progress', id: 42, percent: 50 },
  { event: 'job.completed', id: 42 },
]

for (const event of events) {
  const input = encoder.encode(JSON.stringify(event))
  const frame = codec.compress(input)
  const restored = JSON.parse(decoder.decode(codec.decompress(frame)))
  assert.deepEqual(restored, event)
  console.log(`${frame.byteLength} bytes → ${JSON.stringify(restored)}`)
}
