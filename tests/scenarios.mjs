function assert(condition, message) {
  if (!condition) {
    throw new Error(message)
  }
}

function same(a, b) {
  return a.byteLength === b.byteLength && a.every((byte, index) => byte === b[index])
}

function byteAt(index) {
  let value = index + 1
  value = Math.imul(value ^ (value >>> 16), 0x45d9f3b)
  value = Math.imul(value ^ (value >>> 16), 0x45d9f3b)
  return (value ^ (value >>> 16)) & 255
}

function random(size) {
  const bytes = new Uint8Array(size)
  for (let i = 0; i < size; i += 65536) {
    crypto.getRandomValues(bytes.subarray(i, i + 65536))
  }
  return bytes
}

async function rejects(pending, pattern) {
  try {
    await pending
  } catch (error) {
    if (pattern) {
      assert(pattern.test(String(error)), `Unexpected error: ${error}`)
    }
    return error
  }
  throw new Error('Expected rejection')
}

export async function workerSetupFailures(lib) {
  for (const eventTarget of [true, false]) {
    const listeners = new Map()
    let calls = 0
    let terminated = 0
    const add = (event, listener) => {
      if (++calls === 2) throw new Error('Listener registration failed')
      listeners.set(event, listener)
    }
    const remove = (event) => listeners.delete(event)
    const worker = {
      ...(eventTarget
        ? { addEventListener: add, removeEventListener: remove }
        : { on: add, off: remove }),
      postMessage() {},
      terminate() {
        terminated++
      },
    }
    await rejects(
      lib.createCompressionStream({
        format: 'gzip',
        execution: 'worker',
        workerFactory: () => worker,
      }),
      /Listener registration failed/,
    )
    assert(terminated === 1 && listeners.size === 0, 'Partial listeners were not cleaned up')
  }

  for (const locked of ['readable', 'writable', 'getter']) {
    const pair = { readable: new ReadableStream(), writable: new WritableStream() }
    const lock =
      locked === 'readable'
        ? pair.readable.getReader()
        : locked === 'writable'
          ? pair.writable.getWriter()
          : undefined
    const response =
      locked === 'getter'
        ? {
            get readable() {
              throw new Error('Invalid response getter')
            },
          }
        : pair
    const listeners = new Map()
    let terminated = false
    const worker = {
      addEventListener(event, listener) {
        listeners.set(event, listener)
      },
      removeEventListener(event) {
        listeners.delete(event)
      },
      postMessage() {
        queueMicrotask(() => listeners.get('message')({ data: response }))
      },
      terminate() {
        terminated = true
      },
    }
    try {
      await rejects(
        lib.createCompressionStream({
          format: 'gzip',
          execution: 'worker',
          workerFactory: () => worker,
        }),
        /Invalid (compression worker response|response getter)/,
      )
      assert(terminated && listeners.size === 0, 'Invalid response did not dispose the worker')
    } finally {
      lock?.releaseLock()
    }
  }
}

export async function zstdWindowLimits(lib, execution) {
  const limited = { format: 'zstd', execution, maxWindowLog: 10 }
  const permitted = { ...limited, maxWindowLog: 17 }
  const prepared = execution === 'direct' ? await lib.createZstdCodec({ maxWindowLog: 10 }) : null
  const skip = new Uint8Array([0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0])
  const narrow = await lib.compress(new Uint8Array([7]), { format: 'zstd' })

  for (const size of [0, 4096, 65536, 65537]) {
    const plain = new Uint8Array(size).fill(73)
    const original = await lib.compress(plain, { format: 'zstd', contentSize: true })
    const descriptor = original[4]
    const sizeBytes = descriptor >>> 6 ? 2 ** (descriptor >>> 6) : Number(Boolean(descriptor & 32))
    const body = original.subarray(5 + Number(!(descriptor & 32)) + sizeBytes)
    for (const contentSize of [false, true]) {
      // Declare a 128 KiB window independently of the encoder's chosen window.
      const header = new Uint8Array(contentSize ? 10 : 6)
      header.set(original.subarray(0, 4))
      header[4] = (descriptor & 4) | (contentSize ? 128 : 0)
      header[5] = (17 - 10) << 3
      if (contentSize) new DataView(header.buffer).setUint32(6, size, true)
      const wide = new Uint8Array(header.length + body.length)
      wide.set(header)
      wide.set(body, header.length)
      assert(same(await lib.decompress(wide, permitted), plain), 'Valid window rejected')

      for (const prefix of [new Uint8Array(), new Uint8Array([...skip, ...narrow, ...skip])]) {
        const frame = new Uint8Array(prefix.length + wide.length)
        frame.set(prefix)
        frame.set(wide, prefix.length)
        await rejects(lib.decompress(frame, limited), /too much memory/)
        if (prepared) {
          await rejects(
            Promise.resolve().then(() => prepared.decompress(frame)),
            /too much memory/,
          )
        }
        const splits =
          execution === 'direct'
            ? Array.from({ length: header.length + 1 }, (_, index) => prefix.length + index)
            : [0, prefix.length + 4, prefix.length + header.length]
        for (const split of splits) {
          const source = () =>
            new ReadableStream({
              start(controller) {
                controller.enqueue(frame.subarray(0, split))
                controller.enqueue(frame.subarray(split))
                controller.close()
              },
            })
          const pair = await lib.createDecompressionStream(limited)
          await rejects(source().pipeThrough(pair).pipeTo(new WritableStream()), /too much memory/)
          await rejects(
            (await lib.decompressStream(source(), limited)).pipeTo(new WritableStream()),
            /too much memory/,
          )
        }
      }
    }
  }
}

export async function preparedZstd(lib) {
  const throws = (run, type = Error) => {
    try {
      run()
    } catch (error) {
      assert(error instanceof type, `Unexpected error: ${error}`)
      return
    }
    throw new Error('Expected synchronous failure')
  }
  const codec = await lib.createZstdCodec()
  for (const size of [0, 1, 65536, 65537, 2 * 1024 * 1024 + 13]) {
    const input = random(size)
    const saved = input.slice()
    const frame = codec.compress(input)
    assert(frame instanceof Uint8Array, 'Compression did not finish synchronously')
    const restored = codec.decompress(frame)
    assert(
      restored instanceof Uint8Array && same(restored, saved),
      'Synchronous round trip mismatch',
    )
    input.fill(0)
    codec.compress(new Uint8Array([99]))
    assert(same(restored, saved), 'Output aliases reusable storage')
    const reference = await lib.compress(saved, { format: 'zstd', contentSize: true })
    assert(same(codec.decompress(reference), saved), 'Known-size frame mismatch')
  }
  const storage = random(300)
  const expected = storage.slice(17, 123)
  assert(
    same(codec.decompress(codec.compress(new DataView(storage.buffer, 17, 106))), expected),
    'Input view range ignored',
  )
  const detached = new Uint8Array()
  const dataView = new DataView(detached.buffer)
  structuredClone(detached.buffer, { transfer: [detached.buffer] })
  for (const input of [detached, detached.buffer, dataView, 'text', null]) {
    throws(() => codec.compress(input), TypeError)
  }
  const input = new Uint8Array(200000).fill(73)
  const frame = codec.compress(input)
  const joined = new Uint8Array(frame.length * 2 + 8)
  joined.set(frame)
  joined.set([0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0], frame.length)
  joined.set(frame, frame.length + 8)
  assert(
    codec.decompress(joined).length === input.length * 2,
    'Concatenated/skippable frame mismatch',
  )
  for (const damaged of [frame.subarray(0, -1), new Uint8Array(), new Uint8Array([...frame, 1])]) {
    throws(() => codec.decompress(damaged))
  }
  const corrupted = codec.compress(new Uint8Array(100).fill(71))
  corrupted[corrupted.length - 1] ^= 1
  throws(() => codec.decompress(corrupted))
  assert(same(codec.decompress(frame), input), 'Failure poisoned the next call')
  const bounded = await lib.createZstdCodec({ maxInputBytes: 3, maxOutputBytes: 3 })
  throws(() => bounded.compress(new Uint8Array(4)), RangeError)
  throws(() => bounded.compress(new Uint8Array(3)), RangeError)
  for (const size of [0, 100, 200000]) {
    const bytes = new Uint8Array(size).fill(73)
    const packed = codec.compress(bytes)
    const exact = await lib.createZstdCodec({ maxOutputBytes: size })
    assert(same(exact.decompress(packed), bytes), 'Exact output limit failed')
    if (size) {
      const short = await lib.createZstdCodec({ maxOutputBytes: size - 1 })
      throws(() => short.decompress(packed), RangeError)
    }
  }
  const windowLimited = await lib.createZstdCodec({ maxWindowLog: 10 })
  throws(() => windowLimited.decompress(frame))
  const dictionary = new TextEncoder().encode('shared record timestamp route '.repeat(80))
  const savedDictionary = dictionary.slice()
  const plain = dictionary.slice(17, 1117)
  const settings = { dictionary, level: 3, contentSize: true }
  const pending = lib.createZstdCodec(settings)
  dictionary.fill(0)
  settings.level = 22
  settings.contentSize = false
  const prepared = await pending
  const packed = prepared.compress(plain)
  assert(
    same(await lib.decompress(packed, { format: 'zstd', dictionary: savedDictionary }), plain),
    'Prepared dictionary was not snapshotted',
  )
  assert(same(prepared.decompress(packed), plain), 'Prepared decoder dictionary mismatch')
  assert(
    same(codec.decompress(codec.compress(plain)), plain),
    'Prepared dictionary leaked into another codec',
  )
  for (const options of [
    null,
    [],
    { execution: 'direct' },
    { format: 'zstd' },
    { transferInput: false },
    { highWaterMarkBytes: undefined },
    { level: 1.5 },
    { level: 999 },
    { contentSize: 'true' },
    { contentSize: -1 },
    { checksum: 1 },
    { maxWindowLog: 9 },
    { maxOutputBytes: -1 },
  ]) {
    await rejects(lib.createZstdCodec(options))
  }
  const controller = new AbortController()
  const reason = new Error('prepared codec aborted')
  const abortable = await lib.createZstdCodec({ signal: controller.signal })
  controller.abort(reason)
  try {
    abortable.compress(plain)
    throw new Error('Abort ignored')
  } catch (error) {
    assert(error === reason, 'Abort reason changed')
  }
  assert(
    (await rejects(lib.createZstdCodec({ signal: controller.signal }))) === reason,
    'Initialization abort ignored',
  )
}

export async function roundTrip(lib, format, execution) {
  const options = { format, execution }
  for (const input of [
    new Uint8Array(),
    new Uint8Array([7]),
    new TextEncoder().encode('compress hello '.repeat(1000)),
    random(2 * 1024 * 1024 + 19),
  ]) {
    const original = input.slice()
    const packed = await lib.compress(input, options)
    assert(same(input, original), 'Compression changed input')
    const saved = packed.slice()
    assert(same(await lib.decompress(packed, options), original), 'Round trip mismatch')
    assert(same(packed, saved), 'Decompression changed input')
  }
  const storage = random(10000)
  const original = storage.slice()
  const input = new DataView(storage.buffer, 13, 9001)
  const packed = await lib.compress(input, options)
  assert(
    same(await lib.decompress(packed, options), original.subarray(13, 9014)),
    'View range was ignored',
  )
  assert(same(storage, original), 'Sibling view was modified')
  const consumed = random(32)
  const task = lib.compress(consumed.subarray(7, 17), { ...options, transferInput: true })
  assert(consumed.buffer.detached, 'Explicit transfer did not detach synchronously')
  assert(
    (await lib.decompress(await task, options)).length === 10,
    'Transferred view range was ignored',
  )
}

export async function streaming(lib, format, execution) {
  const options = { format, execution }
  const total = 3 * 1024 * 1024 + 1333
  let produced = 0
  let consumed = 0
  let earlyOutput = false
  const source = new ReadableStream({
    pull(controller) {
      if (produced === total) {
        controller.close()
        return
      }
      const size = Math.min(32771, total - produced)
      const chunk = Uint8Array.from({ length: size }, (_, i) => byteAt(i + produced))
      produced += size
      controller.enqueue(chunk)
    },
  })
  await source
    .pipeThrough(await lib.createCompressionStream(options))
    .pipeThrough(await lib.createDecompressionStream(options))
    .pipeTo(
      new WritableStream({
        write(chunk) {
          assert(chunk instanceof Uint8Array, 'Output is not Uint8Array')
          assert(
            chunk.every((byte, i) => byte === byteAt(i + consumed)),
            'Stream content mismatch',
          )
          if (produced < total) {
            earlyOutput = true
          }
          consumed += chunk.byteLength
        },
      }),
    )
  assert(consumed === total, 'Stream truncated')
  assert(earlyOutput, 'Stream buffered the entire input')
}

export async function limitsAndErrors(lib, format, execution) {
  const options = { format, execution }
  const input = new Uint8Array(1024 * 1024 + 17).fill(42)
  const packed = await lib.compress(input, options)
  const outputLimit = await rejects(
    lib.decompress(packed, { ...options, maxOutputBytes: input.length - 1 }),
    /maxOutputBytes/,
  )
  assert(outputLimit instanceof RangeError, 'Output limit must reject with RangeError')
  assert(
    (await lib.decompress(packed, { ...options, maxOutputBytes: input.length })).length ===
      input.length,
    'Exact output limit failed',
  )
  const inputLimit = await rejects(
    lib.compress(input, { ...options, maxInputBytes: 1, transferInput: true }),
    /maxInputBytes/,
  )
  assert(inputLimit instanceof RangeError, 'Input limit must reject with RangeError')
  assert(!input.buffer.detached, 'Invalid request detached input')
  await rejects(lib.decompress(packed.subarray(0, packed.length - 1), options))
  await rejects(lib.decompress(new Uint8Array([255, 255, 255]), options))
  assert(
    (await lib.decompress(await lib.compress(new Uint8Array([1]), options), options))[0] === 1,
    'Error poisoned subsequent operation',
  )
  if (format === 'zstd') {
    const invalidLevel = { ...options, level: Number.MAX_SAFE_INTEGER }
    assert(
      (await rejects(lib.createCompressionStream(invalidLevel), /level/)) instanceof RangeError,
      'Invalid Zstd level must reject stream initialization with RangeError',
    )
    assert(
      (await rejects(lib.compress(input, invalidLevel), /level/)) instanceof RangeError,
      'Invalid Zstd level must reject buffer compression with RangeError',
    )
    let outputBytes = 0
    const decoder = await lib.createDecompressionStream(options)
    const truncated = new Blob([packed.subarray(0, -1)]).stream().pipeThrough(decoder)
    await rejects(
      truncated.pipeTo(
        new WritableStream({
          write(chunk) {
            outputBytes += chunk.byteLength
          },
        }),
      ),
      /truncated/i,
    )
    assert(outputBytes > 0, 'Streaming decompression can emit bytes before reporting truncation')
  }
  const pair = await lib.createCompressionStream({ ...options, maxInputBytes: 100 })
  const reading = pair.readable.pipeTo(new WritableStream())
  const readError = rejects(reading, /maxInputBytes/)
  const writer = pair.writable.getWriter()
  await writer.write(new Uint8Array(50))
  await writer.write(new Uint8Array(50))
  const writeError = await rejects(writer.write(new Uint8Array(1)), /maxInputBytes/)
  assert(
    writeError instanceof RangeError && (await readError) === writeError,
    'Input limit should error both sides with the same RangeError',
  )
  writer.releaseLock()
}

export async function cancellation(lib, format, execution) {
  const controller = new AbortController()
  const options = { format, execution, signal: controller.signal }
  const stream = await lib.createDecompressionStream(options)
  const writer = stream.writable.getWriter()
  const reader = stream.readable.getReader()
  const reading = rejects(reader.read())
  controller.abort(null)
  assert((await reading) === controller.signal.reason, 'Readable must preserve the abort reason')
  assert(
    (await rejects(writer.write(new Uint8Array([1])))) === controller.signal.reason,
    'Writable must preserve the abort reason',
  )
  reader.releaseLock()
  writer.releaseLock()
  const aborted = new AbortController()
  aborted.abort(new Error('pre-aborted'))
  const bytes = new Uint8Array(1)
  await rejects(
    lib.compress(bytes, { format, execution, signal: aborted.signal, transferInput: true }),
    /pre-aborted/,
  )
  assert(!bytes.buffer.detached, 'Pre-aborted operation detached input')
  const other = await lib.createCompressionStream({ format, execution })
  const otherWriter = other.writable.getWriter()
  const writes = Array.from({ length: 5 }, () => otherWriter.write(random(65536)))
  // Observe all failures before cancellation, including queued writes.
  const settled = Promise.allSettled(writes)
  assert(otherWriter.desiredSize <= 0, 'Writable byte backpressure was not applied')
  const ready = rejects(otherWriter.ready)
  await other.readable.cancel(new Error('reader cancelled'))
  await ready
  assert(
    (await settled).some((result) => result.status === 'rejected'),
    'Cancellation left writes alive',
  )
  otherWriter.releaseLock()
  const invalid = await lib.createCompressionStream({ format, execution })
  const invalidWriter = invalid.writable.getWriter()
  const invalidReader = invalid.readable.getReader()
  const readError = rejects(invalidReader.read())
  await rejects(invalidWriter.write('invalid input'))
  await readError
  invalidReader.releaseLock()
  invalidWriter.releaseLock()
  const abortedPair = await lib.createCompressionStream({ format, execution })
  const abortedReader = abortedPair.readable.getReader()
  const abortedWriter = abortedPair.writable.getWriter()
  const pendingWrites = Promise.allSettled(
    Array.from({ length: 4 }, () => abortedWriter.write(random(512 * 1024))),
  )
  assert(!(await abortedReader.read()).done, 'Expected stream output before abort')
  assert(abortedWriter.desiredSize <= 0, 'Expected writes under backpressure')
  const abortRead = rejects(abortedReader.closed, /writer aborted/)
  await abortedWriter.abort(new Error('writer aborted'))
  await abortRead
  assert(
    (await pendingWrites).some((result) => result.status === 'rejected'),
    'Abort left backpressured writes active',
  )
  abortedReader.releaseLock()
  abortedWriter.releaseLock()
}

export async function zstdScratchBuffers(lib, execution) {
  const options = { format: 'zstd', execution }
  // Exercise partially consumed scratch buffers, exact boundaries, empty writes,
  // output drains, and two live contexts sharing one WASM heap.
  const first = Uint8Array.from({ length: 2 * 1024 * 1024 + 19 }, (_, i) => byteAt(i))
  const second = new Uint8Array(first.length).fill(37)
  const packed = await Promise.all([first, second].map((bytes) => lib.compress(bytes, options)))
  async function decode(frame, expected, chunkSize) {
    const codec = await lib.createDecompressionStream(options)
    let offset = 0
    let consumed = 0
    let empty = true
    const source = new ReadableStream({
      pull(controller) {
        if (empty) {
          empty = false
          controller.enqueue(new Uint8Array())
          return
        }
        if (offset === frame.length) {
          return controller.close()
        }
        const end = Math.min(offset + chunkSize, frame.length)
        controller.enqueue(frame.slice(offset, end))
        offset = end
      },
    })
    await source.pipeThrough(codec).pipeTo(
      new WritableStream({
        write(chunk) {
          assert(
            chunk.every((byte, i) => byte === expected[consumed + i]),
            'Scratch buffer data was overwritten',
          )
          consumed += chunk.length
        },
      }),
    )
    assert(consumed === expected.length, 'Scratch buffer output was truncated')
  }
  const tiny = first.subarray(0, 513)
  await decode(await lib.compress(tiny, options), tiny, 1)
  for (const chunkSize of [8191, 65535, 65536, 65537, 131071, 131072, 131073, 262144]) {
    await Promise.all(packed.map((frame, i) => decode(frame, i ? second : first, chunkSize)))
  }
  for (const bytes of [2 ** 31, 2 ** 32 - 1]) {
    const header = new Uint8Array(8)
    const view = new DataView(header.buffer)
    view.setUint32(0, 0x184d2a50, true)
    view.setUint32(4, bytes, true)
    const codec = await lib.createDecompressionStream(options)
    const reading = rejects(codec.readable.pipeTo(new WritableStream()), /truncated/i)
    void reading.catch(() => {})
    const writer = codec.writable.getWriter()
    try {
      // A valid partial skip header must be accepted; only closing it is truncated.
      await writer.write(header)
      const closing = writer.close().catch(() => {})
      await reading
      await closing
    } finally {
      writer.releaseLock()
    }
  }
}

export async function largeBufferOutput(lib, format, execution) {
  const input = Uint8Array.from({ length: 2 * 1024 * 1024 + 17 }, (_, i) => byteAt(i))
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', input)).join(',')
  const options = { format, execution }
  const frame = await lib.compress(input, {
    ...options,
    ...(format === 'zstd' ? { contentSize: true } : {}),
  })
  const output = await lib.decompress(frame, { ...options, maxOutputBytes: input.length })
  assert(output.length === input.length, 'Large output length changed')
  assert(output.buffer.resizable !== true, 'Result must work with fixed-buffer Web APIs')
  assert(new Blob([output]).size === input.length, 'Blob rejected large output')
  new TextDecoder().decode(output.subarray(0, 16))
  assert(
    new Uint8Array(await crypto.subtle.digest('SHA-256', output)).join(',') === digest,
    'Large collected output changed bytes',
  )
  await rejects(lib.decompress(frame, { ...options, maxOutputBytes: input.length - 1 }))
}

export async function validation(lib) {
  const input = new Uint8Array([1])
  for (const options of [
    { format: 'zip' },
    { format: 'gzip', level: 3 },
    { format: 'gzip', level: undefined },
    { format: 'gzip', dictionary: input },
    { format: 'zstd', maxOutputBytes: -1 },
    { format: 'zstd', highWaterMarkBytes: 0 },
    { format: 'zstd', initializationTimeoutMs: 0 },
    { format: 'zstd', unexpected: true },
    { format: 'zstd', checksum: 'true' },
    { format: 'zstd', loaderUrl: '' },
    { format: 'zstd', maxWindowLog: undefined },
    { format: 'gzip', workerFactory: () => {} },
    { format: 'gzip', transferInput: 'true' },
  ]) {
    const error = await rejects(lib.compress(input, { transferInput: true, ...options }))
    assert(error instanceof TypeError, 'Invalid options must reject with TypeError')
  }
  for (const options of [
    { format: 'zstd', level: 3 },
    { format: 'zstd', maxWindowLog: 31 },
  ]) {
    const error = await rejects(lib.createDecompressionStream(options))
    assert(error instanceof TypeError, 'Invalid decoder options must reject with TypeError')
  }
  assert(!input.buffer.detached, 'Validation detached input')
  let terminated = false
  const registrationError = new Error('custom worker listener failed')
  const workerError = await rejects(
    lib.createCompressionStream({
      format: 'zstd',
      execution: 'worker',
      workerFactory: () => ({
        postMessage() {},
        terminate() {
          terminated = true
        },
        addEventListener() {
          throw registrationError
        },
      }),
    }),
  )
  assert(
    workerError === registrationError && terminated,
    'Worker registration failure leaked its lifetime',
  )
  await rejects(lib.compress('bad', { format: 'gzip' }))
  const detached = new ArrayBuffer(0)
  const detachedBytes = new Uint8Array(detached)
  const detachedView = new DataView(detached)
  structuredClone(detached, { transfer: [detached] })
  await rejects(lib.compress(detached, { format: 'gzip' }))
  await rejects(lib.compress(detachedBytes, { format: 'gzip' }))
  await rejects(lib.compress(detachedView, { format: 'gzip' }))
  const snapshot = new Uint8Array([1, 2, 3])
  const pending = lib.compress(snapshot, { format: 'gzip' })
  snapshot.fill(99)
  assert(
    same(await lib.decompress(await pending, { format: 'gzip' }), new Uint8Array([1, 2, 3])),
    'One-shot input was not snapshotted synchronously',
  )
  await lib.compress(new Uint8Array([1]), { format: 'zstd' })
  const warmInput = new Uint8Array([1, 2, 3])
  const warmPending = lib.compress(warmInput, { format: 'zstd' })
  assert(warmPending instanceof Promise, 'Warm buffer call changed its return type')
  warmInput.fill(99)
  const warmFrame = await warmPending
  assert(
    same(await lib.decompress(warmFrame, { format: 'zstd' }), new Uint8Array([1, 2, 3])),
    'Warm input ownership changed',
  )
  const warmAbort = new AbortController()
  const reason = new Error('abort warm delivery')
  const warmResult = lib.decompress(warmFrame, { format: 'zstd', signal: warmAbort.signal })
  warmAbort.abort(reason)
  assert((await rejects(warmResult)) === reason, 'Warm delivery ignored cancellation')
}

export async function sourceStreams(lib, format, execution) {
  const options = { format, execution }
  const input = random(2 * 1024 * 1024 + 19)
  const packed = await lib.compressStream(new Blob([input]).stream(), options)
  const restored = await lib.decompressStream(packed, options)
  assert(
    same(new Uint8Array(await new Response(restored).arrayBuffer()), input),
    'Source-bound round trip mismatch',
  )
  const frame = await lib.compress(input, options)
  await rejects(
    (
      await lib.decompressStream(new Blob([frame]).stream(), { ...options, maxOutputBytes: 100 })
    ).pipeTo(new WritableStream()),
    /maxOutputBytes/,
  )
  await rejects(
    (
      await lib.compressStream(new Blob([input]).stream(), { ...options, maxInputBytes: 100 })
    ).pipeTo(new WritableStream()),
    /maxInputBytes/,
  )
  let cancelled = false
  const endless = new ReadableStream(
    {
      pull(c) {
        c.enqueue(input)
      },
      cancel() {
        cancelled = true
      },
    },
    { highWaterMark: 1 },
  )
  const output = await lib.compressStream(endless, options)
  const reader = output.getReader()
  assert(!(await reader.read()).done, 'Expected source-bound output')
  await reader.cancel(new Error('stop source-bound stream'))
  for (let attempt = 0; !cancelled && attempt < 100; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  assert(cancelled, 'Output cancellation did not cancel source')
  reader.releaseLock()
  const controller = new AbortController()
  const reason = new Error('abort source-bound input')
  let blockedCancelled = false
  const blocked = new ReadableStream({
    cancel() {
      blockedCancelled = true
    },
  })
  const abortable = await lib.compressStream(blocked, { ...options, signal: controller.signal })
  const reading = rejects(abortable.pipeTo(new WritableStream()))
  controller.abort(reason)
  assert((await reading) === reason, 'Source-bound abort reason changed')
  for (let attempt = 0; !blockedCancelled && attempt < 100; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  assert(blockedCancelled, 'Aborted input remained locked')
  const bad = new ReadableStream({
    start(c) {
      c.error(reason)
    },
  })
  assert(
    (await rejects((await lib.compressStream(bad, options)).pipeTo(new WritableStream()))) ===
      reason,
    'Source error changed',
  )
  const locked = new ReadableStream()
  const lock = locked.getReader()
  await rejects(lib.compressStream(locked, options), /unlocked/)
  lock.releaseLock()
  await rejects(lib.compressStream(new Uint8Array(), options), /unlocked/)
  if (format === 'zstd') {
    const damaged = frame.slice()
    damaged[damaged.length - 1] ^= 1
    await rejects(
      (await lib.decompressStream(new Blob([damaged]).stream(), options)).pipeTo(
        new WritableStream(),
      ),
    )
    await rejects(
      (await lib.decompressStream(new Blob([frame.subarray(0, -1)]).stream(), options)).pipeTo(
        new WritableStream(),
      ),
    )
  }
}
