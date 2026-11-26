import { importRuntime } from './runtime-import.js'
import { createDirectStream, createNativeStream } from './direct.js'
import { hasOnlyKeys, isRecord, parseOptions } from './options.js'

const scope = globalThis as unknown as {
  postMessage?: (value: unknown, transfer: Transferable[]) => void
  onmessage: (event: MessageEvent) => void
}
async function initialize(data: unknown, post: (value: unknown, transfer: Transferable[]) => void) {
  try {
    if (!isRecord(data) || !hasOnlyKeys(data, 'operation', 'options')) {
      throw new TypeError('Invalid compression worker request')
    }
    const { operation } = data
    if (operation !== 'compress' && operation !== 'decompress') {
      throw new TypeError('Invalid compression worker operation')
    }
    const options = parseOptions(operation, data.options)
    if (options.execution !== 'direct') {
      throw new Error('Nested compression workers are not supported')
    }
    // The caller manages transferred native streams; Zstd retains codec-level
    // step limits and scheduling inside this worker.
    const pair =
      options.format === 'zstd'
        ? await createDirectStream(operation, options)
        : createNativeStream(operation, options.format)
    try {
      post(pair, [pair.readable, pair.writable])
    } catch (error) {
      await Promise.allSettled([pair.readable.cancel(error), pair.writable.abort(error)])
      throw error
    }
  } catch (error) {
    const failure =
      error instanceof Error || error instanceof DOMException ? error : new Error(String(error))
    post(
      { error: failure.message, name: failure.name, domException: failure instanceof DOMException },
      [],
    )
  }
}

if (scope.postMessage) {
  scope.onmessage = (event) => {
    void initialize(event.data, (value, transfer) => scope.postMessage!(value, transfer))
  }
} else {
  const specifier = 'node:worker_threads'
  void importRuntime(specifier).then(({ parentPort }) => {
    // Keep the port referenced until the caller terminates this worker.
    parentPort.on('message', (data: unknown) => {
      void initialize(data, (value, transfer) => parentPort.postMessage(value, transfer))
    })
  })
}
