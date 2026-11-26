/// <reference path="./embedded-modules.d.ts" />
import createModule from 'compression:embedded-loader'
import wasmBase64 from 'compression:wasm-base64'
import type { ZstdModule } from './zstd-module.js'

export function createEmbeddedModule(): Promise<ZstdModule> {
  // Decode only when Zstd is first requested. No URL or script evaluation is needed.
  const arrayConstructor = Uint8Array as typeof Uint8Array & {
    fromBase64?: (value: string) => Uint8Array<ArrayBuffer>
  }
  const nodeBuffer = (
    globalThis as typeof globalThis & {
      Buffer?: { from(value: string, encoding: 'base64'): Uint8Array<ArrayBuffer> }
    }
  ).Buffer
  let wasmBinary: Uint8Array<ArrayBuffer>
  if (arrayConstructor.fromBase64) {
    wasmBinary = arrayConstructor.fromBase64(wasmBase64)
  } else if (nodeBuffer) {
    wasmBinary = nodeBuffer.from(wasmBase64, 'base64')
  } else {
    const binary = atob(wasmBase64)
    wasmBinary = new Uint8Array(binary.length)
    for (let index = 0; index < binary.length; index++) {
      wasmBinary[index] = binary.charCodeAt(index)
    }
  }
  return createModule({ wasmBinary, locateFile: () => '' })
}
