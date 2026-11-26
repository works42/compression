declare module 'compression:embedded-loader' {
  export default function createModule(options: {
    wasmBinary: Uint8Array<ArrayBuffer>
    locateFile(path: string): string
  }): Promise<import('./zstd-module.js').ZstdModule>
}

declare module 'compression:wasm-base64' {
  const base64: string
  export default base64
}
