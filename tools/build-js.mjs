import { build } from 'esbuild'
import { execFileSync } from 'node:child_process'
import { chmod, copyFile, mkdir, readFile, readdir, rm } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'

// Bundle each entry. The shared embedded codec is a lazy JS module, including
// its WASM bytes, so native formats and worker startup need not load it.
await rm('dist', { recursive: true, force: true })
await mkdir('dist/wasm', { recursive: true })
for (const file of ['zstd.mjs', 'zstd.wasm']) {
  await copyFile(`.local/build/${file}`, `dist/wasm/${file}`)
  await chmod(`dist/wasm/${file}`, 0o644)
}
await copyFile('LICENSE.zstd', 'dist/wasm/LICENSE.zstd')

await build({
  entryPoints: ['src/index.ts', 'src/worker.ts', 'src/embedded.ts'],
  outdir: 'dist',
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2023',
  minify: true,
  plugins: [
    {
      name: 'embedded-zstd',
      setup(context) {
        context.onResolve({ filter: /^\.\/(?:runtime-import|embedded)\.js$/ }, (args) => ({
          path: args.path,
          external: true,
        }))
        context.onResolve({ filter: /^compression:(embedded-loader|wasm-base64)$/ }, (args) => ({
          path: args.path,
          namespace: 'embedded-zstd',
        }))
        context.onLoad({ filter: /.*/, namespace: 'embedded-zstd' }, async (args) => {
          if (args.path === 'compression:wasm-base64') {
            const bytes = await readFile('dist/wasm/zstd.wasm')
            return {
              contents: `export default ${JSON.stringify(bytes.toString('base64'))}`,
              loader: 'js',
            }
          }
          const source = await readFile('dist/wasm/zstd.mjs', 'utf8')
          // Keep the Node builtin import opaque to browser bundlers. Loader code
          // stays ordinary ESM, without blob/data script URLs or eval.
          const nodeImport = 'await import("module")'
          if (!source.includes(nodeImport)) {
            throw new Error('Zstd loader Node import changed; review the embedded build adapter')
          }
          const wasmUrl = 'new URL("zstd.wasm",import.meta.url).href'
          if (!source.includes(wasmUrl)) {
            throw new Error('Zstd loader asset URL changed; review the embedded build adapter')
          }
          return {
            // This entry always receives wasmBinary. Remove its unused URL fallback
            // so consumer bundlers never resolve a separate WASM file.
            contents:
              'import { importRuntime } from "./runtime-import.js";\n' +
              source
                .replace(nodeImport, 'await importRuntime("node:module")')
                .replace(wasmUrl, '""'),
            loader: 'js',
          }
        })
      },
    },
  ],
})

// Preserve Vite's import annotation for runtime-only modules.
await copyFile('src/runtime-import.js', 'dist/runtime-import.js')

execFileSync(
  process.execPath,
  ['node_modules/typescript/bin/tsc', '--emitDeclarationOnly', '-p', 'tsconfig.json'],
  { stdio: 'inherit' },
)
// JS is bundled, so keep only declarations reachable from public entry points.
const declarations = new Set()
async function retainDeclaration(path) {
  if (declarations.has(path)) {
    return
  }
  declarations.add(path)
  const source = await readFile(path, 'utf8')
  for (const match of source.matchAll(/(?:from\s*|import\s*\()\s*['"](\.[^'"]+)\.js['"]/g)) {
    await retainDeclaration(resolve(dirname(path), `${match[1]}.d.ts`))
  }
}

await retainDeclaration(resolve('dist/index.d.ts'))
await retainDeclaration(resolve('dist/worker.d.ts'))
for (const file of await readdir('dist')) {
  if (file.endsWith('.d.ts') && !declarations.has(resolve('dist', file))) {
    await rm(resolve('dist', file))
  }
}
