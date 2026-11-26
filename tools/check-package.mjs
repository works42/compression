import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const temporary = await mkdtemp(join(tmpdir(), 'compression-pack-'))
try {
  const [packed] = Object.values(
    JSON.parse(
      execFileSync('npm', ['pack', '--ignore-scripts', '--json', '--pack-destination', temporary], {
        cwd: root,
        encoding: 'utf8',
      }),
    ),
  )
  const files = packed.files.map((file) => file.path).sort()
  assert.deepEqual(
    files,
    [
      'CHANGELOG.md',
      'LICENSE',
      'LICENSE.zstd',
      'README.md',
      'dist/codec.d.ts',
      'dist/embedded.js',
      'dist/index.d.ts',
      'dist/index.js',
      'dist/options.d.ts',
      'dist/runtime-import.js',
      'dist/wasm/LICENSE.zstd',
      'dist/wasm/zstd.mjs',
      'dist/wasm/zstd.wasm',
      'dist/worker-pool.d.ts',
      'dist/worker.d.ts',
      'dist/worker.js',
      'docs/api.md',
      'examples/browser-file.html',
      'examples/node-stream.mjs',
      'examples/node-worker-pool.mjs',
      'examples/prepared-codec.mjs',
      'package.json',
    ],
    'Unexpected npm package contents',
  )
  for (const file of packed.files) {
    assert.equal(file.mode & 0o111, 0, `Unexpected executable package file: ${file.path}`)
  }
  for (const path of files.filter((path) => /\.(?:m?js|html)$/.test(path))) {
    const source = await readFile(join(root, path), 'utf8')
    assert.doesNotMatch(
      source,
      /\/(?:\/|\*)[#@]\s*sourceMappingURL=/,
      `Source map reference in ${path}`,
    )
  }
  assert.equal(
    await readFile(join(root, 'LICENSE.zstd'), 'utf8'),
    await readFile(join(root, 'dist/wasm/LICENSE.zstd'), 'utf8'),
  )
  // Package output must remain self-contained even when no working tree files exist.
  const manifest = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'))
  assert.equal(manifest.exports['.'].import, './dist/index.js')
  assert.equal(manifest.exports['.'].types, './dist/index.d.ts')
  assert.equal(manifest.exports['./worker'], './dist/worker.js')
  assert.equal(manifest.exports['./wasm/*'], './dist/wasm/*')
  assert.equal(manifest.publishConfig.access, 'public')
  assert.equal(Object.keys(manifest.dependencies ?? {}).length, 0)
  console.log(
    `Package contents verified: ${files.length} files, ${packed.size} tarball bytes, ${packed.unpackedSize} unpacked bytes`,
  )
  console.log(`sha512 integrity: ${packed.integrity}`)
} finally {
  await rm(temporary, { recursive: true, force: true })
}
