import { readFile, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { format, resolveConfig } from 'prettier'

const colors = ['#047857', '#2563eb', '#7c3aed', '#c2410c', '#475569']
const number = (value, digits = 1) => value.toFixed(digits)
const xml = (value) =>
  String(value).replace(
    /[&<>"']/g,
    (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' })[char],
  )

function chart(report) {
  const { environment: env, configuration: config } = report
  const parts = [
    '<svg xmlns="http://www.w3.org/2000/svg" width="1056" height="1008" viewBox="0 0 1056 1008" role="img" aria-labelledby="title description">',
    '<title id="title">Compression performance: throughput and output size</title>',
    `<desc id="description">${xml(config.samples)} warmed samples per operation. Bars show median throughput; whiskers show the interquartile range. See the accompanying report for a text table and methodology.</desc>`,
    '<rect width="1056" height="1008" fill="#ffffff"/>',
    '<g font-family="system-ui, -apple-system, Segoe UI, sans-serif" fill="#0f172a">',
  ]
  const text = (x, y, value, extra = '') =>
    parts.push(`<text x="${x}" y="${y}" ${extra}>${xml(value)}</text>`)
  text(32, 42, 'Compression performance', 'font-size="26" font-weight="700"')
  text(
    32,
    69,
    `${env.node} · ${env.platform}/${env.arch} · ${env.cpu}`,
    'font-size="14" fill="#475569"',
  )
  text(
    32,
    92,
    `Warm · serial · ${config.samples} samples · median + interquartile range · throughput: higher is better`,
    'font-size="14" fill="#475569"',
  )
  text(
    32,
    113,
    'Zstd: level 3, checksum on, content size off · gzip: runtime defaults',
    'font-size="14" fill="#475569"',
  )

  for (const [index, corpus] of report.corpora.entries()) {
    const y = 132 + index * 276
    const rows = report.methods.map((method) =>
      report.results.find((row) => row.corpus === corpus.id && row.method === method.id),
    )
    parts.push(
      `<rect x="24" y="${y}" width="1008" height="260" rx="12" fill="#f8fafc" stroke="#e2e8f0"/>`,
    )
    text(40, y + 30, corpus.label, 'font-size="18" font-weight="650"')
    text(232, y + 56, 'Compress · MiB/s', 'font-size="14" fill="#475569"')
    text(582, y + 56, 'Decompress · MiB/s', 'font-size="14" fill="#475569"')
    text(1008, y + 56, 'Output % ↓', 'font-size="14" fill="#475569" text-anchor="end"')
    for (const [direction, x] of [
      ['compress', 232],
      ['decompress', 582],
    ]) {
      const max = Math.max(...rows.map((row) => row[direction].p75MiBPerSecond)) * 1.06
      const width = 222
      for (const fraction of [0, 0.5, 1]) {
        const tick = x + fraction * width
        parts.push(
          `<line x1="${tick}" y1="${y + 82}" x2="${tick}" y2="${y + 238}" stroke="#e2e8f0"/>`,
        )
        text(
          tick,
          y + 75,
          number(max * fraction, 0),
          'font-size="11" fill="#64748b" text-anchor="middle"',
        )
      }
      for (const [rowIndex, row] of rows.entries()) {
        const center = y + 100 + rowIndex * 30
        const values = row[direction]
        const scale = (value) => x + (value / max) * width
        parts.push(
          `<rect x="${x}" y="${center - 9}" width="${(values.medianMiBPerSecond / max) * width}" height="18" rx="3" fill="${colors[rowIndex]}"/>`,
        )
        const low = scale(values.p25MiBPerSecond)
        const high = scale(values.p75MiBPerSecond)
        parts.push(
          `<path d="M${low},${center} H${high} M${low},${center - 5} V${center + 5} M${high},${center - 5} V${center + 5}" fill="none" stroke="#0f172a" stroke-width="1.3"/>`,
        )
        text(
          x + 292,
          center + 4,
          number(values.medianMiBPerSecond),
          'font-size="14" text-anchor="end" font-variant-numeric="tabular-nums"',
        )
      }
    }
    for (const [rowIndex, method] of report.methods.entries()) {
      const center = y + 100 + rowIndex * 30
      parts.push(`<circle cx="44" cy="${center - 1}" r="4" fill="${colors[rowIndex]}"/>`)
      text(56, center + 4, method.label, 'font-size="14"')
      text(
        1008,
        center + 4,
        `${number(rows[rowIndex].compressedPercent, 3)}%`,
        'font-size="14" text-anchor="end" font-variant-numeric="tabular-nums"',
      )
    }
  }
  text(
    32,
    974,
    'Output % = compressed / original bytes · each throughput column uses its own linear scale',
    'font-size="14" fill="#475569"',
  )
  text(
    32,
    994,
    `${report.startedAt.slice(0, 10)} UTC · synthetic inputs · initialization excluded · see report for scope and raw samples`,
    'font-size="14" fill="#475569"',
  )
  parts.push('</g></svg>')
  return `${parts.join('\n')}\n`
}

function markdown(report) {
  const { environment: env, configuration: config } = report
  const time = new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(report.startedAt))
  const tables = report.corpora.map((corpus) => {
    const rows = report.methods.map((method) => {
      const row = report.results.find(
        (entry) => entry.corpus === corpus.id && entry.method === method.id,
      )
      return `| ${method.label} | ${number(row.compress.medianMiBPerSecond)} | ${number(row.decompress.medianMiBPerSecond)} | ${number(row.compress.medianMs, 3)} | ${number(row.decompress.medianMs, 3)} | ${row.compressedBytes} | ${number(row.compressedPercent, 3)}% |`
    })
    return `### ${corpus.label}\n\n| Method | Compress MiB/s | Decompress MiB/s | Compress ms/op | Decompress ms/op | Compressed Bytes | Output % |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n${rows.join('\n')}`
  })
  return `# Performance Report

Generated by [benchmark.mjs](../../tools/benchmark.mjs) and [render-benchmark.mjs](../../tools/render-benchmark.mjs).
Charts, tables, and [results.json](results.json) come from the same run; do not hand-edit generated numbers.

![Warmed compression and decompression throughput and output size](summary.svg)

## Environment

| Item | Value |
| --- | --- |
| Timestamp | ${time} UTC |
| Package | ${report.package.name} ${report.package.version} |
| Node.js / V8 | ${env.node} / ${env.v8} |
| OS | ${env.platform} ${env.release} / ${env.arch} |
| CPU | ${env.cpu} |
| Logical CPUs / Parallelism | ${env.logicalCpus} / ${env.availableParallelism} |
| Zstd / WASM | ${config.zstd.version} |
| Scheduling | Serial single-task; Worker pool size ${config.workerPoolSize} |
| Zstd settings | level ${config.zstd.level}, checksum: true, contentSize: false; no dictionary |
| gzip settings | Runtime native compression API defaults |

SHA-256 digests of sources and build artifacts are recorded under \`sources\` and \`artifacts\` in \`results.json\`;
input digests are recorded under \`corpora\`.

## Reproduce

With Node.js 24+, npm, Docker, and Make:

\`\`\`sh
npm ci
make toolchain
npm run bench
\`\`\`

Use \`npm run bench:render\` to regenerate charts and Markdown from \`results.json\`. Tune sampling with \`BENCH_SAMPLES\` (3–100, default 9) and \`BENCH_SAMPLE_MS\` (10–10000, default 100):

\`\`\`sh
BENCH_SAMPLES=3 BENCH_SAMPLE_MS=10 npm run bench -- .local/benchmark-smoke
\`\`\`

## Methodology

- **Inputs**: Seeded 4 KiB and 1 MiB JSON record arrays plus 1 MiB pseudorandom bytes.
- **Verification**: Round-trip and Node native Zstd cross-checks run before and after sampling.
- **Sampling**: Warmup loop followed by ${config.samples} rounds (~${config.targetSampleMs} ms per batch), rotating corpus, method, and direction. Bars show median throughput; whiskers show the 25th–75th percentiles.
- **Units**: Throughput uses original uncompressed bytes (1 MiB = 1,048,576 B) over full warmed API calls (excluding input generation and init). \`Output %\` is compressed / original bytes.
- **Methods**: \`prepared\` uses \`createZstdCodec()\`, \`buffer\` uses \`compress()\`/\`decompress()\`, \`stream\` uses 64 KiB chunks, \`worker\` uses a warmed pool of size 1 (no \`transferInput\`), and \`gzip\` uses this library's native adapter.
- **Scope**: Node.js serial execution only; browser performance, cold startup, and memory usage are not measured.

## Results

Higher throughput is better; lower latency and output % are better. ms/op is in milliseconds.

${tables.join('\n\n')}
`
}

export async function renderReport(report, outputDirectory) {
  const config = await resolveConfig('package.json')
  await writeFile(
    resolve(outputDirectory, 'summary.svg'),
    await format(chart(report), { ...config, parser: 'html' }),
  )
  await writeFile(
    resolve(outputDirectory, 'README.md'),
    await format(markdown(report), { ...config, parser: 'markdown' }),
  )
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const outputDirectory = resolve(process.argv[2] ?? 'docs/performance')
  const report = JSON.parse(await readFile(resolve(outputDirectory, 'results.json'), 'utf8'))
  await renderReport(report, outputDirectory)
  console.log(`Rendered report in ${outputDirectory}`)
}
