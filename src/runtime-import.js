// Keep this tiny module unbundled in the package build so consumer bundlers
// receive the Vite annotation. esbuild otherwise strips comments inside import().
export function importRuntime(specifier) {
  return import(/* @vite-ignore */ specifier)
}
