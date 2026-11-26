import { joinChunks } from './options.js'

/** Own codec output; a known bounded length avoids retaining a second full result. */
export class KnownBufferOutput {
  private target?: Uint8Array<ArrayBuffer>
  private chunks: Uint8Array<ArrayBuffer>[] = []
  private size = 0
  private knownSize?: number

  constructor(
    knownSize: number,
    private limit: number,
  ) {
    // The factory bounds this hint and waits for actual output before allocating.
    this.knownSize = knownSize
  }

  append(chunk: Uint8Array<ArrayBuffer>) {
    if (!chunk.byteLength) {
      return
    }
    const size = this.size + chunk.byteLength
    if (!Number.isSafeInteger(size) || size > this.limit) {
      throw new RangeError('Output exceeds maxOutputBytes')
    }
    if (this.knownSize !== undefined) {
      this.target = new Uint8Array(this.knownSize)
      this.knownSize = undefined
    }
    if (this.target && size > this.target.length) {
      // Concatenated frames can produce more than the first frame declares.
      this.chunks.push(this.target.subarray(0, this.size))
      this.target = undefined
    }
    if (this.target) {
      this.target.set(chunk, this.size)
    } else {
      // Borrowed WASM bytes must be copied before another codec step or yield.
      this.chunks.push(new Uint8Array(chunk))
    }
    this.size = size
  }

  finish(): Uint8Array<ArrayBuffer> {
    if (this.target) {
      return this.size === this.target.length ? this.target : this.target.slice(0, this.size)
    }
    return joinChunks(this.chunks, this.size)
  }

  clear() {
    this.target = undefined
    this.chunks = []
  }
}
