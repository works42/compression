#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zstd_errors.h>

/* Scratch buffers bound each WASM call, not the size of a stream or frame. */
#define CHUNK_BYTES (64U * 1024U)
#define INPUT_BYTES (128U * 1024U)
#define OUTPUT_BYTES (64U * 1024U)
#define BUFFER_BYTES (128U * 1024U)
#define CACHE_BYTES (4U * 1024U * 1024U)
typedef struct {
  ZSTD_CCtx *compressor;
  ZSTD_DCtx *decompressor;
  unsigned char input[INPUT_BYTES];
  unsigned char output[OUTPUT_BYTES];
  size_t consumed, produced;
  unsigned char header[ZSTD_FRAMEHEADERSIZE_MAX];
  size_t header_bytes, max_window;
  int header_checked;
} Codec;
static const char *last_error;
static int check(size_t status) {
  /* Streaming callers only need zero versus a positive continuation hint.
   * A skippable frame can request up to UINT32_MAX bytes on WASM32; narrowing
   * that hint directly to int would turn valid input into a negative error. */
  if (!ZSTD_isError(status)) {
    return status > INT_MAX ? INT_MAX : (int)status;
  }
  last_error = ZSTD_getErrorName(status);
  return -1;
}

const char *codec_error(void) {
  return last_error ? last_error : "Zstd allocation failed";
}

int codec_min_level(void) {
  return ZSTD_minCLevel();
}

int codec_max_level(void) {
  return ZSTD_maxCLevel();
}

static int valid_window_log(int window_log) {
  if (window_log < ZSTD_WINDOWLOG_MIN || window_log > ZSTD_WINDOWLOG_MAX_32) {
    last_error = "Zstd window log must be between 10 and 30";
    return 0;
  }
  return 1;
}

/* Buffer calls are synchronous within a WASM instance. These contexts never
 * overlap a call and are separate from the per-stream contexts below. */
static ZSTD_CCtx *buffer_compressor;
static ZSTD_DCtx *buffer_decompressor;
/* Synchronous calls cannot overlap inside an instance. Streaming contexts have
 * independent scratch. Keep one bounded staging area instead of malloc/free
 * pairs on every small message. Outputs are copied into owned JS arrays. */
static unsigned char buffer_input[BUFFER_BYTES];
static unsigned char buffer_output[ZSTD_COMPRESSBOUND(BUFFER_BYTES)];
size_t codec_buffer_capacity(void) {
  return BUFFER_BYTES;
}

uintptr_t codec_buffer_input(void) {
  return (uintptr_t)buffer_input;
}

uintptr_t codec_buffer_output(void) {
  return (uintptr_t)buffer_output;
}

size_t codec_buffer_bound(size_t size) {
  return ZSTD_compressBound(size);
}

int codec_buffer_compress(void *output, size_t capacity, const void *input, size_t size, int level,
                          int checksum, int include_size, const void *dictionary,
                          size_t dictionary_size) {
  last_error = NULL;
  if (!buffer_compressor) {
    buffer_compressor = ZSTD_createCCtx();
  }
  if (!buffer_compressor) {
    return -1;
  }
  int result = -1;
  if (check(ZSTD_CCtx_setParameter(buffer_compressor, ZSTD_c_compressionLevel, level)) < 0 ||
      check(ZSTD_CCtx_setParameter(buffer_compressor, ZSTD_c_checksumFlag, checksum)) < 0 ||
      check(ZSTD_CCtx_setParameter(buffer_compressor, ZSTD_c_contentSizeFlag, include_size)) < 0 ||
      (dictionary_size &&
       check(ZSTD_CCtx_loadDictionary(buffer_compressor, dictionary, dictionary_size)) < 0)) {
    goto cleanup;
  }
  /* At the default level, use streaming search parameters once a full minimum
   * window is available, while bounding the window to the known input. */
  if (!dictionary_size && size >= 1024 && (level == 0 || level == ZSTD_CLEVEL_DEFAULT)) {
    ZSTD_compressionParameters parameters = ZSTD_getCParams(level, 0, 0);
    unsigned window_log = 10;
    while (((size_t)1 << window_log) < size && window_log < parameters.windowLog) {
      window_log++;
    }
    parameters.windowLog = window_log;
    if (check(ZSTD_CCtx_setCParams(buffer_compressor, parameters)) < 0) {
      goto cleanup;
    }
  }
  /* compress2 knows size even when the frame header omits it. */
  result = check(ZSTD_compress2(buffer_compressor, output, capacity, input, size));
cleanup:
  /* Drop dictionary ownership on every exit and bound the idle workspace. */
  if (ZSTD_isError(ZSTD_CCtx_reset(buffer_compressor, ZSTD_reset_session_and_parameters)) ||
      ZSTD_sizeof_CCtx(buffer_compressor) > CACHE_BYTES) {
    ZSTD_freeCCtx(buffer_compressor);
    buffer_compressor = NULL;
  }
  return result;
}
/* >=0 is a known small output size; -1 is an unknown-size single frame;
 * -2 requires incremental processing (including malformed/concatenated input). */
int codec_buffer_size(const void *input, size_t size, int window_log) {
  ZSTD_frameHeader header;
  if (!valid_window_log(window_log) || ZSTD_getFrameHeader(&header, input, size) != 0 ||
      header.frameType != ZSTD_frame || header.windowSize > ((size_t)1 << window_log) ||
      ZSTD_findFrameCompressedSize(input, size) != size) {
    return -2;
  }
  if (header.frameContentSize == ZSTD_CONTENTSIZE_UNKNOWN) {
    return -1;
  }
  return header.frameContentSize > CHUNK_BYTES ? -2 : (int)header.frameContentSize;
}

/* Read only a frame-header prefix. A collector may use this as an allocation
 * hint; full frame, checksum and window validation still happens while decoding. */
double codec_frame_content_size(const void *input, size_t size) {
  unsigned long long bytes = ZSTD_getFrameContentSize(input, size);
  return bytes == ZSTD_CONTENTSIZE_UNKNOWN || bytes == ZSTD_CONTENTSIZE_ERROR ? -1 : (double)bytes;
}

int codec_buffer_decompress(void *output, size_t capacity, const void *input, size_t size,
                            const void *dictionary, size_t dictionary_size) {
  last_error = NULL;
  if (!buffer_decompressor) {
    buffer_decompressor = ZSTD_createDCtx();
  }
  if (!buffer_decompressor) {
    return -1;
  }
  int result = -1;
  size_t status;
  if (dictionary_size &&
      check(ZSTD_DCtx_loadDictionary(buffer_decompressor, dictionary, dictionary_size)) < 0) {
    goto cleanup;
  }
  status = ZSTD_decompressDCtx(buffer_decompressor, output, capacity, input, size);
  if (ZSTD_isError(status) && ZSTD_getErrorCode(status) == ZSTD_error_dstSize_tooSmall) {
    result = -2;
  } else {
    result = check(status);
  }
cleanup:
  if (ZSTD_isError(ZSTD_DCtx_reset(buffer_decompressor, ZSTD_reset_session_and_parameters)) ||
      ZSTD_sizeof_DCtx(buffer_decompressor) > CACHE_BYTES) {
    ZSTD_freeDCtx(buffer_decompressor);
    buffer_decompressor = NULL;
  }
  return result;
}

void codec_free(Codec *codec) {
  if (!codec) {
    return;
  }
  ZSTD_freeCCtx(codec->compressor);
  ZSTD_freeDCtx(codec->decompressor);
  free(codec);
}

static int codec_configure(Codec *codec, int level, int checksum, int window_log,
                           const void *dictionary, size_t dictionary_size) {
  if (codec->compressor) {
    if (check(ZSTD_CCtx_setParameter(codec->compressor, ZSTD_c_compressionLevel, level)) < 0 ||
        check(ZSTD_CCtx_setParameter(codec->compressor, ZSTD_c_checksumFlag, checksum)) < 0 ||
        check(ZSTD_CCtx_loadDictionary(codec->compressor, dictionary, dictionary_size)) < 0) {
      return -1;
    }
  } else {
    if (!valid_window_log(window_log)) {
      return -1;
    }
    codec->max_window = (size_t)1 << window_log;
    codec->header_bytes = 0;
    codec->header_checked = 0;
    if (check(ZSTD_DCtx_setParameter(codec->decompressor, ZSTD_d_windowLogMax, window_log)) < 0 ||
        check(ZSTD_DCtx_loadDictionary(codec->decompressor, dictionary, dictionary_size)) < 0) {
      return -1;
    }
  }
  return 0;
}

Codec *codec_create(int compress, int level, int checksum, int window_log, const void *dictionary,
                    size_t dictionary_size) {
  last_error = NULL;
  Codec *codec = malloc(sizeof(Codec));
  if (!codec) {
    return NULL;
  }
  codec->compressor = NULL;
  codec->decompressor = NULL;
  codec->consumed = codec->produced = 0;
  if (compress) {
    codec->compressor = ZSTD_createCCtx();
    if (!codec->compressor) {
      goto fail;
    }
  } else {
    codec->decompressor = ZSTD_createDCtx();
    if (!codec->decompressor) {
      goto fail;
    }
  }
  if (codec_configure(codec, level, checksum, window_log, dictionary, dictionary_size) < 0) {
    goto fail;
  }
  return codec;
fail:
  codec_free(codec);
  return NULL;
}

/* Only idle Buffer compressors are cached, at most 4 MiB including scratch
 * and Zstd allocations. Incremental decoders use fresh contexts. Active/yielded calls own their
 * handles exclusively; streams always use codec_create/codec_free. */
static Codec *cached_compressor;

Codec *codec_buffer_acquire(int compress, int level, int checksum, int window_log,
                            const void *dictionary, size_t dictionary_size) {
  if (!compress) {
    return codec_create(0, level, checksum, window_log, dictionary, dictionary_size);
  }
  Codec *codec = cached_compressor;
  cached_compressor = NULL;
  if (!codec) {
    return codec_create(compress, level, checksum, window_log, dictionary, dictionary_size);
  }
  last_error = NULL;
  codec->consumed = codec->produced = 0;
  if (codec_configure(codec, level, checksum, window_log, dictionary, dictionary_size) < 0) {
    codec_free(codec);
    return NULL;
  }
  return codec;
}

void codec_buffer_release(Codec *codec) {
  if (!codec) {
    return;
  }
  if (!codec->compressor) {
    codec_free(codec);
    return;
  }
  size_t status = ZSTD_CCtx_reset(codec->compressor, ZSTD_reset_session_and_parameters);
  size_t bytes = sizeof(Codec) + ZSTD_sizeof_CCtx(codec->compressor);
  /* Reset after errors/cancellation too. Do not overwrite the failing call's last_error. */
  if (!ZSTD_isError(status) && !cached_compressor && bytes <= CACHE_BYTES) {
    cached_compressor = codec;
  } else {
    codec_free(codec);
  }
}

/* A double carries JS safe integers without WASM32 truncation; -1 means unknown length. */
int codec_set_pledged_size(Codec *codec, double size, int include_size) {
  if (!codec || !codec->compressor) {
    last_error = "Content size requires a Zstd compressor";
    return -1;
  }
  if (!(size == -1 ||
        (size >= 0 && size <= 9007199254740991.0 && size == (unsigned long long)size))) {
    last_error = "Zstd content size must be a safe integer or -1";
    return -1;
  }
  if (check(ZSTD_CCtx_setParameter(codec->compressor, ZSTD_c_contentSizeFlag, include_size)) < 0) {
    return -1;
  }
  return size < 0 ? 0
                  : check(ZSTD_CCtx_setPledgedSrcSize(codec->compressor, (unsigned long long)size));
}

size_t codec_input_capacity(void) {
  return INPUT_BYTES;
}

size_t codec_output_capacity(Codec *codec) {
  return OUTPUT_BYTES;
}

uintptr_t codec_input(Codec *codec) {
  return (uintptr_t)codec->input;
}

uintptr_t codec_output(Codec *codec) {
  return (uintptr_t)codec->output;
}

size_t codec_consumed(Codec *codec) {
  return codec->consumed;
}

size_t codec_produced(Codec *codec) {
  return codec->produced;
}

int codec_process_at(Codec *codec, size_t size, int end, size_t offset, size_t capacity) {
  if (!codec || offset > INPUT_BYTES || size > INPUT_BYTES - offset || !capacity ||
      capacity > codec_output_capacity(codec)) {
    last_error = "Invalid Zstd scratch buffer range";
    return -1;
  }
  ZSTD_inBuffer input = {codec->input + offset, size, 0};
  ZSTD_outBuffer output = {codec->output, capacity, 0};
  if (codec->decompressor && !codec->header_checked) {
    size_t prefix = sizeof(codec->header) - codec->header_bytes;
    if (prefix > size) {
      prefix = size;
    }
    memcpy(codec->header + codec->header_bytes, input.src, prefix);
    ZSTD_frameHeader header;
    size_t status = ZSTD_getFrameHeader(&header, codec->header, codec->header_bytes + prefix);
    if (check(status) < 0) {
      return -1;
    }
    if (status == 0) {
      /* The upstream one-pass shortcut can bypass its streaming window limit. */
      if (header.windowSize > codec->max_window) {
        last_error = ZSTD_getErrorString(ZSTD_error_frameParameter_windowTooLarge);
        return -1;
      }
      codec->header_checked = 1;
    }
  }
  size_t status = codec->compressor ? ZSTD_compressStream2(codec->compressor, &output, &input,
                                                           end ? ZSTD_e_end : ZSTD_e_continue)
                                    : ZSTD_decompressStream(codec->decompressor, &output, &input);
  codec->consumed = input.pos;
  codec->produced = output.pos;
  if (codec->decompressor) {
    if (!codec->header_checked) {
      codec->header_bytes += input.pos;
    }
    /* decompressStream stops at each frame, including skippable frames. */
    if (status == 0) {
      codec->header_bytes = 0;
      codec->header_checked = 0;
    }
  }
  /* On WASM32 the largest valid skip hints also overlap the encoded error
   * range. Consult the decoder's input type before interpreting those hints. */
  if (status > INT_MAX && codec->decompressor &&
      ZSTD_nextInputType(codec->decompressor) == ZSTDnit_skippableFrame) {
    return INT_MAX;
  }
  return check(status);
}
