#include <assert.h>
#include <math.h>
#include <stdio.h>
#include "../c/codec.c"
#include "common/huf.h"

static unsigned streamed_huffman_calls;

static size_t counted_huffman(HUF_DTable *table, void *output, size_t output_size,
                              const void *input, size_t input_size, void *workspace,
                              size_t workspace_size, int flags) {
  streamed_huffman_calls++;
  return HUF_decompress1X2_DCtx_wksp(table, output, output_size, input, input_size, workspace,
                                     workspace_size, flags);
}

/* Observe the actual selected decoder without exporting production diagnostics. */
#define HUF_decompress1X2_DCtx_wksp(...) counted_huffman(__VA_ARGS__)
#include "../c/decode.c"
#undef HUF_decompress1X2_DCtx_wksp

static void test_dictionary_cleanup(void) {
  size_t size = 4096;
  size_t dictionary_size = 8U * 1024U * 1024U;
  unsigned char *dictionary = malloc(dictionary_size);
  unsigned char frame[8192];
  assert(dictionary);
  memset(dictionary, 67, dictionary_size);
  memset(buffer_input, 66, size);

  for (int fail = 0; fail <= 1; fail++) {
    int bytes = codec_buffer_compress(buffer_output, fail ? 0 : sizeof(buffer_output), buffer_input,
                                      size, 3, 1, 1, dictionary, dictionary_size);
    assert(fail ? bytes == -1 : bytes > 0);
    assert(!buffer_compressor || ZSTD_sizeof_CCtx(buffer_compressor) <= CACHE_BYTES);
    /* Reset must release the dictionary itself, not just satisfy the cache budget. */
    if (buffer_compressor) {
      size_t before = ZSTD_sizeof_CCtx(buffer_compressor);
      assert(!ZSTD_isError(ZSTD_CCtx_loadDictionary(buffer_compressor, NULL, 0)));
      assert(ZSTD_sizeof_CCtx(buffer_compressor) == before);
    }
    if (fail) {
      assert(strstr(codec_error(), "Destination buffer is too small"));
    } else {
      assert((size_t)bytes < sizeof(frame));
      memcpy(frame, buffer_output, bytes);
      for (int too_small = 0; too_small <= 1; too_small++) {
        int decoded = codec_buffer_decompress(buffer_output, too_small ? size - 1 : size, frame,
                                              bytes, dictionary, dictionary_size);
        assert(too_small ? decoded == -2 : decoded == (int)size);
        assert(!buffer_decompressor || ZSTD_sizeof_DCtx(buffer_decompressor) <= CACHE_BYTES);
        if (buffer_decompressor) {
          size_t before = ZSTD_sizeof_DCtx(buffer_decompressor);
          assert(!ZSTD_isError(ZSTD_DCtx_loadDictionary(buffer_decompressor, NULL, 0)));
          assert(ZSTD_sizeof_DCtx(buffer_decompressor) == before);
        }
      }
    }
  }

  /* Prime an oversized reusable workspace, then return through the bounded API. */
  size_t large_size = 8U * 1024U * 1024U;
  size_t large_capacity = ZSTD_compressBound(large_size);
  unsigned char *large_input = calloc(1, large_size);
  unsigned char *large_output = malloc(large_capacity);
  assert(large_input && large_output);
  if (!buffer_compressor) {
    buffer_compressor = ZSTD_createCCtx();
  }
  assert(buffer_compressor);
  assert(!ZSTD_isError(ZSTD_CCtx_setParameter(buffer_compressor, ZSTD_c_compressionLevel, 19)));
  assert(!ZSTD_isError(
      ZSTD_compress2(buffer_compressor, large_output, large_capacity, large_input, large_size)));
  assert(!ZSTD_isError(ZSTD_CCtx_reset(buffer_compressor, ZSTD_reset_session_and_parameters)));
  assert(ZSTD_sizeof_CCtx(buffer_compressor) > CACHE_BYTES);
  free(large_output);
  free(large_input);
  memset(buffer_input, 73, sizeof(buffer_input));
  int bytes = codec_buffer_compress(buffer_output, sizeof(buffer_output), buffer_input,
                                    sizeof(buffer_input), 22, 1, 1, NULL, 0);
  assert(bytes > 0);
  assert(!buffer_compressor);
  free(dictionary);
}

static void test_arguments(void) {
  const int invalid_windows[] = {-1, 0, 9, 31, INT_MAX};
  for (size_t i = 0; i < sizeof(invalid_windows) / sizeof(invalid_windows[0]); i++) {
    assert(codec_buffer_size(buffer_input, 0, invalid_windows[i]) == -2);
    assert(!codec_create(0, 3, 1, invalid_windows[i], NULL, 0));
  }
  Codec *codec = codec_create(1, 3, 1, 27, NULL, 0);
  assert(codec);
  const double invalid_sizes[] = {NAN, INFINITY, -INFINITY, -2, -0.5, 1.5, 9007199254740992.0};
  for (size_t i = 0; i < sizeof(invalid_sizes) / sizeof(invalid_sizes[0]); i++) {
    assert(codec_set_pledged_size(codec, invalid_sizes[i], 1) == -1);
  }
  assert(codec_set_pledged_size(codec, -1, 0) == 0);
  assert(codec_set_pledged_size(codec, 9007199254740991.0, 1) == 0);
  codec_free(codec);
}

static void test_huffman_selection(void) {
  unsigned char plain[240];
  unsigned char frame[1024];
  unsigned char decoded[sizeof(plain)];
  unsigned state = 42;
  for (size_t i = 0; i < sizeof(plain); i++) {
    state = state * 1664525U + 1013904223U;
    plain[i] = (state >> 24) & 15;
  }
  size_t bytes = ZSTD_compress(frame, sizeof(frame), plain, sizeof(plain), 3);
  assert(!ZSTD_isError(bytes));
  streamed_huffman_calls = 0;
  assert(ZSTD_decompress(decoded, sizeof(decoded), frame, bytes) == sizeof(plain));
  assert(memcmp(decoded, plain, sizeof(plain)) == 0);
  assert(streamed_huffman_calls == 0);

  Codec *codec = codec_create(0, 3, 1, 27, NULL, 0);
  assert(codec);
  size_t total = 0;
  int status = 1;
  for (size_t offset = 0; offset < bytes; offset++) {
    codec->input[0] = frame[offset];
    status = codec_process_at(codec, 1, 0, 0, OUTPUT_BYTES);
    assert(status >= 0);
    assert(codec->consumed == 1);
    assert(total + codec->produced <= sizeof(decoded));
    memcpy(decoded + total, codec->output, codec->produced);
    total += codec->produced;
  }
  assert(status == 0);
  assert(total == sizeof(plain));
  assert(memcmp(decoded, plain, sizeof(plain)) == 0);
  assert(streamed_huffman_calls > 0);
  codec_free(codec);
}

int main(void) {
  test_arguments();
  test_dictionary_cleanup();
  test_huffman_selection();
  puts("C/WASM: argument validation, cache cleanup, and Huffman selection passed");
  return 0;
}
