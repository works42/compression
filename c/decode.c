/* Keep upstream one-pass Huffman choice for small buffers, but use its
 * two-symbol implementation for streamed single-bitstream literal blocks.
 * The pinned upstream source is included without modifying its files. */
#include "common/huf.h"
#include <zstd.h>

#if ZSTD_VERSION_NUMBER != 10600
#error "Review the streamed Huffman selection before upgrading Zstd"
#endif

#define HUF_decompress1X1_DCtx_wksp(...)                                                           \
  (streaming == is_streaming ? HUF_decompress1X2_DCtx_wksp(__VA_ARGS__)                            \
                             : HUF_decompress1X1_DCtx_wksp(__VA_ARGS__))

#include "decompress/zstd_decompress_block.c"

#undef HUF_decompress1X1_DCtx_wksp
