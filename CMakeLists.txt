cmake_minimum_required(VERSION 3.20)
project(compression LANGUAGES C)
if(NOT EMSCRIPTEN)
  message(FATAL_ERROR "Build with emcmake")
endif()
set(CMAKE_C_STANDARD 99)
include(FetchContent)
set(ZSTD_VERSION 1.6.0)
FetchContent_Declare(
  zstd
  URL "https://github.com/meta/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz"
  URL_HASH SHA256=2ba599ae2a257976278e7e71e998433a387bfd18aa7b78678229fd5d84f5e9b8
  TLS_VERIFY TRUE
  TIMEOUT 120
  INACTIVITY_TIMEOUT 30
  # Compile only the library sources below, without the upstream build targets.
  SOURCE_SUBDIR lib)
FetchContent_MakeAvailable(zstd)
set(ZSTD_LIB_DIR "${zstd_SOURCE_DIR}/lib")
file(GLOB ZSTD_COMMON CONFIGURE_DEPENDS "${ZSTD_LIB_DIR}/common/*.c")
file(GLOB ZSTD_COMPRESS CONFIGURE_DEPENDS "${ZSTD_LIB_DIR}/compress/*.c")
file(GLOB ZSTD_DECOMPRESS CONFIGURE_DEPENDS "${ZSTD_LIB_DIR}/decompress/*.c")
list(REMOVE_ITEM ZSTD_DECOMPRESS "${ZSTD_LIB_DIR}/decompress/zstd_decompress_block.c")
add_executable(compression_zstd c/codec.c c/decode.c ${ZSTD_COMMON} ${ZSTD_COMPRESS}
                                ${ZSTD_DECOMPRESS})
target_include_directories(compression_zstd PRIVATE "${ZSTD_LIB_DIR}" "${ZSTD_LIB_DIR}/common")
# Select upstream's non-prefetch sequence implementation for this WASM build; it still supports
# large offsets. c/decode.c selects the single-stream Huffman implementation without changing
# one-pass buffers.
target_compile_definitions(compression_zstd PRIVATE ZSTD_DISABLE_ASM=1
                                                    ZSTD_FORCE_DECOMPRESS_SEQUENCES_SHORT=1)
target_compile_options(compression_zstd PRIVATE -O2 -flto)
set(CODEC_EXPORTS
    "'_malloc'"
    "'_free'"
    "'_codec_error'"
    "'_codec_min_level'"
    "'_codec_max_level'"
    "'_codec_buffer_input'"
    "'_codec_buffer_capacity'"
    "'_codec_buffer_output'"
    "'_codec_buffer_bound'"
    "'_codec_buffer_compress'"
    "'_codec_buffer_size'"
    "'_codec_frame_content_size'"
    "'_codec_buffer_decompress'"
    "'_codec_create'"
    "'_codec_buffer_acquire'"
    "'_codec_buffer_release'"
    "'_codec_set_pledged_size'"
    "'_codec_free'"
    "'_codec_input_capacity'"
    "'_codec_output_capacity'"
    "'_codec_input'"
    "'_codec_output'"
    "'_codec_consumed'"
    "'_codec_produced'"
    "'_codec_process_at'")
list(JOIN CODEC_EXPORTS "," CODEC_EXPORTS)

target_link_options(
  compression_zstd
  PRIVATE
  -O2
  -flto
  --no-entry
  -sMODULARIZE=1
  -sEXPORT_ES6=1
  -sENVIRONMENT=web,worker,node
  -sFILESYSTEM=0
  -sALLOW_MEMORY_GROWTH=1
  -sINITIAL_MEMORY=8388608
  -sMAXIMUM_MEMORY=2147483648
  -sABORTING_MALLOC=0
  "-sEXPORTED_RUNTIME_METHODS=['HEAPU8','UTF8ToString']"
  "-sEXPORTED_FUNCTIONS=[${CODEC_EXPORTS}]")
set_target_properties(compression_zstd PROPERTIES OUTPUT_NAME zstd SUFFIX .mjs)

option(COMPRESSION_BUILD_TESTS "Build the C/WASM regression tests" OFF)
if(COMPRESSION_BUILD_TESTS)
  # The test includes codec.c and decode.c to inspect private caches and decoder selection.
  add_executable(compression_codec_test tests/codec.c ${ZSTD_COMMON} ${ZSTD_COMPRESS}
                                        ${ZSTD_DECOMPRESS})
  target_include_directories(compression_codec_test PRIVATE "${ZSTD_LIB_DIR}"
                                                            "${ZSTD_LIB_DIR}/common")
  target_compile_definitions(compression_codec_test PRIVATE ZSTD_DISABLE_ASM=1
                                                            ZSTD_FORCE_DECOMPRESS_SEQUENCES_SHORT=1)
  target_compile_options(compression_codec_test PRIVATE -O2 -UNDEBUG -fsanitize=undefined
                                                        -fno-sanitize-recover=undefined)
  target_link_options(
    compression_codec_test
    PRIVATE
    -O2
    -fsanitize=undefined
    -fno-sanitize-recover=undefined
    -sENVIRONMENT=node
    -sALLOW_MEMORY_GROWTH=1
    -sABORTING_MALLOC=0
    -sEXIT_RUNTIME=1
    -sASSERTIONS=1)
  set_target_properties(compression_codec_test PROPERTIES SUFFIX .cjs)
endif()
