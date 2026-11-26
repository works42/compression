.DEFAULT_GOAL := wasm
IMAGE ?= compression-toolchain:4.0.10
JOBS ?= 4
RUN = docker run --rm --user $(shell id -u):$(shell id -g) -e EM_CACHE=/work/.local/cache/emscripten -v "$(CURDIR):/work" -w /work $(IMAGE)

.PHONY: toolchain wasm test-wasm format format-check
toolchain:
	docker build -t $(IMAGE) -f tools/Dockerfile .
wasm:
	$(RUN) emcmake cmake -S . -B .local/build -G Ninja -DCMAKE_BUILD_TYPE=Release
	$(RUN) cmake --build .local/build --target compression_zstd --parallel "$(JOBS)"
test-wasm:
	$(RUN) emcmake cmake -S . -B .local/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCOMPRESSION_BUILD_TESTS=ON
	$(RUN) cmake --build .local/build --target compression_codec_test --parallel "$(JOBS)"
	$(RUN) node .local/build/compression_codec_test.cjs
format:
	$(RUN) clang-format -i c/*.c tests/codec.c
	$(RUN) cmake-format -i CMakeLists.txt
format-check:
	$(RUN) clang-format --dry-run --Werror c/*.c tests/codec.c
	$(RUN) cmake-format --check CMakeLists.txt
