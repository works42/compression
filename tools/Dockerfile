FROM emscripten/emsdk:4.0.10
RUN apt-get update && apt-get install -y --no-install-recommends ninja-build && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir clang-format==19.1.7 cmakelang==0.6.13
WORKDIR /work
