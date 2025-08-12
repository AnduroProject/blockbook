# ---------- build stage ----------
FROM ubuntu:22.04 AS build

LABEL maintainer="michael.casey@mara.com"
ARG DEBIAN_FRONTEND=noninteractive

# Toolchain & deps
RUN apt-get update && apt-get install -y \
    build-essential git pkg-config ca-certificates curl wget lsb-release \
    libzmq3-dev libsnappy-dev zlib1g-dev libbz2-dev libzstd-dev liblz4-dev \
    libssl-dev libgflags-dev \
 && rm -rf /var/lib/apt/lists/*

# Go (portable baseline)
ENV GOLANG_VERSION=1.24.4
RUN wget -q https://dl.google.com/go/go${GOLANG_VERSION}.linux-amd64.tar.gz \
 && tar -C /usr/local -xzf go${GOLANG_VERSION}.linux-amd64.tar.gz \
 && rm go${GOLANG_VERSION}.linux-amd64.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOAMD64=v1
ENV CGO_ENABLED=1

# Build RocksDB *portably*
# PORTABLE=1 disables -march=native; we also enforce generic flags.
ENV ROCKSDB_V=v9.10.0
RUN cd /opt && git clone -b ${ROCKSDB_V} --depth 1 https://github.com/facebook/rocksdb.git
ENV CFLAGS="-O2 -pipe -fPIC -march=x86-64 -mtune=generic"
ENV CXXFLAGS="${CFLAGS}"
# Extra safety: avoid over-eager vectorization on some hosts
ENV DISABLE_WARNING_AS_ERROR=1 PORTABLE=1 USE_RTTI=1 ROCKSDB_DISABLE_AVX512=1
RUN cd /opt/rocksdb && \
    make -j"$(nproc)" release

# CGO include & link flags for RocksDB
ENV CGO_CFLAGS="-I/opt/rocksdb/include ${CFLAGS}"
ENV CGO_CXXFLAGS="${CXXFLAGS}"
ENV CGO_LDFLAGS="-L/opt/rocksdb -lrocksdb -lstdc++ -lm -ldl -lz -lbz2 -lsnappy -llz4 -lzstd -lgflags"

# Build Blockbook
WORKDIR /src
# If you build your fork from context:
COPY . .

# Build blockbook binary
RUN go build -o /usr/local/bin/blockbook

# Run blockbook by default
ENTRYPOINT ["blockbook"]