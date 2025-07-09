########################################
# Stage 1: Build Environment
########################################
FROM golang:1.20 AS builder

# Set environment variables
ENV GOPATH=/go
ENV PATH=$PATH:$GOPATH/bin
ENV ROCKSDB_VERSION=v9.10.0

# Install required tools and libs
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    pkg-config \
    lxc-dev \
    libzmq3-dev \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    libzstd-dev \
    liblz4-dev \
    libssl-dev \
    curl \
    ca-certificates \
    && apt-get clean

# Download and build RocksDB
RUN cd /opt && \
    git clone --depth 1 -b $ROCKSDB_VERSION https://github.com/facebook/rocksdb.git && \
    cd rocksdb && \
    make -j4 shared_lib && \
    make install-shared

# Copy blockbook source code
WORKDIR /app
COPY . .

# Set CGO flags to include RocksDB
ENV CGO_CFLAGS="-I/usr/local/include"
ENV CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"

# Build blockbook binary
RUN go build -o build/blockbook .

########################################
# Stage 2: Runtime Image
########################################
FROM debian:bullseye-slim

# Install required runtime dependencies
RUN apt-get update && apt-get install -y \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    libgflags2.2 \
    libzmq5 \
    libssl1.1 \
    ca-certificates \
    && apt-get clean

# Set working directory
WORKDIR /blockbook

# Copy built binary from builder
COPY --from=builder /app/build/blockbook /usr/local/bin/blockbook

# Run blockbook by default
ENTRYPOINT ["blockbook"]
