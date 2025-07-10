FROM ubuntu:22.04

LABEL maintainer="michael.casey@mara.com"
LABEL version="0.01"
LABEL description="Docker file for anduro governance"

ARG DEBIAN_FRONTEND=noninteractive

# Install dependencies if needed
RUN apt-get update && apt-get install -y \
    build-essential git pkg-config libzmq3-dev \
    libsnappy-dev zlib1g-dev libbz2-dev libzstd-dev liblz4-dev \
    libssl-dev libgflags-dev lsb-release ca-certificates curl wget\
    && rm -rf /var/lib/apt/lists/*

ENV GOLANG_VERSION=1.24.4
RUN wget https://dl.google.com/go/go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    rm go${GOLANG_VERSION}.linux-amd64.tar.gz
ENV PATH="/usr/local/go/bin:$PATH"

RUN cd /opt && git clone -b v9.10.0 --depth 1 https://github.com/facebook/rocksdb.git
RUN cd /opt/rocksdb && CFLAGS=-fPIC CXXFLAGS=-fPIC PORTABLE=0 DISABLE_WARNING_AS_ERROR=1 make -j 4 release

# Set RocksDB environment flags (optional)
ENV CGO_CFLAGS="-I/opt/rocksdb/include"
ENV CGO_LDFLAGS="-L/opt/rocksdb -ldl -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"

# Set working directory
WORKDIR /blockbook

# Copy source code
COPY . .

# Build blockbook binary
RUN go build -o /usr/local/bin/blockbook

# Run blockbook by default
ENTRYPOINT ["blockbook"]
