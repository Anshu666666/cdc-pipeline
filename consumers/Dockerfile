FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    librdkafka-dev \
    libavro-dev \
    libjansson-dev \
    libhiredis-dev \
    libspdlog-dev \
    libfmt-dev \
    nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install libserdes from source for Schema Registry & Avro support
RUN git clone https://github.com/confluentinc/libserdes.git /tmp/libserdes && \
    cd /tmp/libserdes && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    rm -rf /tmp/libserdes

# Install cpr (C++ Requests for Elasticsearch indexing)
RUN git clone --depth 1 https://github.com/libcpr/cpr.git /tmp/cpr && \
    cmake -B /tmp/cpr/build -S /tmp/cpr -DCPR_USE_SYSTEM_CURL=ON -DCPR_ENABLE_SSL=OFF -DCPR_BUILD_TESTS=OFF && \
    cmake --build /tmp/cpr/build -j$(nproc) && \
    cmake --install /tmp/cpr/build && \
    ldconfig && \
    rm -rf /tmp/cpr

WORKDIR /app
COPY . /app

RUN cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
RUN cmake --build build -j1

CMD ["/app/build/consumers"]
