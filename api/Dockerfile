FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    libpq-dev \
    libpqxx-dev \
    libhiredis-dev \
    nlohmann-json3-dev \
    libspdlog-dev \
    libfmt-dev \
    && rm -rf /var/lib/apt/lists/*

# Install single-header cpp-httplib manually
RUN curl -o /usr/local/include/httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/v0.14.1/httplib.h

WORKDIR /app
COPY . /app

RUN cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
# Use -j2 (not nproc) to prevent WSL RAM exhaustion during Docker builds
RUN cmake --build build -j2

EXPOSE 8080
CMD ["/app/build/api_server"]
