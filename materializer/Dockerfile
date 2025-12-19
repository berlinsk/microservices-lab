FROM --platform=linux/amd64 swift:5.9-jammy AS build

WORKDIR /build
COPY Package.swift ./
RUN swift package resolve
COPY Sources ./Sources

RUN swift build -c release --static-swift-stdlib

FROM --platform=linux/amd64 ubuntu:22.04

RUN apt-get update && apt-get install -y \
    libcurl4 \
    libxml2 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --user-group --create-home --system --skel /dev/null --home-dir /app vapor

COPY --from=build /build/.build/release/App /app/
WORKDIR /app

USER vapor
EXPOSE 8085

ENTRYPOINT ["./App"]
