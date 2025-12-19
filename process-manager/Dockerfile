FROM --platform=linux/amd64 swift:5.9-jammy as build
WORKDIR /build
COPY Package.swift ./
RUN swift package resolve
COPY Sources ./Sources
RUN swift build -c release --static-swift-stdlib

FROM --platform=linux/amd64 ubuntu:22.04
RUN apt-get update && apt-get install -y libcurl4 libxml2 tzdata
WORKDIR /app
COPY --from=build /build/.build/release/App /app/
ENTRYPOINT ["/app/App"]


