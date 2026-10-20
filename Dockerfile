# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/quorumkv ./cmd/quorumkv \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/qkv ./cmd/qkv \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/quorumkv /out/qkv /usr/local/bin/
# Named volumes inherit this ownership, so the non-root user can write.
COPY --from=build --chown=65532:65532 /out/data /data
VOLUME /data
EXPOSE 7001
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/quorumkv"]
