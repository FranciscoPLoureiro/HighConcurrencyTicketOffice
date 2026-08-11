# syntax=docker/dockerfile:1

# ---- build ------------------------------------------------------------------
FROM golang:1.26-alpine AS build

WORKDIR /src

# Manifests are copied and resolved before the source, so the dependency layer
# survives in the cache across every change that does not touch go.mod/go.sum.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH

# CGO_ENABLED=0 produces a fully static binary, which is what allows the
# runtime stage to be an image with no libc at all. -trimpath keeps build
# machine paths out of the binary, and -s -w drop the symbol and DWARF tables
# that nothing in production reads.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

# ---- runtime ----------------------------------------------------------------
# Distroless carries no shell, no package manager and no busybox: an attacker
# with code execution has nothing to pivot to. The cost is that the container
# healthcheck has nothing to run, which is why the binary probes itself.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/api /api

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/api"]
