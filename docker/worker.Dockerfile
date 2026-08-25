# syntax=docker/dockerfile:1

# The worker's image, built the same way as the API's and deliberately not
# shared with it.
#
# One image with two entrypoints would be smaller by a few megabytes and worse
# in every other way: the two processes would be redeployed together whether or
# not both changed, and an image that can start either is one environment
# variable away from starting the wrong one.

# ---- build ------------------------------------------------------------------
FROM golang:1.27-alpine AS build

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
    go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# ---- runtime ----------------------------------------------------------------
# Distroless carries no shell, no package manager and no busybox: an attacker
# with code execution has nothing to pivot to.
#
# The worker exposes no port and answers no probe. Its liveness is visible in
# the depth of the queue it is meant to be draining, which is a better signal
# than a process that is running: a worker wedged on a dependency passes any
# healthcheck it could plausibly serve itself, and shows up immediately as a
# backlog that stops moving.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/worker /worker

USER nonroot:nonroot

ENTRYPOINT ["/worker"]
