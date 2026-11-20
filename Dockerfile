FROM golang:1.25 AS build
ARG GOPROXY=https://proxy.golang.org,direct
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -mod=readonly -trimpath -o /out/ingest ./cmd/ingest \
 && CGO_ENABLED=0 go build -trimpath -o /out/consumer ./cmd/consumer \
 && CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -trimpath -o /out/simulator ./cmd/simulator

FROM build AS bench-build
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -mod=readonly -trimpath -o /out/bench ./cmd/testbench

FROM golang:1.25 AS bench
COPY --from=bench-build /out/bench /tmp/bench
ENTRYPOINT ["sleep", "infinity"]

FROM golang:1.25
COPY --from=build /out/ /usr/local/bin/
COPY --from=bench-build /out/bench /usr/local/bin/testbench
WORKDIR /app
ENTRYPOINT []

