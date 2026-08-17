# WirePay Orchestrator - Multi-stage build (parametric by SERVICE_NAME)
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE_NAME=wire-order
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/wirepay-$SERVICE_NAME \
    ./cmd/$SERVICE_NAME/

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D wpo

COPY --from=builder /bin/wirepay-wire-order \
                    /bin/wirepay-wire-inventory \
                    /bin/wirepay-wire-payment \
                    /bin/wirepay-wire-notification \
                    /bin/wirepay-wire-ledger \
                    /usr/local/bin/

USER wpo
WORKDIR /app

EXPOSE 8080
