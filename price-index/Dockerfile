FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o price-index .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/price-index .
COPY exchange_info.json .

CMD ["./price-index"]
