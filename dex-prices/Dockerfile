FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o dex-prices .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/dex-prices .
# Copy config files if they exist
COPY config.json . 

CMD ["./dex-prices"]
