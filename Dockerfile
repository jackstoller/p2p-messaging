FROM golang:1.24 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/node .

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /out/node /app/node
EXPOSE 9000
ENTRYPOINT ["/app/node"]
