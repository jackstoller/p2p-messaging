FROM golang:1.24 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/node .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/monitor ./cmd/monitor

FROM docker:28-cli
WORKDIR /app
COPY --from=builder /out/node /app/node
COPY --from=builder /out/monitor /app/monitor
EXPOSE 9000
EXPOSE 8081
EXPOSE 8080
ENTRYPOINT ["/app/node"]
