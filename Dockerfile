FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o kvp

FROM alpine:3.23

WORKDIR /app

COPY --from=builder /app/kvp .
COPY index.html .

EXPOSE 8080

CMD ["./kvp"]
