FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install git for fetching dependencies
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o event-service .

FROM alpine:latest
RUN apk add --no-cache curl
WORKDIR /root/
COPY --from=builder /app/event-service .

EXPOSE 8081

CMD ["./event-service"]
