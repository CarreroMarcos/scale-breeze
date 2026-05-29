FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install git for fetching dependencies
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o event-service .

FROM alpine:latest
RUN apk add --no-cache curl ca-certificates && \
    addgroup -S scalebreeze && adduser -S scalebreeze -G scalebreeze

WORKDIR /root/
COPY --from=builder /app/event-service .

USER scalebreeze

EXPOSE 8081

CMD ["./event-service"]
