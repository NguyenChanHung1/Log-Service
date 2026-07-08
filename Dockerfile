FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

ARG SERVICE=log-api
RUN go build -o /out/service ./cmd/${SERVICE}

FROM alpine:3.20

RUN addgroup -S app && adduser -S app -G app && mkdir -p /tmp/log-service-spool && chown -R app:app /tmp/log-service-spool

WORKDIR /app
COPY --from=builder /out/service /app/service

USER app

EXPOSE 8080 8081 8082
ENTRYPOINT ["/app/service"]
