FROM golang:1.21.6-bullseye as builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./
RUN go build -o main examples/hooks/sharedsub/main.go

FROM debian:bullseye-slim as deploy

WORKDIR /app
COPY --from=builder /app/main .

ENTRYPOINT ["./main"]