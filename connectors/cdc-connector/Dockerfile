FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o app .

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /app/app .
EXPOSE 8080
# Use a non-root user for better security
USER nonroot:nonroot

CMD ["./app"]
