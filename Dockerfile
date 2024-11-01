# Build stage
FROM golang:1.22.5-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o nimbus .

# Final stage
FROM alpine:3.17
WORKDIR /app
COPY --from=builder /app/nimbus .
RUN chmod +x ./nimbus
ENTRYPOINT ["/app/nimbus"]