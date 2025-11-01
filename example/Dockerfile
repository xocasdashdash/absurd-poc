# Simple Dockerfile - expects binary to be built locally
FROM alpine:latest

# Install CA certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the pre-built binary
COPY absurd-example /app/absurd-example

# Make it executable
RUN chmod +x /app/absurd-example

# Run the binary
ENTRYPOINT ["/app/absurd-example"]
