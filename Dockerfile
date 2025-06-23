FROM golang:1.23-bullseye AS builder

WORKDIR /app
COPY go.mod go.sum ./
COPY fsm/ /fsm/
RUN go mod download

COPY *.go ./
RUN go build -o flyd-lite

FROM debian:bullseye-slim

# Install required system packages for devicemapper
RUN apt-get update && apt-get install -y \
    util-linux \
    dmsetup \
    e2fsprogs \
    ca-certificates \
    thin-provisioning-tools \
    && rm -rf /var/lib/apt/lists/*

# Load required kernel modules for thin provisioning
RUN modprobe dm_thin_pool || echo "dm_thin_pool module not available"

WORKDIR /app
COPY --from=builder /app/flyd-lite .

# Create a directory for the images
RUN mkdir -p /app/images

# Run as root since devicemapper requires it
CMD ["./flyd-lite"]