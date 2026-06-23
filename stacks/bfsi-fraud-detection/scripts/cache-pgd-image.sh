#!/bin/bash
# Cache PGD image to local registry for faster deployments
#
# For demonstration purposes only.
#
# Usage:
#   ./scripts/cache-pgd-image.sh push   # Build and push to local registry
#   ./scripts/cache-pgd-image.sh pull   # Pull from local registry (skip build)
#   ./scripts/cache-pgd-image.sh status # Check if cached image exists

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_DIR="$(dirname "$SCRIPT_DIR")"
REGISTRY_URL="${REGISTRY_URL:-localhost:5005}"
IMAGE_NAME="bfsi-pgd"
IMAGE_TAG="${IMAGE_TAG:-latest}"
FULL_IMAGE="${REGISTRY_URL}/${IMAGE_NAME}:${IMAGE_TAG}"

cd "$STACK_DIR"

start_registry() {
    if ! docker ps --format '{{.Names}}' | grep -q '^bfsi-registry$'; then
        echo "Starting local registry..."
        # Ensure cache directory exists (survives make clean)
        mkdir -p "${HOME}/.databox-cache/registry"
        docker compose --profile registry up -d registry
        sleep 2
    fi
}

case "${1:-status}" in
    push)
        echo "=== Caching PGD Image ==="

        # Check if local image exists
        if ! docker images bfsi-fraud-detection-pgd:latest -q | grep -q .; then
            echo "No local PGD image found. Building..."
            echo "(This requires EDB_SUBSCRIPTION_TOKEN and takes ~5 min)"
            docker compose build pgd
        else
            echo "Using existing local PGD image"
        fi

        # Start registry if not running
        start_registry

        # Tag for local registry
        echo "[1/2] Tagging image..."
        docker tag bfsi-fraud-detection-pgd:latest "$FULL_IMAGE"

        # Push to local registry
        echo "[2/2] Pushing to local registry..."
        docker push "$FULL_IMAGE"

        echo ""
        echo "=== PGD Image Cached ==="
        echo "Image: $FULL_IMAGE"
        echo ""
        echo "To use cached image, set environment variable:"
        echo "  export PGD_IMAGE=$FULL_IMAGE"
        echo "Then run: docker compose up -d"
        ;;

    pull)
        echo "=== Pulling Cached PGD Image ==="

        # Start registry if not running
        start_registry

        # Pull from local registry
        docker pull "$FULL_IMAGE"

        # Tag as the compose image name
        docker tag "$FULL_IMAGE" bfsi-fraud-detection-pgd:latest

        echo ""
        echo "=== PGD Image Ready ==="
        echo "Pulled: $FULL_IMAGE"
        echo "Tagged as: bfsi-fraud-detection-pgd:latest"
        ;;

    status)
        echo "=== PGD Image Cache Status ==="

        # Check local image
        echo ""
        echo "Local images:"
        docker images | grep -E "bfsi-pgd|bfsi-fraud-detection-pgd" || echo "  (none)"

        # Check registry
        echo ""
        echo "Registry ($REGISTRY_URL):"
        if docker ps --format '{{.Names}}' | grep -q '^bfsi-registry$'; then
            CATALOG=$(curl -s "http://${REGISTRY_URL}/v2/_catalog" 2>/dev/null)
            if echo "$CATALOG" | grep -q "$IMAGE_NAME"; then
                TAGS=$(curl -s "http://${REGISTRY_URL}/v2/${IMAGE_NAME}/tags/list" 2>/dev/null)
                echo "  $IMAGE_NAME: $(echo "$TAGS" | grep -o '"tags":\[[^]]*\]')"
            else
                echo "  (no cached images)"
            fi
        else
            echo "  Registry not running. Start with: docker compose --profile registry up -d"
        fi
        ;;

    *)
        echo "Usage: $0 {push|pull|status}"
        echo ""
        echo "Commands:"
        echo "  push   - Build PGD and push to local registry"
        echo "  pull   - Pull cached PGD from local registry"
        echo "  status - Show cache status"
        exit 1
        ;;
esac
