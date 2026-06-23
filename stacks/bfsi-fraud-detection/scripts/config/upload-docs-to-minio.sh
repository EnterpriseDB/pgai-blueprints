#!/bin/bash
# ==============================================================================
# Upload Fraud Rule Documents to MinIO
# ==============================================================================
# For demonstration purposes only.
#
# This script uploads the .txt fraud rule documents to the fraud-rules bucket
# in MinIO, where AIDB/PGFS can access them for semantic search.
#
# Usage:
#   From host: ./scripts/upload-docs-to-minio.sh
#   From container: Run after minio-init has created the bucket
# ==============================================================================

set -e

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"
BUCKET_NAME="fraud-rules"
DOCS_DIR="$(dirname "$0")/../docs"

echo "╔══════════════════════════════════════════════════════╗"
echo "║  Upload Fraud Rule Documents to MinIO                 ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "MinIO Endpoint: $MINIO_ENDPOINT"
echo "Bucket: $BUCKET_NAME"
echo "Docs Directory: $DOCS_DIR"
echo ""

# Check if mc (MinIO client) is available
if ! command -v mc &> /dev/null; then
    echo "MinIO client (mc) not found. Using curl instead..."
    USE_CURL=true
else
    USE_CURL=false
    # Configure mc alias
    mc alias set myminio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api S3v4 2>/dev/null || true
fi

# Create bucket if it doesn't exist
echo "[1/3] Creating bucket if needed..."
if [ "$USE_CURL" = true ]; then
    # Create bucket via S3 API
    curl -sf -X PUT "$MINIO_ENDPOINT/$BUCKET_NAME" \
        -u "$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY" 2>/dev/null || true
else
    mc mb "myminio/$BUCKET_NAME" 2>/dev/null || echo "  Bucket already exists"
fi
echo "  Bucket: $BUCKET_NAME"

# Upload documents
echo ""
echo "[2/3] Uploading fraud rule documents..."
count=0
for doc in "$DOCS_DIR"/*.txt; do
    if [ -f "$doc" ]; then
        filename=$(basename "$doc")
        if [ "$USE_CURL" = true ]; then
            curl -sf -X PUT "$MINIO_ENDPOINT/$BUCKET_NAME/$filename" \
                -u "$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY" \
                -H "Content-Type: text/plain" \
                --data-binary "@$doc" 2>/dev/null
        else
            mc cp "$doc" "myminio/$BUCKET_NAME/$filename" 2>/dev/null
        fi
        echo "  Uploaded: $filename"
        ((count++))
    fi
done
echo ""
echo "  Total documents uploaded: $count"

# Verify upload
echo ""
echo "[3/3] Verifying uploads..."
if [ "$USE_CURL" = true ]; then
    echo "  Bucket contents (via S3 API):"
    curl -sf "$MINIO_ENDPOINT/$BUCKET_NAME" \
        -u "$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY" 2>/dev/null | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g' | head -5
    echo "  ..."
else
    mc ls "myminio/$BUCKET_NAME" | head -10
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Upload Complete!                                     ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo "  1. Run AIDB setup to create semantic search:"
echo "     docker exec -i bfsi-pgd psql -U postgres -d demo -f /scripts/setup-minio-aidb.sql"
echo ""
echo "  2. Test semantic search in psql:"
echo "     SELECT * FROM search_fraud_rules_semantic('North America 2024 rules', 3);"
echo ""
echo "  3. Use AIDBRagToolSemantic in Langflow for semantic queries"
echo ""
