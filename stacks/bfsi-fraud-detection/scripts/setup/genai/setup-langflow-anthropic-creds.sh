#!/bin/bash
# Setup Langflow Anthropic Global Variables
# For demonstration purposes only.

set -e

LANGFLOW_URL=${LANGFLOW_URL:-http://127.0.0.1:7861}

echo "=== Setting up Langflow A Credentials ==="
echo "Using ANTHROPIC_API"
echo "Langflow URL: $LANGFLOW_URL"
echo ""

# Get Langflow auth token (required for API calls in Langflow 1.9+)
echo "[0/2] Getting Langflow auth token..."
AUTH_RESPONSE=$(curl -s "$LANGFLOW_URL/api/v1/auto_login")
AUTH_TOKEN=$(echo "$AUTH_RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)
if [ -z "$AUTH_TOKEN" ]; then
    echo "ERROR: Failed to get Langflow auth token"
    exit 1
fi
echo "  Auth token obtained"
echo ""

# Function to create or update a variable
set_variable() {
    local name=$1
    local value=$2
    local type=${3:-Generic}

    # Check if variable exists (with auth)
    existing=$(curl -s -H "Authorization: Bearer $AUTH_TOKEN" "$LANGFLOW_URL/api/v1/variables/" | grep -o "\"id\":\"[^\"]*\",\"name\":\"$name\"" | head -1)

    if [ -n "$existing" ]; then
        # Extract ID and update
        id=$(echo "$existing" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
        curl -s -X PATCH "$LANGFLOW_URL/api/v1/variables/$id" \
            -H "Authorization: Bearer $AUTH_TOKEN" \
            -H "Content-Type: application/json" \
            -d "{\"name\": \"$name\", \"value\": \"$value\", \"type\": \"$type\", \"default_fields\": []}" > /dev/null
        echo "  Updated: $name"
    else
        # Create new
        curl -s -X POST "$LANGFLOW_URL/api/v1/variables/" \
            -H "Authorization: Bearer $AUTH_TOKEN" \
            -H "Content-Type: application/json" \
            -d "{\"name\": \"$name\", \"value\": \"$value\", \"type\": \"$type\", \"default_fields\": []}" > /dev/null
        echo "  Created: $name"
    fi
}

echo "[2/2] Setting ANTHROPIC_API_KEY..."
set_variable "ANTHROPIC_API_KEY" "$ANTHROPIC_API_KEY" "Credential"

echo ""
echo "=== Langflow Anthropic Credentials Ready ==="
echo "Variables set in Langflow Global Variables."
echo "Use the globe icon in Agent component to select them."
echo ""

