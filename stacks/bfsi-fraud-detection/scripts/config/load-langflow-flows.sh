#!/bin/bash
# Langflow MCP template setup - loads Fraud Agent with PG MCP template
# For bfsi-fraud-detection stack
# For demonstration purposes only.

set -e

# Configuration - use environment variable or default
LANGFLOW_URL="${LANGFLOW_URL:-http://langflow:7860}"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Langflow MCP Template Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Wait for Langflow
echo "⏳ Waiting for Langflow to be ready at $LANGFLOW_URL..."
for i in {1..60}; do
    if curl -s -f "$LANGFLOW_URL/health" > /dev/null 2>&1; then
        echo "✅ Langflow is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "❌ Langflow did not start in time"
        exit 1
    fi
    sleep 2
done

# Get user_id
echo "🔍 Fetching user_id from Langflow..."
USER_ID=$(curl -s $LANGFLOW_URL/api/v1/users/whoami | jq -r '.id' 2>/dev/null)

if [ -z "$USER_ID" ] || [ "$USER_ID" = "null" ]; then
    echo "⚠️  Could not fetch user_id, using template default"
    USER_ID=$(jq -r '.user_id' "./langflow-templates/fraud-agent-mcp.json")
else
    echo "✅ Got user_id: $USER_ID"
fi

# Get folder_id
echo "📁 Fetching folder_id from Langflow..."
ACCESS_TOKEN=$(curl -s -H "Accept: application/json" \
    $LANGFLOW_URL/api/v1/auto_login 2>/dev/null | jq -r '.access_token // empty')

if [ -n "$ACCESS_TOKEN" ] && [ "$ACCESS_TOKEN" != "null" ]; then
    FOLDERS_RESPONSE=$(curl -s -L -m 5 \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Accept: application/json" \
        $LANGFLOW_URL/api/v1/folders/ 2>/dev/null)

    if echo "$FOLDERS_RESPONSE" | jq empty 2>/dev/null; then
        FOLDER_ID=$(echo "$FOLDERS_RESPONSE" | jq -r '.[0].id // empty' 2>/dev/null)
        if [ -n "$FOLDER_ID" ] && [ "$FOLDER_ID" != "null" ]; then
            FOLDER_NAME=$(echo "$FOLDERS_RESPONSE" | jq -r '.[0].name // "Unknown"' 2>/dev/null)
            echo "✅ Got folder_id: $FOLDER_ID ($FOLDER_NAME)"
        else
            FOLDER_ID=$(jq -r '.folder_id' "./langflow-templates/fraud-agent-mcp.json")
            echo "📂 Using template folder_id: $FOLDER_ID"
        fi
    else
        FOLDER_ID=$(jq -r '.folder_id' "./langflow-templates/fraud-agent-mcp.json")
        echo "📂 Using template folder_id: $FOLDER_ID (API error)"
    fi
else
    FOLDER_ID=$(jq -r '.folder_id' "./langflow-templates/fraud-agent-mcp.json")
    echo "📂 Using template folder_id: $FOLDER_ID (auto-login disabled)"
fi

# Configure PG Airman MCP server in Langflow
echo ""
echo "🔧 Configuring PG Airman MCP server in Langflow..."

MCP_SERVER_NAME="pg-airman-mcp"
MCP_SSE_URL="http://pg-airman-mcp:8200/sse"

# Check if MCP server already exists
if [ -n "$ACCESS_TOKEN" ]; then
    # First, check if the MCP servers API endpoint exists
    MCP_API_CHECK=$(curl -s -w "%{http_code}" -o /dev/null -L -m 5 \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Accept: application/json" \
        "$LANGFLOW_URL/api/v1/mcp/servers" 2>/dev/null || echo "000")

    if [ "$MCP_API_CHECK" = "404" ] || [ "$MCP_API_CHECK" = "000" ]; then
        echo "⚠️  MCP servers API endpoint not available (this may be expected for some Langflow versions)"
        echo "   You can manually configure the MCP server in Langflow Settings → MCP Servers"
        echo "   MCP Server Name: $MCP_SERVER_NAME"
        echo "   Transport: SSE"
        echo "   URL: $MCP_SSE_URL"
    else
        # API endpoint exists, try to get existing servers
        MCP_LIST_RESPONSE=$(curl -s -L -m 5 \
            -H "Authorization: Bearer $ACCESS_TOKEN" \
            -H "Accept: application/json" \
            "$LANGFLOW_URL/api/v1/mcp/servers" 2>/dev/null || echo "[]")

        # Check if response is valid JSON
        if echo "$MCP_LIST_RESPONSE" | jq empty 2>/dev/null; then
            EXISTING_MCP=$(echo "$MCP_LIST_RESPONSE" | jq -r ".[] | select(.name == \"$MCP_SERVER_NAME\") | .name" 2>/dev/null || echo "")

            if [ "$EXISTING_MCP" = "$MCP_SERVER_NAME" ]; then
                echo "✅ MCP server '$MCP_SERVER_NAME' already configured"
            else
                echo "➕ Adding MCP server '$MCP_SERVER_NAME'..."

                # Create MCP server configuration
                MCP_CONFIG=$(cat <<EOF
{
  "name": "$MCP_SERVER_NAME",
  "transport": "sse",
  "config": {
    "url": "$MCP_SSE_URL"
  }
}
EOF
)

                # Try to add the MCP server
                ADD_RESPONSE=$(curl -s -X POST \
                    -H "Authorization: Bearer $ACCESS_TOKEN" \
                    -H "Content-Type: application/json" \
                    -H "Accept: application/json" \
                    "$LANGFLOW_URL/api/v1/mcp/servers" \
                    -d "$MCP_CONFIG" 2>&1 || echo "{}")

                if echo "$ADD_RESPONSE" | jq -e '.id' >/dev/null 2>&1; then
                    echo "✅ MCP server '$MCP_SERVER_NAME' added successfully"
                else
                    echo "⚠️  Could not add MCP server via API"
                    echo "   You may need to configure it manually in Langflow Settings"
                    echo "   MCP Server Name: $MCP_SERVER_NAME"
                    echo "   Transport: SSE"
                    echo "   URL: $MCP_SSE_URL"
                fi
            fi
        else
            echo "⚠️  Unexpected API response format"
            echo "   You may need to configure the MCP server manually in Langflow Settings"
            echo "   MCP Server Name: $MCP_SERVER_NAME"
            echo "   Transport: SSE"
            echo "   URL: $MCP_SSE_URL"
        fi
    fi
else
    echo "⚠️  No access token available, skipping MCP server configuration"
    echo "   You may need to configure it manually in Langflow Settings → MCP Servers"
    echo "   MCP Server Name: $MCP_SERVER_NAME"
    echo "   Transport: SSE"
    echo "   URL: $MCP_SSE_URL"
fi

# Load MCP template
echo ""
echo "📦 Loading Fraud Agent with PG MCP template..."

TEMPLATE_FILE="./langflow-templates/fraud-agent-mcp.json"

if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "❌ Template file not found: $TEMPLATE_FILE"
    exit 1
fi

FLOW_NAME=$(jq -r '.name' "$TEMPLATE_FILE")

# Check if flow already exists
echo "🔍 Checking for existing '$FLOW_NAME' flow..."

EXISTING_FLOWS=$(timeout 10 curl -s -H "Accept: application/json" $LANGFLOW_URL/api/v1/flows/ 2>/dev/null | gunzip 2>/dev/null || echo "[]")
EXISTING_FLOW=$(echo "$EXISTING_FLOWS" | jq -r "[.[] | select(.name == \"$FLOW_NAME\" or (.name | startswith(\"$FLOW_NAME (\")))] | .[0] // empty" 2>/dev/null || echo "")

if [ -n "$EXISTING_FLOW" ]; then
    EXISTING_ID=$(echo "$EXISTING_FLOW" | jq -r '.id' 2>/dev/null)
    EXISTING_NAME=$(echo "$EXISTING_FLOW" | jq -r '.name' 2>/dev/null)
    echo "✅ Flow already exists: '$EXISTING_NAME'"
    echo "   Flow ID: $EXISTING_ID"
    echo "   Skipping creation (idempotent)"
else
    # Create modified template with dynamic user_id, folder_id, and MCP server config
    echo "🔧 Injecting user_id, folder_id, and MCP server configuration..."

    MODIFIED_TEMPLATE=$(jq --arg uid "$USER_ID" \
                           --arg fid "$FOLDER_ID" \
                           --arg mcp_name "$MCP_SERVER_NAME" \
        '.user_id = $uid |
         .folder_id = $fid |
         .data.nodes |= map(
           if (.data.node.display_name // "") == "MCP Tools" then
             .data.node.template.mcp_server.value.name = $mcp_name |
             .data.node.template.mcp_server.value.config = {}
           else . end
         )' "$TEMPLATE_FILE")

    # Create the flow
    RESPONSE=$(curl -s -X POST $LANGFLOW_URL/api/v1/flows/ \
        -H "Content-Type: application/json" \
        -d "$MODIFIED_TEMPLATE" 2>&1)

    if echo "$RESPONSE" | grep -q '"id"'; then
        FLOW_ID=$(echo "$RESPONSE" | jq -r '.id // .flow_id // empty' 2>/dev/null)
        echo "✅ MCP template created successfully!"
        echo "   Flow: '$FLOW_NAME'"
        [ -n "$FLOW_ID" ] && echo "   Flow ID: $FLOW_ID"
    else
        echo "⚠️  Could not auto-load flow"
        echo "   Response: $RESPONSE"
        echo "   You can manually import: $TEMPLATE_FILE"
    fi
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Langflow MCP Template Ready!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🌐 Open: $LANGFLOW_URL"
echo ""
echo "🎉 Fraud Agent with PG MCP Template:"
echo ""
echo "   ✅ MCP Server: $MCP_SERVER_NAME"
echo "   ✅ Endpoint: $MCP_SSE_URL"
echo "   ✅ Transport: SSE (Server-Sent Events)"
echo "   ✅ Access Mode: Restricted (read-only)"
echo ""
echo "   Available MCP Tools:"
echo "   • execute_sql - Run SELECT queries"
echo "   • analyze_db_health - Database health checks"
echo "   • get_top_queries - Find slowest queries"
echo "   • explain_query - Query execution plans"
echo "   • list_schemas - List database schemas"
echo "   • list_objects - List tables/views"
echo "   • get_object_details - Table details"
echo "   • analyze_workload_indexes - Index recommendations"
echo "   • analyze_query_indexes - Query-specific indexes"
echo "   • add_comment_to_object - Add documentation"
echo ""
echo "📖 Documentation:"
echo "   • Integration Guide: docs/LANGFLOW_PG_AIRMAN_INTEGRATION.md"
echo "   • PG Airman: https://github.com/EnterpriseDB/pg-airman-mcp"
echo ""
echo "🚀 Try asking:"
echo "   • 'Check the health of my database'"
echo "   • 'What are the slowest queries?'"
echo "   • 'Show me fraud statistics for the last 24 hours'"
echo "   • 'Recommend indexes to improve performance'"
echo ""
