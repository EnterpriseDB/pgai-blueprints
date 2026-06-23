"""
Fraud Rules Search Tool - Semantic + Text Search

For demonstration purposes only.

This tool searches fraud detection rules using:
1. AIDB BERT semantic search (primary) - finds conceptually similar content
2. ILIKE text search (fallback) - keyword matching on structured table

Semantic search enables queries like "North America 2024" to find US/CA rules
with 2024 effective dates, even without exact keyword matches.

AIDB Setup Required:
  docker exec -i bfsi-pgd psql -U postgres -d demo -f /scripts/setup-minio-aidb.sql
"""

import psycopg2
from lfx.custom.custom_component.component import Component
from lfx.io import MessageTextInput, IntInput, Output
from lfx.schema.data import Data


class AIDBRagTool(Component):
    display_name = "AIDB Semantic Search"
    description = (
        "Semantic search over fraud detection rulebooks using AIDB BERT embeddings. "
        "Understands concepts like 'North America 2024', 'crypto fraud', 'HITL thresholds'. "
        "Falls back to text search if AIDB is not configured."
    )
    icon = "brain"
    name = "AIDBRagTool"

    inputs = [
        MessageTextInput(
            name="query",
            display_name="Query",
            info="Natural language question about fraud rules (e.g., 'What rules apply to North America in 2024?')",
            tool_mode=True,
        ),
        IntInput(
            name="top_k",
            display_name="Max Results",
            info="Maximum number of results to return",
            value=5,
        ),
    ]

    outputs = [
        Output(display_name="Matching Rules", name="output", method="search_rules"),
    ]

    def search_rules(self) -> Data:
        """Search fraud rules - tries semantic search first, falls back to ILIKE"""
        query = self.query.strip()
        top_k = self.top_k

        if not query:
            return Data(value="Please provide a search query for fraud rules.")

        try:
            conn = psycopg2.connect(
                host='pgd',
                port=5432,
                user='postgres',
                password='secret',
                database='demo'
            )
            cur = conn.cursor()

            # Check if semantic search is available
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.routines
                WHERE routine_name = 'search_fraud_rules_semantic'
            """)
            has_semantic = cur.fetchone()[0] > 0

            if has_semantic:
                result = self._semantic_search(cur, query, top_k)
            else:
                result = self._text_search(cur, query, top_k)

            cur.close()
            conn.close()
            return result

        except Exception as e:
            error_msg = f"Error searching fraud rules: {str(e)}"
            self.status = error_msg
            return Data(value=error_msg)

    def _semantic_search(self, cur, query: str, top_k: int) -> Data:
        """Search using AIDB BERT semantic similarity"""
        self.status = "Searching with AIDB semantic search..."

        cur.execute("""
            SELECT chunk_id, source_doc, chunk_text, similarity
            FROM search_fraud_rules_semantic(%s, %s)
        """, (query, top_k))
        rows = cur.fetchall()

        if not rows:
            self.status = "No semantic results, trying text search..."
            return self._text_search(cur, query, top_k)

        response_text = f"Fraud Rules for: '{query}'\n"
        response_text += "=" * 60 + "\n"
        response_text += f"(Semantic search - {len(rows)} relevant sections)\n\n"

        for i, (chunk_id, source_doc, chunk_text, similarity) in enumerate(rows, 1):
            rule_id = source_doc.replace('.txt', '') if source_doc else 'Unknown'
            response_text += f"**{i}. {rule_id}** (Relevance: {similarity:.0%})\n"
            response_text += "-" * 40 + "\n"
            preview = chunk_text[:600] + "..." if len(chunk_text) > 600 else chunk_text
            response_text += f"{preview}\n\n"

        self.status = f"Found {len(rows)} relevant sections via semantic search"
        return Data(value=response_text)

    def _text_search(self, cur, query: str, top_k: int) -> Data:
        """Fallback to ILIKE text search on structured table"""
        self.status = "Using text search (AIDB not configured)..."

        search_terms = query.split()
        conditions = []
        params = []

        for term in search_terms:
            term_conditions = """(
                rule_id ILIKE %s OR rule_name ILIKE %s OR
                rule_description ILIKE %s OR rule_category ILIKE %s OR
                region ILIKE %s OR vendor ILIKE %s
            )"""
            conditions.append(term_conditions)
            pattern = f'%{term}%'
            params.extend([pattern] * 6)

        where_clause = ' AND '.join(conditions) if conditions else '1=1'

        sql = f"""
            SELECT rule_id, rule_name, rule_description, rule_category,
                   region, vendor, threshold_amount, risk_score_threshold, action
            FROM fraud_rules
            WHERE {where_clause}
            ORDER BY CASE WHEN region = 'GLOBAL' THEN 1 ELSE 0 END, rule_id
            LIMIT %s
        """
        params.append(top_k)

        cur.execute(sql, params)
        rows = cur.fetchall()

        if not rows:
            response_text = (
                f"No fraud rules found matching: '{query}'\n\n"
                "Tip: Run AIDB setup for semantic search:\n"
                "  docker exec -i bfsi-pgd psql -U postgres -d demo -f /scripts/setup-minio-aidb.sql\n\n"
                "Or try keywords: US, UK, CA, Stripe, PayPal, high value, velocity"
            )
            self.status = "No matching rules found"
            return Data(value=response_text)

        response_text = f"Fraud Rules matching: '{query}'\n"
        response_text += "=" * 60 + "\n"
        response_text += "(Text search - for semantic search, run AIDB setup)\n\n"

        for row in rows:
            rule_id, rule_name, rule_desc, category, region, vendor, threshold, risk_threshold, action = row
            response_text += f"**{rule_id}**: {rule_name}\n"
            response_text += f"   Region: {region or 'All'} | Vendor: {vendor or 'All'} | Action: {action}\n"
            if threshold:
                response_text += f"   Threshold: ${threshold:,.2f}\n"
            response_text += f"   {rule_desc}\n\n"

        self.status = f"Found {len(rows)} rules via text search"
        return Data(value=response_text)
