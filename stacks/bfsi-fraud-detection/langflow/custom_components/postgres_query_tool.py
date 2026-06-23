"""
Postgres Query Tool - Execute SQL queries against PostgreSQL databases

For demonstration purposes only.

This tool allows dynamic connection to any PostgreSQL database and execution
of SQL queries with parameterized connection settings.
"""

import json
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Any, Dict, List
from lfx.custom.custom_component.component import Component
from lfx.io import StrInput, IntInput, MessageTextInput, MultilineInput, Output
from lfx.schema.data import Data


class PostgresQueryTool(Component):
    display_name = "Postgres Query Tool"
    description = (
        "Execute SQL queries against PostgreSQL databases with configurable "
        "connection parameters. Returns query results as formatted JSON."
    )
    icon = "database"
    name = "PostgresQueryTool"

    inputs = [
        StrInput(
            name="host",
            display_name="Host",
            info="PostgreSQL server hostname or IP",
            value="pgd",
        ),
        IntInput(
            name="port",
            display_name="Port",
            info="PostgreSQL server port",
            value=5432,
        ),
        StrInput(
            name="database",
            display_name="Database",
            info="Database name to connect to",
            value="demo",
        ),
        StrInput(
            name="user",
            display_name="Username",
            info="PostgreSQL username",
            value="postgres",
        ),
        StrInput(
            name="password",
            display_name="Password",
            info="PostgreSQL password",
            value="secret",
            password=True,
        ),
        MultilineInput(
            name="query",
            display_name="SQL Query",
            info="SQL query to execute (SELECT, INSERT, UPDATE, DELETE)",
            tool_mode=True,
        ),
        IntInput(
            name="max_rows",
            display_name="Max Rows",
            info="Maximum number of rows to return (0 = all)",
            value=100,
        ),
    ]

    outputs = [
        Output(display_name="Query Results", name="output", method="execute_query"),
    ]

    def execute_query(self) -> Data:
        """Execute SQL query and return results"""
        host = self.host
        port = self.port
        database = self.database
        user = self.user
        password = self.password
        query = self.query.strip()
        max_rows = self.max_rows

        # Validate query is not empty
        if not query:
            error_msg = "❌ Error: SQL query cannot be empty"
            self.status = error_msg
            return Data(value=error_msg)

        try:
            # Connect to database
            self.status = f"Connecting to {host}:{port}/{database}..."
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                cursor_factory=RealDictCursor,
            )
            cur = conn.cursor()

            # Execute query
            self.status = "Executing query..."
            cur.execute(query)

            # Check if query returns results (SELECT)
            if cur.description:
                # Fetch results
                if max_rows > 0:
                    rows = cur.fetchmany(max_rows)
                else:
                    rows = cur.fetchall()

                # Convert to list of dicts
                results = [dict(row) for row in rows]
                row_count = len(results)

                # Check if there are more rows
                more_rows = False
                if max_rows > 0 and row_count == max_rows:
                    # Try to fetch one more to see if there are more rows
                    extra = cur.fetchone()
                    if extra:
                        more_rows = True

                # Format response
                response = {
                    "success": True,
                    "row_count": row_count,
                    "columns": [desc[0] for desc in cur.description],
                    "rows": results,
                    "more_rows": more_rows,
                }

                # Create human-readable output
                output_text = f"📊 Query Results\n{'=' * 50}\n\n"
                output_text += f"Rows returned: {row_count}"
                if more_rows:
                    output_text += f" (limited to {max_rows}, more available)"
                output_text += "\n\n"

                if row_count > 0:
                    # Show column names
                    output_text += f"Columns: {', '.join(response['columns'])}\n\n"

                    # Show first few rows in readable format
                    preview_count = min(5, row_count)
                    output_text += f"Preview (first {preview_count} rows):\n"
                    for i, row in enumerate(results[:preview_count], 1):
                        output_text += f"\nRow {i}:\n"
                        for key, value in row.items():
                            output_text += f"  {key}: {value}\n"

                    if row_count > preview_count:
                        output_text += f"\n... and {row_count - preview_count} more rows\n"

                    # Add full JSON for downstream processing
                    output_text += f"\n\n{'=' * 50}\n"
                    output_text += "Full Results (JSON):\n"
                    output_text += json.dumps(response, indent=2, default=str)
                else:
                    output_text += "No rows returned.\n"

                self.status = f"✅ Query executed: {row_count} rows returned"

            else:
                # Query doesn't return results (INSERT, UPDATE, DELETE, etc.)
                conn.commit()
                affected_rows = cur.rowcount

                response = {
                    "success": True,
                    "affected_rows": affected_rows,
                    "query_type": "modification",
                }

                output_text = f"✅ Query Executed Successfully\n{'=' * 50}\n\n"
                output_text += f"Rows affected: {affected_rows}\n\n"
                output_text += json.dumps(response, indent=2)

                self.status = f"✅ Query executed: {affected_rows} rows affected"

            cur.close()
            conn.close()

            return Data(value=output_text)

        except psycopg2.Error as e:
            # Database error
            error_msg = f"❌ Database Error\n{'=' * 50}\n\n"
            error_msg += f"Error Code: {e.pgcode}\n"
            error_msg += f"Error: {str(e)}\n\n"
            error_msg += f"Query:\n{query}\n"

            self.status = f"❌ Database error: {str(e)}"

            return Data(value=error_msg)

        except Exception as e:
            # General error
            error_msg = f"❌ Error: {str(e)}\n\n"
            error_msg += f"Query:\n{query}\n"

            self.status = f"❌ Error: {str(e)}"

            return Data(value=error_msg)
