#!/usr/bin/env python3
"""
Author: Raghavendra Rao Tadipathri (Raghav)
Email: raghavendra.rao@enterprise.com
License: EDB Corporation, MIT License

For demonstration purposes only.
"""

import csv
from pathlib import Path
import re
from datetime import datetime

class DatabaseLoader:
    """Load synthetic data to PostgreSQL or Oracle"""
    
    # Data type mappings for CREATE TABLE statements
    TYPE_MAPPINGS = {
        'postgresql': {
            'integer': 'INTEGER',
            'string': 'VARCHAR(255)',
            'decimal': 'NUMERIC(12,2)',
            'date': 'DATE',
            'boolean': 'BOOLEAN',
            'text': 'TEXT',
            'time': 'TIME'
        },
        'oracle': {
            'integer': 'NUMBER',
            'string': 'VARCHAR2(255)',
            'decimal': 'NUMBER(12,2)',
            'date': 'DATE',
            'boolean': 'NUMBER(1)',
            'text': 'CLOB',
            'time': 'VARCHAR2(8)'
        }
    }
    
    def __init__(self, db_type, connection_string):
        """
        Initialize database loader
        
        Args:
            db_type: 'postgresql' or 'oracle'
            connection_string: Database connection string
        """
        self.db_type = db_type.lower()
        self.connection_string = connection_string
        
        # Import appropriate driver
        if self.db_type == 'postgresql':
            try:
                import psycopg2
                self.psycopg2 = psycopg2
            except ImportError:
                raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")
        elif self.db_type == 'oracle':
            try:
                import oracledb
                self.oracledb = oracledb
            except ImportError:
                raise ImportError("oracledb not installed. Run: pip install oracledb")
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def test_connection(self):
        """Test database connection"""
        try:
            if self.db_type == 'postgresql':
                conn = self.psycopg2.connect(self.connection_string)
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                conn.close()
            else:  # oracle
                conn = self.oracledb.connect(self.connection_string)
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM DUAL")
                cur.close()
                conn.close()
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def create_tables(self, schema, drop_existing=False):
        """
        Create tables from schema
        
        Args:
            schema: Schema dictionary with table definitions
            drop_existing: If True, drop existing tables first
        """
        if self.db_type == 'postgresql':
            conn = self.psycopg2.connect(self.connection_string)
        else:  # oracle
            conn = self.oracledb.connect(self.connection_string)
        
        cur = conn.cursor()
        
        try:
            # Get tables in reverse dependency order for dropping
            tables = list(schema.get('tables', {}).keys())
            
            # Drop tables if requested
            if drop_existing:
                for table_name in reversed(tables):
                    if self.db_type == 'postgresql':
                        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                    else:  # oracle
                        try:
                            cur.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
                        except:
                            pass  # Table might not exist
            
            # Create tables
            for table_name, table_config in schema.get('tables', {}).items():
                create_sql = self._generate_create_table(table_name, table_config, schema)
                cur.execute(create_sql)
                print(f"  Created table: {table_name}")
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()
    
    def _generate_create_table(self, table_name, table_config, schema):
        """Generate CREATE TABLE statement"""
        columns = []
        
        # Get primary key
        pk = table_config.get('primary_key')
        
        # For simple schemas, infer columns from seed data structure
        # This is a basic implementation - extend based on your schema format
        if pk:
            # Add primary key column
            columns.append(f"{pk} {self.TYPE_MAPPINGS[self.db_type]['integer']} PRIMARY KEY")
        
        # Add foreign key columns from relationships
        for rel in schema.get('relationships', []):
            if rel.get('child') == table_name or rel.get('child_table') == table_name:
                parent = rel.get('parent') or rel.get('parent_table')
                parent_config = schema['tables'].get(parent, {})
                parent_pk = parent_config.get('primary_key')
                
                if parent_pk and table_config.get('foreign_keys'):
                    for fk_col, fk_ref in table_config['foreign_keys'].items():
                        if fk_col not in [pk]:  # Don't duplicate if it's also the PK
                            columns.append(f"{fk_col} {self.TYPE_MAPPINGS[self.db_type]['integer']}")
        
        # Add other columns (simplified - you may want to extend this)
        # For now, we'll rely on the CSV structure to define other columns
        
        # If no columns defined, create a basic structure
        if not columns:
            columns = [f"id {self.TYPE_MAPPINGS[self.db_type]['integer']} PRIMARY KEY"]
        
        create_sql = f"CREATE TABLE {table_name} (\n  "
        create_sql += ",\n  ".join(columns)
        create_sql += "\n)"
        
        return create_sql
    
    def load_csv_files(self, output_dir, generation_order, mode='truncate'):
        """
        Load CSV files into database using fastest method for each database
        
        Args:
            output_dir: Directory containing CSV files
            generation_order: List of table names in order to load
            mode: 'append', 'truncate' (default), or 'replace' - how to handle existing data
                  - 'append': Add data to existing tables (may cause PK conflicts)
                  - 'truncate': Clear tables before loading (default, safest)
                  - 'replace': Drop and recreate tables before loading
        """
        output_path = Path(output_dir)
        
        # Report the mode being used
        print(f"  Load mode: {mode.upper()}")
        
        # Handle different modes
        if mode == 'replace':
            print("  Action: Dropping and recreating tables before loading")
            # This would require schema info, so we just truncate instead
            self._truncate_tables(generation_order)
        elif mode == 'truncate':
            print("  Action: Truncating existing tables before loading")
            self._truncate_tables(generation_order)
        elif mode == 'append':
            print("  Action: Appending to existing tables (may cause conflicts)")
        
        # Load data based on database type
        if self.db_type == 'postgresql':
            return self._load_postgresql(output_path, generation_order)
        else:  # oracle
            return self._load_oracle(output_path, generation_order)
    
    def _truncate_tables(self, generation_order):
        """Truncate tables before loading new data"""
        if self.db_type == 'postgresql':
            conn = self.psycopg2.connect(self.connection_string)
        else:  # oracle
            conn = self.oracledb.connect(self.connection_string)
        
        cur = conn.cursor()
        tables_truncated = []
        
        try:
            # Truncate in reverse order to handle foreign keys
            for table_name in reversed(generation_order):
                try:
                    if self.db_type == 'postgresql':
                        cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                    else:  # oracle
                        cur.execute(f"TRUNCATE TABLE {table_name}")
                    tables_truncated.append(table_name)
                    print(f"    Truncated: {table_name}")
                except Exception as e:
                    # Table might not exist, continue
                    print(f"    Skipped truncate {table_name}: Table may not exist")
            
            conn.commit()
            
            if tables_truncated:
                print(f"  Truncation complete: {len(tables_truncated)} tables cleared")
            
        finally:
            cur.close()
            conn.close()
    
    def _load_postgresql(self, output_path, generation_order):
        """Load data into PostgreSQL using COPY command (fastest method)"""
        conn = self.psycopg2.connect(self.connection_string)
        cur = conn.cursor()
        tables_loaded = []
        
        print("\n  Loading CSV files to PostgreSQL:")
        
        try:
            for table_name in generation_order:
                csv_path = output_path / f"{table_name}.csv"
                if not csv_path.exists():
                    print(f"    ⚠ Warning: {csv_path.name} not found, skipping")
                    continue
                
                print(f"    Loading: {csv_path.name}")
                
                # Count rows for reporting
                with open(csv_path, 'r') as f:
                    row_count = sum(1 for line in f) - 1  # Subtract header
                
                # Use COPY for fast loading
                with open(csv_path, 'r') as f:
                    # Read header to get column names
                    header = next(f).strip()
                    columns = header.split(',')
                    
                    # COPY command
                    cur.copy_expert(
                        f"COPY {table_name} ({','.join(columns)}) FROM STDIN WITH CSV",
                        f
                    )
                
                tables_loaded.append(table_name)
                print(f"      ✓ Loaded {table_name}: {row_count:,} rows")
            
            conn.commit()
            
            print(f"\n  Load summary: {len(tables_loaded)} tables, "
                  f"{sum(1 for t in generation_order if (output_path / f'{t}.csv').exists())} CSV files processed")
            
            return tables_loaded
            
        except Exception as e:
            conn.rollback()
            print(f"    ✗ Error during load: {e}")
            raise e
        finally:
            cur.close()
            conn.close()
    
    def _parse_date(self, date_str):
        """Parse various date formats to Oracle-compatible format"""
        if not date_str or date_str == 'None':
            return None
        
        # Clean the date string
        date_str = date_str.strip()
        
        # Try various date formats
        date_formats = [
            '%Y-%m-%d',           # 2024-01-15
            '%Y-%m-%d %H:%M:%S',  # 2024-01-15 10:30:00
            '%m/%d/%Y',           # 01/15/2024
            '%m-%d-%Y',           # 01-15-2024
            '%d/%m/%Y',           # 15/01/2024
            '%d-%m-%Y',           # 15-01-2024
            '%Y/%m/%d',           # 2024/01/15
            '%d-%b-%Y',           # 15-Jan-2024
            '%d-%B-%Y',           # 15-January-2024
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # Return in Oracle's expected format
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If no format matches, return the original string
        # Oracle might handle it
        return date_str
    
    def _load_oracle(self, output_path, generation_order):
        """Load data into Oracle using batch inserts"""
        conn = self.oracledb.connect(self.connection_string)
        cursor = conn.cursor()
        tables_loaded = []
        
        # Set array size for better performance
        cursor.arraysize = 1000
        
        # Set date format for the session to handle various date formats
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
        
        print("\n  Loading CSV files to Oracle:")
        
        try:
            total_rows_loaded = 0
            
            for table_name in generation_order:
                csv_path = output_path / f"{table_name}.csv"
                if not csv_path.exists():
                    print(f"    ⚠ Warning: {csv_path.name} not found, skipping")
                    continue
                
                print(f"    Loading: {csv_path.name}")
                row_count = 0
                successful_rows = 0
                
                with open(csv_path, 'r') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    
                    # Identify which columns might be dates based on name patterns
                    date_columns = []
                    for idx, col in enumerate(header):
                        col_lower = col.lower()
                        if any(pattern in col_lower for pattern in ['date', 'time', 'created', 'updated', 'modified', 'birth', 'hire', 'start', 'end', 'joined']):
                            date_columns.append(idx)
                            print(f"      Detected potential date column: {col} (index {idx})")
                    
                    # Prepare insert statement
                    placeholders = ','.join([f':{i+1}' for i in range(len(header))])
                    sql = f"INSERT INTO {table_name} ({','.join(header)}) VALUES ({placeholders})"
                    
                    # Batch insert in chunks for better performance
                    batch = []
                    batch_size = 1000
                    
                    for row in reader:
                        # Process the row - handle dates and empty values
                        processed_row = []
                        for idx, value in enumerate(row):
                            if value == '' or value is None:
                                processed_row.append(None)
                            elif idx in date_columns and value:
                                # Try to standardize date format
                                try:
                                    # Handle various date formats
                                    date_value = self._parse_date(value)
                                    processed_row.append(date_value)
                                except:
                                    # If date parsing fails, use the value as-is
                                    processed_row.append(value)
                            else:
                                processed_row.append(value)
                        
                        batch.append(processed_row)
                        row_count += 1
                        
                        if len(batch) >= batch_size:
                            try:
                                cursor.executemany(sql, batch)
                                successful_rows += len(batch)
                            except self.oracledb.DatabaseError as e:
                                if "ORA-01861" in str(e):
                                    # Date format issue - try inserting one by one to identify problem row
                                    print(f"      Date format issue detected, processing rows individually...")
                                    for single_row in batch:
                                        try:
                                            cursor.execute(sql, single_row)
                                            successful_rows += 1
                                        except self.oracledb.DatabaseError as row_error:
                                            if "ORA-01861" in str(row_error):
                                                # Log the problematic row but continue
                                                print(f"        Skipped row with date format issue: {single_row[:3]}...")
                                            else:
                                                raise row_error
                                elif "ORA-01747" in str(e):
                                    # Column name issue - try with quoted columns
                                    print(f"      Column name issue detected, retrying with quoted columns...")
                                    quoted_columns = [f'"{col}"' for col in header]
                                    sql_quoted = f"INSERT INTO {table_name} ({','.join(quoted_columns)}) VALUES ({placeholders})"
                                    cursor.executemany(sql_quoted, batch)
                                    successful_rows += len(batch)
                                    sql = sql_quoted  # Use quoted SQL for remaining batches
                                else:
                                    raise e
                            batch = []
                    
                    # Insert remaining rows
                    if batch:
                        try:
                            cursor.executemany(sql, batch)
                            successful_rows += len(batch)
                        except self.oracledb.DatabaseError as e:
                            if "ORA-01861" in str(e):
                                # Date format issue - process individually
                                print(f"      Date format issue in final batch, processing individually...")
                                for single_row in batch:
                                    try:
                                        cursor.execute(sql, single_row)
                                        successful_rows += 1
                                    except self.oracledb.DatabaseError as row_error:
                                        if "ORA-01861" in str(row_error):
                                            print(f"        Skipped row with date format issue")
                                        else:
                                            raise row_error
                            elif "ORA-01747" in str(e):
                                # Column name issue - try with quoted columns
                                quoted_columns = [f'"{col}"' for col in header]
                                sql_quoted = f"INSERT INTO {table_name} ({','.join(quoted_columns)}) VALUES ({placeholders})"
                                cursor.executemany(sql_quoted, batch)
                                successful_rows += len(batch)
                            else:
                                raise e
                
                tables_loaded.append(table_name)
                total_rows_loaded += successful_rows
                if successful_rows < row_count:
                    print(f"      ✓ Loaded {table_name}: {successful_rows:,}/{row_count:,} rows")
                else:
                    print(f"      ✓ Loaded {table_name}: {successful_rows:,} rows")
            
            conn.commit()
            
            print(f"\n  Load summary: {len(tables_loaded)} tables, "
                  f"{sum(1 for t in generation_order if (output_path / f'{t}.csv').exists())} CSV files processed, "
                  f"{total_rows_loaded:,} total rows loaded")
            
            return tables_loaded
            
        except Exception as e:
            conn.rollback()
            print(f"    ✗ Error during load: {e}")
            # More detailed error information
            if "ORA-01861" in str(e):
                print(f"\n    Date Format Error:")
                print(f"    - The synthetic data contains dates in a format Oracle doesn't recognize")
                print(f"    - Common formats: YYYY-MM-DD, MM/DD/YYYY, DD-MON-YYYY")
                print(f"    - Oracle expects dates matching NLS_DATE_FORMAT settings")
            elif "ORA-01747" in str(e):
                print(f"\n    Column Name Error:")
                print(f"    - Column names may contain Oracle reserved words or special characters")
                print(f"    - Common problematic names: POSITION, LEVEL, TYPE, DATE, COMMENT")
            raise e
        finally:
            cursor.close()
            conn.close()
    
    def create_tables_from_csv(self, output_dir, generation_order, drop_existing=False):
        """
        Create tables by inferring structure from CSV files
        This is a helper method when schema doesn't have complete column definitions
        """
        output_path = Path(output_dir)
        
        if self.db_type == 'postgresql':
            conn = self.psycopg2.connect(self.connection_string)
        else:  # oracle
            conn = self.oracledb.connect(self.connection_string)
        
        cur = conn.cursor()
        
        try:
            # Drop tables if requested
            if drop_existing:
                for table_name in reversed(generation_order):
                    if self.db_type == 'postgresql':
                        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                    else:  # oracle
                        try:
                            cur.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
                        except:
                            pass
            
            # Create tables based on CSV structure
            for table_name in generation_order:
                csv_path = output_path / f"{table_name}.csv"
                if not csv_path.exists():
                    continue
                
                # Read header and sample data to infer types
                with open(csv_path, 'r') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    
                    # Read a few rows to infer data types
                    sample_rows = []
                    for i, row in enumerate(reader):
                        sample_rows.append(row)
                        if i >= 5:  # Sample 5 rows
                            break
                
                # Create table with inferred structure
                columns = []
                for i, col_name in enumerate(header):
                    # Simple type inference (extend as needed)
                    col_type = self._infer_column_type(col_name, [row[i] for row in sample_rows if i < len(row)])
                    columns.append(f"{col_name} {col_type}")
                
                create_sql = f"CREATE TABLE {table_name} (\n  "
                create_sql += ",\n  ".join(columns)
                create_sql += "\n)"
                
                cur.execute(create_sql)
                print(f"  Created table: {table_name}")
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()
    
    def _infer_column_type(self, col_name, sample_values):
        """Infer column type from sample values"""
        # Remove None/empty values
        values = [v for v in sample_values if v and v != '']
        
        if not values:
            return self.TYPE_MAPPINGS[self.db_type]['string']
        
        # Check if primary key (simple heuristic)
        if 'id' in col_name.lower() and col_name.endswith('_id'):
            return self.TYPE_MAPPINGS[self.db_type]['integer']
        
        # Check if all values are numeric
        try:
            for v in values:
                if '.' in v:
                    float(v)
                else:
                    int(v)
            if any('.' in v for v in values):
                return self.TYPE_MAPPINGS[self.db_type]['decimal']
            else:
                return self.TYPE_MAPPINGS[self.db_type]['integer']
        except:
            pass
        
        # Check if date
        if any(char in values[0] for char in ['-', '/']):
            if len(values[0].split('-')[0]) == 4 or len(values[0].split('/')[0]) == 4:
                return self.TYPE_MAPPINGS[self.db_type]['date']
        
        # Default to string
        return self.TYPE_MAPPINGS[self.db_type]['string']
