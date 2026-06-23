#!/usr/bin/env python3
"""
Author: Raghavendra Rao Tadipathri (Raghav)
Email: raghavendra.rao@enterprise.com
License: EDB Corporation, MIT License

For demonstration purposes only.
"""

import csv
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class CompatibilityChecker:
    """Check data compatibility with PostgreSQL and Oracle databases"""
    
    # Oracle reserved words (comprehensive list)
    ORACLE_RESERVED = {
        'ACCESS', 'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'AUDIT', 'BETWEEN',
        'BY', 'CHAR', 'CHECK', 'CLUSTER', 'COLUMN', 'COMMENT', 'COMPRESS', 'CONNECT',
        'CREATE', 'CURRENT', 'DATE', 'DECIMAL', 'DEFAULT', 'DELETE', 'DESC', 'DISTINCT',
        'DROP', 'ELSE', 'EXCLUSIVE', 'EXISTS', 'FILE', 'FLOAT', 'FOR', 'FROM', 'GRANT',
        'GROUP', 'HAVING', 'IDENTIFIED', 'IMMEDIATE', 'IN', 'INCREMENT', 'INDEX',
        'INITIAL', 'INSERT', 'INTEGER', 'INTERSECT', 'INTO', 'IS', 'LEVEL', 'LIKE',
        'LOCK', 'LONG', 'MAXEXTENTS', 'MINUS', 'MODE', 'MODIFY', 'NOAUDIT', 'NOCOMPRESS',
        'NOT', 'NOTFOUND', 'NOWAIT', 'NULL', 'NUMBER', 'OF', 'OFFLINE', 'ON', 'ONLINE',
        'OPTION', 'OR', 'ORDER', 'PCTFREE', 'PRIOR', 'PRIVILEGES', 'PUBLIC', 'RAW',
        'RENAME', 'RESOURCE', 'REVOKE', 'ROW', 'ROWID', 'ROWNUM', 'ROWS', 'SELECT',
        'SESSION', 'SET', 'SHARE', 'SIZE', 'SMALLINT', 'START', 'SUCCESSFUL', 'SYNONYM',
        'SYSDATE', 'TABLE', 'THEN', 'TO', 'TRIGGER', 'UID', 'UNION', 'UNIQUE', 'UPDATE',
        'USER', 'VALIDATE', 'VALUES', 'VARCHAR', 'VARCHAR2', 'VIEW', 'WHENEVER', 'WHERE',
        'WITH', 'POSITION', 'RANK', 'TYPE', 'END', 'BEGIN', 'EXCEPTION', 'PACKAGE',
        'PROCEDURE', 'FUNCTION', 'RETURN', 'RETURNING', 'CURSOR', 'OPEN', 'CLOSE', 'FETCH'
    }
    
    # PostgreSQL reserved words
    POSTGRESQL_RESERVED = {
        'ALL', 'ANALYSE', 'ANALYZE', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC', 'ASYMMETRIC',
        'AUTHORIZATION', 'BINARY', 'BOTH', 'CASE', 'CAST', 'CHECK', 'COLLATE', 'COLLATION',
        'COLUMN', 'CONCURRENTLY', 'CONSTRAINT', 'CREATE', 'CROSS', 'CURRENT_CATALOG',
        'CURRENT_DATE', 'CURRENT_ROLE', 'CURRENT_SCHEMA', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
        'CURRENT_USER', 'DEFAULT', 'DEFERRABLE', 'DESC', 'DISTINCT', 'DO', 'ELSE', 'END',
        'EXCEPT', 'FALSE', 'FETCH', 'FOR', 'FOREIGN', 'FREEZE', 'FROM', 'FULL', 'GRANT',
        'GROUP', 'HAVING', 'ILIKE', 'IN', 'INITIALLY', 'INNER', 'INTERSECT', 'INTO', 'IS',
        'ISNULL', 'JOIN', 'LATERAL', 'LEADING', 'LEFT', 'LIKE', 'LIMIT', 'LOCALTIME',
        'LOCALTIMESTAMP', 'NATURAL', 'NOT', 'NOTNULL', 'NULL', 'OFFSET', 'ON', 'ONLY',
        'OR', 'ORDER', 'OUTER', 'OVERLAPS', 'PLACING', 'PRIMARY', 'REFERENCES', 'RETURNING',
        'RIGHT', 'SELECT', 'SESSION_USER', 'SIMILAR', 'SOME', 'SYMMETRIC', 'TABLE', 'TABLESAMPLE',
        'THEN', 'TO', 'TRAILING', 'TRUE', 'UNION', 'UNIQUE', 'USER', 'USING', 'VARIADIC',
        'VERBOSE', 'WHEN', 'WHERE', 'WINDOW', 'WITH'
    }
    
    def __init__(self, db_type: str = 'oracle'):
        """
        Initialize the compatibility checker
        
        Args:
            db_type: 'oracle' or 'postgresql'
        """
        self.db_type = db_type.lower()
        self.issues = []
        self.warnings = []
        self.fixes = []
    
    def validate_directory(self, directory: str, generation_order: List[str] = None) -> bool:
        """
        Validate all CSV files in a directory for database compatibility
        
        Args:
            directory: Path to directory containing CSV files
            generation_order: Optional list of table names in load order
            
        Returns:
            True if compatible (can proceed), False if critical issues found
        """
        dir_path = Path(directory)
        if not dir_path.exists():
            print(f"  [-] Error: Directory not found: {directory}")
            return False
        
        print(f"\n  Running compatibility check for {self.db_type.upper()}...")
        
        # Get CSV files
        if generation_order:
            csv_files = [dir_path / f"{table}.csv" for table in generation_order 
                        if (dir_path / f"{table}.csv").exists()]
        else:
            csv_files = list(dir_path.glob('*.csv'))
        
        if not csv_files:
            print(f"  [!] No CSV files found in {directory}")
            return True
        
        total_issues = 0
        total_warnings = 0
        file_issues = {}
        
        # Check each file
        for csv_file in csv_files:
            self.issues = []
            self.warnings = []
            self.fixes = []
            
            self._check_csv_file(csv_file)
            
            if self.issues or self.warnings:
                file_issues[csv_file.name] = {
                    'issues': self.issues.copy(),
                    'warnings': self.warnings.copy(),
                    'fixes': self.fixes.copy()
                }
                total_issues += len(self.issues)
                total_warnings += len(self.warnings)
        
        # Report results
        print(f"    Files checked: {len(csv_files)}")
        print(f"    Critical issues: {total_issues}")
        print(f"    Warnings: {total_warnings}")
        
        if total_issues > 0:
            print(f"\n  [-] COMPATIBILITY CHECK FAILED")
            print(f"  Critical issues must be fixed before loading to {self.db_type.upper()}:\n")
            
            for filename, file_data in file_issues.items():
                if file_data['issues']:
                    print(f"    {filename}:")
                    for issue in file_data['issues']:
                        print(f"      • {issue['message']}")
                    
                    # Show fixes
                    relevant_fixes = [f for f in file_data['fixes'] 
                                    if any(f.get('identifier') == i.get('identifier') 
                                          for i in file_data['issues'])]
                    if relevant_fixes:
                        print(f"      Suggested fixes:")
                        for fix in relevant_fixes:
                            print(f"        - {fix['fix']}")
            
            print(f"\n  To fix these issues:")
            print(f"    1. Rename problematic columns in your model's seed data")
            print(f"    2. Regenerate the synthetic data")
            print(f"    3. Or use quoted identifiers in your database")
            
            return False
        
        elif total_warnings > 0:
            print(f"\n  [!] COMPATIBILITY CHECK PASSED WITH WARNINGS")
            
            # Show brief warning summary
            warning_types = set()
            for file_data in file_issues.values():
                for warning in file_data['warnings']:
                    warning_types.add(warning['type'])
            
            if 'date_format' in warning_types:
                print(f"    - Date format warnings detected (will be handled automatically)")
            if 'spaces' in warning_types:
                print(f"    - Column names with spaces (will use quoted identifiers)")
            
            print(f"  Data will be loaded with automatic handling of these issues.")
            return True
        
        else:
            print(f"\n  [+] COMPATIBILITY CHECK PASSED")
            print(f"  All data is compatible with {self.db_type.upper()}")
            return True
    
    def _check_csv_file(self, csv_path: Path):
        """Check a single CSV file for compatibility issues"""
        
        table_name = csv_path.stem
        
        # Check table name
        self._check_identifier(table_name, 'table')
        
        # Check CSV structure
        with open(csv_path, 'r') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                self.issues.append({
                    'type': 'empty_file',
                    'message': f'File {csv_path.name} is empty'
                })
                return
            
            # Check column names
            for col in header:
                self._check_identifier(col, 'column')
            
            # Sample data for type and format checking
            sample_rows = []
            for i, row in enumerate(reader):
                sample_rows.append(row)
                if i >= 10:  # Sample 10 rows
                    break
            
            # Check data formats
            self._check_data_formats(header, sample_rows)
    
    def _check_identifier(self, identifier: str, identifier_type: str):
        """Check if an identifier (table/column name) is valid"""
        if not identifier:
            self.issues.append({
                'type': 'empty_identifier',
                'identifier': identifier,
                'identifier_type': identifier_type,
                'message': f'Empty {identifier_type} name found'
            })
            return
        
        # Check for reserved words
        reserved_words = self.ORACLE_RESERVED if self.db_type == 'oracle' else self.POSTGRESQL_RESERVED
        
        if identifier.upper() in reserved_words:
            self.issues.append({
                'type': 'reserved_word',
                'identifier': identifier,
                'identifier_type': identifier_type,
                'message': f'{identifier_type.capitalize()} "{identifier}" is a reserved word'
            })
            self.fixes.append({
                'identifier': identifier,
                'fix': f'Rename to "{identifier}_col" or "{identifier}_val"'
            })
        
        # Check for special characters (excluding underscore which is valid)
        special_chars = re.findall(r'[^a-zA-Z0-9_]', identifier)
        if special_chars:
            self.issues.append({
                'type': 'special_characters',
                'identifier': identifier,
                'identifier_type': identifier_type,
                'message': f'{identifier_type.capitalize()} "{identifier}" contains special characters: {list(set(special_chars))}'
            })
            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', identifier)
            self.fixes.append({
                'identifier': identifier,
                'fix': f'Replace with: "{clean_name}"'
            })
        
        # Check for spaces (warning only, can be handled with quotes)
        if ' ' in identifier:
            self.warnings.append({
                'type': 'spaces',
                'identifier': identifier,
                'identifier_type': identifier_type,
                'message': f'{identifier_type.capitalize()} "{identifier}" contains spaces'
            })
        
        # Check if starts with number
        if identifier and identifier[0].isdigit():
            self.issues.append({
                'type': 'starts_with_number',
                'identifier': identifier,
                'identifier_type': identifier_type,
                'message': f'{identifier_type.capitalize()} "{identifier}" starts with a number'
            })
            self.fixes.append({
                'identifier': identifier,
                'fix': f'Prefix with letter: "col_{identifier}"'
            })
        
        # Check length limits
        max_length = 30 if self.db_type == 'oracle' else 63
        if len(identifier) > max_length:
            self.issues.append({
                'type': 'too_long',
                'identifier': identifier,
                'identifier_type': identifier_type,
                'message': f'{identifier_type.capitalize()} "{identifier}" exceeds {max_length} characters'
            })
            self.fixes.append({
                'identifier': identifier,
                'fix': f'Shorten to: "{identifier[:max_length]}"'
            })
    
    def _check_data_formats(self, headers: List[str], sample_rows: List[List[str]]):
        """Check data formats for compatibility issues"""
        
        for col_idx, col_name in enumerate(headers):
            col_values = [row[col_idx] if col_idx < len(row) else '' for row in sample_rows]
            
            # Check for date format issues
            if self._is_likely_date_column(col_name):
                self._check_date_formats(col_name, col_values)
            
            # Check for null bytes
            for value in col_values:
                if value and '\x00' in value:
                    self.issues.append({
                        'type': 'null_byte',
                        'column': col_name,
                        'message': f'Column "{col_name}" contains null bytes'
                    })
                    break
            
            # Check for oversized values (Oracle specific)
            if self.db_type == 'oracle':
                for value in col_values:
                    if value and len(value) > 4000:
                        self.warnings.append({
                            'type': 'large_value',
                            'column': col_name,
                            'message': f'Column "{col_name}" has values > 4000 chars'
                        })
                        break
    
    def _is_likely_date_column(self, col_name: str) -> bool:
        """Check if column name suggests it contains dates"""
        date_patterns = ['date', 'time', 'created', 'updated', 'modified', 
                        'birth', 'hire', 'start', 'end', 'joined', 'timestamp']
        col_lower = col_name.lower()
        return any(pattern in col_lower for pattern in date_patterns)
    
    def _check_date_formats(self, col_name: str, values: List[str]):
        """Check date format compatibility"""
        non_iso_formats = set()
        
        for value in values:
            if not value or value == 'None':
                continue
            
            # Check if it's not in ISO format
            if not re.match(r'^\d{4}-\d{2}-\d{2}', value):
                if re.match(r'^\d{2}/\d{2}/\d{4}', value):
                    non_iso_formats.add('MM/DD/YYYY')
                elif re.match(r'^\d{2}-\d{2}-\d{4}', value):
                    non_iso_formats.add('MM-DD-YYYY')
                elif value:
                    non_iso_formats.add('Other')
        
        if non_iso_formats:
            self.warnings.append({
                'type': 'date_format',
                'column': col_name,
                'message': f'Column "{col_name}" uses non-ISO date formats: {", ".join(non_iso_formats)}'
            })
