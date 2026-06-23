#!/usr/bin/env python3
"""
Author: Raghavendra Rao Tadipathri (Raghav)
Email: raghavendra.rao@enterprise.com
License: EDB Corporation, MIT License

For demonstration purposes only.

edb-synthdb -  EDB Synthetic Data Generator using SDV/GaussianCopulaSynthesizer
               Creates database schemas and generates realistic synthetic data 
               using SDV's GaussianCopulaSynthesizer. Supports Oracle and 
               PostgreSQL with multiple data models.

"""

import json
import pandas as pd
import numpy as np
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

class ModelGenerator:
    """Generate synthetic data for any model"""
    
    def __init__(self, model_name: str, models_dir: str = "models", output_base_dir: str = None):
        self.model_name = model_name
        self.models_dir = Path(models_dir)
        
        # Construct file paths
        self.schema_file = self.models_dir / f"{model_name}_schema.json"
        self.seed_file = self.models_dir / f"{model_name}_seed_data.json"
        
        # Set output directory - allow custom location
        if output_base_dir:
            # Validate custom output directory exists
            custom_path = Path(output_base_dir)
            if not custom_path.exists():
                print(f"Error: Custom output directory '{output_base_dir}' does not exist!")
                print("Falling back to default output directory...")
                # Fall back to default
                self.output_dir = Path("output") / model_name
            else:
                # Use custom output directory
                self.output_dir = custom_path
                # If the path doesn't include the model name, add it as a subdirectory
                if self.output_dir.name != model_name:
                    self.output_dir = self.output_dir / model_name
        else:
            # Default: output directory in current location
            self.output_dir = Path("output") / model_name
        
        # Check if model exists
        if not self.schema_file.exists():
            raise FileNotFoundError(f"Schema not found: {self.schema_file}")
        if not self.seed_file.exists():
            raise FileNotFoundError(f"Seed data not found: {self.seed_file}")
        
        # Load schema
        with open(self.schema_file) as f:
            self.schema = json.load(f)
        
        self.tables = self.schema.get('tables', {})
        self.relationships = self.schema.get('relationships', [])
        
        # Initialize state
        self.seed_data = {}
        self.synthetic_data = {}
        self.synthesizers = {}
        self.generation_order = self._calculate_generation_order()
    
    def _calculate_generation_order(self) -> List[str]:
        """Determine table generation order"""
        dependencies = defaultdict(set)
        all_tables = set(self.tables.keys())
        
        for rel in self.relationships:
            parent = rel.get('parent', rel.get('parent_table'))
            child = rel.get('child', rel.get('child_table'))
            if parent and child:
                dependencies[child].add(parent)
        
        order = []
        visited = set()
        
        def visit(table):
            if table in visited:
                return
            visited.add(table)
            for parent in dependencies.get(table, []):
                visit(parent)
            order.append(table)
        
        for table in all_tables:
            visit(table)
        
        return order
    
    def load_seed_data(self):
        """Load seed data from JSON file"""
        with open(self.seed_file) as f:
            all_data = json.load(f)
        
        for table_name in self.tables:
            if table_name in all_data:
                self.seed_data[table_name] = pd.DataFrame(all_data[table_name])
        
        return sum(len(df) for df in self.seed_data.values())
    
    def train(self):
        """Train synthesizers for each table"""
        for table_name, df in self.seed_data.items():
            metadata = SingleTableMetadata()
            metadata.detect_from_dataframe(df)
            
            # Set primary key if defined
            table_config = self.tables.get(table_name, {})
            if 'primary_key' in table_config:
                pk = table_config['primary_key']
                if pk in df.columns:
                    metadata.set_primary_key(pk)
            
            synthesizer = GaussianCopulaSynthesizer(metadata)
            synthesizer.fit(df)
            self.synthesizers[table_name] = synthesizer
    
    def generate(self, scale: Optional[float] = None, total_rows: Optional[int] = None):
        """Generate synthetic data"""
        # Calculate rows per table
        if total_rows:
            # Distribute total rows proportionally
            seed_total = sum(len(df) for df in self.seed_data.values())
            row_distribution = {}
            for table_name, df in self.seed_data.items():
                proportion = len(df) / seed_total
                row_distribution[table_name] = int(total_rows * proportion)
            
            # Adjust for rounding
            diff = total_rows - sum(row_distribution.values())
            if diff > 0 and row_distribution:
                # Add difference to largest table
                largest_table = max(row_distribution.keys(), key=lambda k: row_distribution[k])
                row_distribution[largest_table] += diff
        else:
            # Use scale factor
            scale = scale or 10.0
            row_distribution = {
                table: int(len(df) * scale) 
                for table, df in self.seed_data.items()
            }
        
        # Generate each table
        for table_name in self.generation_order:
            if table_name not in self.synthesizers:
                continue
            
            num_rows = row_distribution.get(table_name, 100)
            
            # Generate base data
            synthetic_df = self.synthesizers[table_name].sample(num_rows=num_rows)
            
            # Fix primary keys
            table_config = self.tables.get(table_name, {})
            if 'primary_key' in table_config:
                pk = table_config['primary_key']
                if pk in synthetic_df.columns:
                    synthetic_df[pk] = range(1, num_rows + 1)
            
            # Fix foreign keys
            for rel in self.relationships:
                child = rel.get('child', rel.get('child_table'))
                if child == table_name:
                    parent = rel.get('parent', rel.get('parent_table'))
                    
                    if 'foreign_keys' in table_config:
                        for fk_col, fk_ref in table_config['foreign_keys'].items():
                            if parent in fk_ref and parent in self.synthetic_data:
                                parent_df = self.synthetic_data[parent]
                                parent_pk = self.tables[parent].get('primary_key')
                                if parent_pk and parent_pk in parent_df.columns:
                                    valid_keys = parent_df[parent_pk].values
                                    synthetic_df[fk_col] = np.random.choice(valid_keys, size=num_rows)
            
            self.synthetic_data[table_name] = synthetic_df
        
        return sum(len(df) for df in self.synthetic_data.values())
    
    def validate_integrity(self, detailed=False):
        """
        Validate referential integrity of generated data
        
        Returns:
            tuple: (all_valid, valid_count, failed_count)
        """
        all_valid = True
        validation_results = []
        
        for rel in self.relationships:
            parent = rel.get('parent', rel.get('parent_table'))
            child = rel.get('child', rel.get('child_table'))
            
            if parent not in self.synthetic_data or child not in self.synthetic_data:
                continue
            
            parent_df = self.synthetic_data[parent]
            child_df = self.synthetic_data[child]
            
            # Find foreign key column
            child_config = self.tables.get(child, {})
            if 'foreign_keys' not in child_config:
                continue
            
            for fk_col, fk_ref in child_config['foreign_keys'].items():
                if parent not in fk_ref:
                    continue
                
                parent_pk = self.tables[parent].get('primary_key')
                if not parent_pk or parent_pk not in parent_df.columns:
                    continue
                
                # Get valid parent keys and child foreign keys
                valid_parents = set(parent_df[parent_pk].dropna())
                child_fks = child_df[fk_col].dropna()
                child_fk_set = set(child_fks)
                
                # Check for invalid foreign keys
                invalid = child_fk_set - valid_parents
                is_valid = len(invalid) == 0
                
                if not is_valid:
                    all_valid = False
                
                validation_results.append(is_valid)
                
                if detailed and not is_valid:
                    print(f"  FAILED: {child}.{fk_col} -> {parent}.{parent_pk}")
                    print(f"    Invalid keys: {list(invalid)[:5]}")
        
        valid_count = sum(validation_results)
        failed_count = len(validation_results) - valid_count
        
        return all_valid, valid_count, failed_count
    
    def save(self):
        """Save synthetic data to CSV files"""
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save each table to CSV
        for table_name, df in self.synthetic_data.items():
            file_path = self.output_dir / f"{table_name}.csv"
            df.to_csv(file_path, index=False)
        
        # Report absolute path for clarity
        abs_path = self.output_dir.resolve()
        return abs_path
    
    def validate_model(self):
        """Validate model schema and seed data"""
        print(f"Validating model: {self.model_name}")
        print("="*50)
        
        issues = []
        warnings = []
        
        # Check schema structure
        print("\nSchema validation:")
        
        if not self.tables:
            issues.append("No tables defined in schema")
        else:
            print(f"  Tables found: {len(self.tables)}")
            
            # Check each table
            for table_name, table_config in self.tables.items():
                if 'primary_key' not in table_config:
                    warnings.append(f"Table '{table_name}' has no primary key defined")
                else:
                    print(f"  {table_name}: PK={table_config['primary_key']}")
        
        # Check relationships
        print("\nRelationship validation:")
        if not self.relationships:
            warnings.append("No relationships defined")
        else:
            print(f"  Relationships found: {len(self.relationships)}")
            
            for i, rel in enumerate(self.relationships):
                parent = rel.get('parent', rel.get('parent_table'))
                child = rel.get('child', rel.get('child_table'))
                
                if not parent or not child:
                    issues.append(f"Relationship {i+1}: missing parent or child")
                elif parent not in self.tables:
                    issues.append(f"Relationship {i+1}: parent table '{parent}' not found")
                elif child not in self.tables:
                    issues.append(f"Relationship {i+1}: child table '{child}' not found")
                else:
                    print(f"  {parent} -> {child}")
        
        # Check seed data
        print("\nSeed data validation:")
        with open(self.seed_file) as f:
            seed_data = json.load(f)
        
        for table_name in self.tables:
            if table_name not in seed_data:
                issues.append(f"No seed data for table '{table_name}'")
            else:
                rows = len(seed_data[table_name])
                if rows == 0:
                    issues.append(f"Table '{table_name}' has no seed data rows")
                elif rows < 3:
                    warnings.append(f"Table '{table_name}' has only {rows} rows (recommend at least 3)")
                else:
                    print(f"  {table_name}: {rows} rows")
        
        # Check referential integrity in seed data
        print("\nReferential integrity check:")
        integrity_valid = True
        
        for rel in self.relationships:
            parent = rel.get('parent', rel.get('parent_table'))
            child = rel.get('child', rel.get('child_table'))
            
            if parent in seed_data and child in seed_data:
                parent_df = pd.DataFrame(seed_data[parent])
                child_df = pd.DataFrame(seed_data[child])
                
                parent_pk = self.tables[parent].get('primary_key')
                child_config = self.tables.get(child, {})
                
                if parent_pk and 'foreign_keys' in child_config:
                    for fk_col, fk_ref in child_config['foreign_keys'].items():
                        if parent in fk_ref and fk_col in child_df.columns:
                            valid_parents = set(parent_df[parent_pk])
                            child_fks = set(child_df[fk_col].dropna())
                            invalid = child_fks - valid_parents
                            
                            if invalid:
                                issues.append(f"Invalid foreign keys in {child}.{fk_col}: {invalid}")
                                integrity_valid = False
                            else:
                                print(f"  Valid: {child}.{fk_col} -> {parent}.{parent_pk}")
        
        if integrity_valid and self.relationships:
            print("  All foreign keys valid")
        
        # Summary
        print("\n" + "="*50)
        if issues:
            print("VALIDATION FAILED")
            print("\nIssues found:")
            for issue in issues:
                print(f"  ERROR: {issue}")
        else:
            print("VALIDATION PASSED")
        
        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(f"  WARNING: {warning}")
        
        return len(issues) == 0


def list_models(models_dir: str = "models"):
    """List all available models"""
    models_path = Path(models_dir)
    
    if not models_path.exists():
        print(f"Models directory '{models_dir}' not found")
        return
    
    print("Available models:")
    print("-" * 50)
    
    # Find all schema files
    schema_files = list(models_path.glob("*_schema.json"))
    
    if not schema_files:
        print("No models found. Add model files to the models directory.")
        print("Expected format: <model>_schema.json and <model>_seed_data.json")
        return
    
    for schema_file in schema_files:
        model_name = schema_file.stem.replace('_schema', '')
        seed_file = models_path / f"{model_name}_seed_data.json"
        
        try:
            with open(schema_file) as f:
                schema = json.load(f)
            
            tables = schema.get('tables', {})
            relationships = schema.get('relationships', [])
            
            status = "Ready" if seed_file.exists() else "Missing seed data"
            
            print(f"\nModel: {model_name}")
            print(f"  Status: {status}")
            print(f"  Tables: {len(tables)} ({', '.join(tables.keys())})")
            print(f"  Relationships: {len(relationships)}")
            
            if seed_file.exists():
                with open(seed_file) as f:
                    seed_data = json.load(f)
                total_rows = sum(len(v) for v in seed_data.values())
                print(f"  Seed rows: {total_rows}")
        
        except Exception as e:
            print(f"\nModel: {model_name}")
            print(f"  Error: {e}")


def main():
    parser = argparse.ArgumentParser(description='EDB Synthetic Data simulator for Oracle and Postgres')
    parser.add_argument('--version', action='version', version='EDB Synthetic Data Simulator version 1.0')
    parser.add_argument('--model', '-m', help='Model name (e.g., hr, finance)')
    parser.add_argument('--scale', '-s', type=float, help='Scale factor for generation (max: 100)')
    parser.add_argument('--total-rows', '-t', type=int, help='Total rows to generate (max: 1 million)')
    parser.add_argument('--validate', '-v', action='store_true', help='Validate model schema and data')
    parser.add_argument('--list', '-l', action='store_true', help='List available models')
    parser.add_argument('--models-dir', default='models', help='Models directory (default: models)')
    parser.add_argument('--output-dir', '-o', help='Output directory for generated CSV files (default: output/<model>)')
    
    # Database loading arguments
    parser.add_argument('--load-db', choices=['postgresql', 'oracle'], 
                       help='Load generated data to database')
    parser.add_argument('--conn', help='Database connection string')
    parser.add_argument('--recreate-tables', action='store_true', 
                       help='Drop and recreate tables before loading')
    parser.add_argument('--db-mode', choices=['append', 'truncate', 'replace'], 
                       default='truncate',
                       help='Database load mode: truncate (default), append, or replace')
    parser.add_argument('--skip-compat-check', action='store_true',
                       help='Skip database compatibility check (not recommended)')
    
    args = parser.parse_args()
    
    # List models
    if args.list:
        list_models(args.models_dir)
        return
    
    # Require model name for other operations
    if not args.model:
        print("Error: Model name required")
        print("\nUsage examples:")
        print("  python edb-synthdb.py --list")
        print("  python edb-synthdb.py --model hr --scale 10")
        print("  python edb-synthdb.py --model hr --total-rows 10000")
        print("  python edb-synthdb.py --model hr --validate")
        print("\nOutput directory examples:")
        print("  python edb-synthdb.py --model hr --scale 10 --output-dir /tmp/synthetic_data")
        print("  python edb-synthdb.py --model hr --scale 10 --output-dir ./my_data/hr_test")
        print("\nDatabase loading examples:")
        print("  python edb-synthdb.py --model hr --scale 10 --load-db postgresql --conn 'postgresql://DB_USER_PLACEHOLDER:DB_PASSWORD_PLACEHOLDER@DB_HOST_PLACEHOLDER:port/db'")
        print("  python edb-synthdb.py --model hr --scale 10 --load-db oracle --conn 'DB_USER_PLACEHOLDER/DB_PASSWORD_PLACEHOLDER@DB_HOST_PLACEHOLDER:1521/ORCL'")
        return
    
    # Check database arguments
    if args.load_db and not args.conn:
        print("Error: --conn required when using --load-db")
        return
    
    if args.conn and not args.load_db:
        print("Error: --load-db required when using --conn. Must specify 'postgresql' or 'oracle'")
        return
    
    # Validate connection string format matches database type
    if args.conn and args.load_db:
        conn_str = args.conn.lower()
        if args.load_db == 'postgresql':
            if not (conn_str.startswith('postgresql://') or conn_str.startswith('postgres://')):
                print("Error: PostgreSQL connection string must start with 'postgresql://' or 'postgres://'")
                print("Example: postgresql://DB_USER_PLACEHOLDER:DB_PASSWORD_PLACEHOLDER@DB_HOST_PLACEHOLDER:5432/database")
                return
        elif args.load_db == 'oracle':
            if 'postgresql://' in conn_str or 'postgres://' in conn_str:
                print("Error: Oracle connection string cannot contain PostgreSQL format")
                print("Example: DB_USER_PLACEHOLDER/DB_PASSWORD_PLACEHOLDER@DB_HOST_PLACEHOLDER:1521/ORCL")
                return
    
    # Check if both scale and total-rows are provided
    if args.scale and args.total_rows:
        print("Error: Cannot use both --scale and --total-rows. Choose one.")
        return
    
    # Validate limits
    MAX_SCALE = 100  # 100x is reasonable
    MAX_TOTAL_ROWS = 1000000  # 1 million
    
    if args.scale:
        if args.scale <= 0:
            print("Error: Scale must be greater than 0")
            return
        if args.scale > MAX_SCALE:
            print(f"Error: Scale cannot exceed {MAX_SCALE}")
            return
    
    if args.total_rows:
        if args.total_rows <= 0:
            print("Error: Total rows must be greater than 0")
            return
        if args.total_rows > MAX_TOTAL_ROWS:
            print(f"Error: Total rows cannot exceed {MAX_TOTAL_ROWS:,}")
            return
    
    try:
        # Initialize generator with custom output directory if provided
        generator = ModelGenerator(args.model, args.models_dir, args.output_dir)
        
        # Validate model
        if args.validate:
            generator.validate_model()
            return
        
        # Default scale if neither provided
        if not args.scale and not args.total_rows:
            args.scale = 10.0
        
        print("="*60)
        print("EDB Synthetic Data Simualator for Oracle and Postgres v1.0")
        print("="*60)
        
        # Configuration summary
        print("\nCONFIGURATION:")
        print(f"  Model: {args.model}")
        print(f"  Tables: {len(generator.tables)}")
        print(f"  Relationships: {len(generator.relationships)}")
        if args.scale:
            print(f"  Generation mode: Scale")
            print(f"  Scale factor: {args.scale}x")
        else:
            print(f"  Generation mode: Total rows")
            print(f"  Target rows: {args.total_rows:,}")
        
        # Show output directory configuration
        if args.output_dir:
            print(f"  Output directory: {generator.output_dir} (custom)")
        else:
            print(f"  Output directory: {generator.output_dir} (default)")
        
        if args.load_db:
            print(f"  Database: {args.load_db}")
            print(f"  DB Mode: {args.db_mode}")
            print(f"  Recreate tables: {args.recreate_tables}")
            print(f"  Compatibility check: {'Disabled' if args.skip_compat_check else 'Enabled'}")
        
        print("\n" + "-"*60)
        
        # Phase 1: Load
        print("\nPHASE 1: LOADING DATA")
        seed_count = generator.load_seed_data()
        print(f"  Loaded {seed_count} seed records from {len(generator.tables)} tables")
        
        # Phase 2: Train  
        print("\nPHASE 2: TRAINING MODELS")
        generator.train()
        print(f"  Trained {len(generator.synthesizers)} table models")
        
        # Phase 3: Generate
        print("\nPHASE 3: GENERATING SYNTHETIC DATA")
        if args.total_rows:
            generated_count = generator.generate(total_rows=args.total_rows)
        else:
            generated_count = generator.generate(scale=args.scale)
        print(f"  Generated {generated_count:,} synthetic records")
        
        # Phase 4: Validate
        print("\nPHASE 4: VALIDATING INTEGRITY")
        is_valid, valid_count, failed_count = generator.validate_integrity(detailed=False)
        if is_valid:
            print(f"  All {valid_count} relationships validated successfully")
        else:
            print(f"  Validation failed: {failed_count} relationships have issues")
            generator.validate_integrity(detailed=True)
        
        # Phase 5: Save
        print("\nPHASE 5: SAVING OUTPUT")
        output_path = generator.save()
        print(f"  Saved to: {output_path}")
        
        # Phase 6: Database Compatibility Check (if loading to database)
        if args.load_db and not args.skip_compat_check:
            print("\nPHASE 6: DATABASE COMPATIBILITY CHECK")
            try:
                from db.compatibility_checker import CompatibilityChecker
                
                checker = CompatibilityChecker(args.load_db)
                is_compatible = checker.validate_directory(
                    generator.output_dir, 
                    generator.generation_order
                )
                
                if not is_compatible:
                    print("\n" + "="*60)
                    print("DATABASE LOADING CANCELLED")
                    print("="*60)
                    print("Please fix the compatibility issues and try again.")
                    print("Or use --skip-compat-check to force loading (not recommended).")
                    return
                
            except ImportError:
                print("  [!] Warning: Compatibility checker not found")
                print("  Proceeding without compatibility check...")
        elif args.load_db and args.skip_compat_check:
            print("\nPHASE 6: DATABASE COMPATIBILITY CHECK")
            print("  Skipped (--skip-compat-check flag used)")
        
        # Phase 7: Load to database (optional)
        database_loaded = False
        if args.load_db:
            print("\nPHASE 7: DATABASE LOADING")
            
            # Proceed with database loading
            try:
                from db.database_loader import DatabaseLoader
                
                loader = DatabaseLoader(args.load_db, args.conn)
                
                # Test connection
                if not loader.test_connection():
                    print("  Failed to connect to database")
                    print("  Check your connection string and database availability")
                    print("  Continuing with summary of completed phases...")
                else:
                    print(f"  Connected to {args.load_db}")
                    
                    # Create tables if needed
                    if args.recreate_tables:
                        print("  Creating tables...")
                        loader.create_tables_from_csv(
                            generator.output_dir, 
                            generator.generation_order,
                            drop_existing=True
                        )
                    
                    # Load data with specified mode
                    tables_loaded = loader.load_csv_files(
                        generator.output_dir, 
                        generator.generation_order, 
                        mode=args.db_mode
                    )
                    print(f"  Database loading complete: {len(tables_loaded)} tables loaded")
                    database_loaded = True
                
            except ImportError as e:
                print(f"  Error: {e}")
                print("  Install required packages:")
                if args.load_db == 'postgresql':
                    print("    pip install psycopg2-binary")
                else:
                    print("    pip install oracledb")
                print("  Continuing with summary of completed phases...")
            except Exception as e:
                print(f"  Error loading to database: {e}")
                print("  Continuing with summary of completed phases...")
        
        # Final Summary - Always show this regardless of database loading success
        print("\n" + "="*60)
        print("GENERATION SUMMARY")
        print("="*60)
        print(f"Model: {args.model}")
        print(f"Input: {seed_count} seed records")
        print(f"Output: {generated_count:,} synthetic records")
        print(f"Data integrity: {'PASSED' if is_valid else 'FAILED'}")
        print(f"Output location: {output_path}")
        
        # Show phases completed
        print(f"\nPhases Completed:")
        print(f"  [+] Phase 1: Loading Data")
        print(f"  [+] Phase 2: Training Models")
        print(f"  [+] Phase 3: Generating Synthetic Data")
        print(f"  [+] Phase 4: Validating Integrity")
        print(f"  [+] Phase 5: Saving Output")
        
        if args.load_db:
            if not args.skip_compat_check:
                print(f"  [+] Phase 6: Compatibility Check")
            else:
                print(f"  [-] Phase 6: Compatibility Check (skipped)")
            
            if database_loaded:
                print(f"  [+] Phase 7: Database Loading")
            else:
                print(f"  [-] Phase 7: Database Loading (failed)")
        
        # Database details section (if attempted)
        if args.load_db:
            print(f"\nDatabase Details:")
            print(f"  Target: {args.load_db}")
            print(f"  Mode: {args.db_mode}")
            print(f"  Tables recreated: {'Yes' if args.recreate_tables else 'No'}")
            print(f"  Compatibility check: {'Skipped' if args.skip_compat_check else 'Passed'}")
            print(f"  Status: {'Successfully loaded' if database_loaded else 'Failed to connect'}")
            
            if not database_loaded:
                print(f"\n  Note: Synthetic data was successfully generated and saved to:")
                print(f"        {output_path}")
                print(f"        You can retry database loading later using the generated CSV files.")
        
        # Table breakdown
        print("\nTable breakdown:")
        for table_name, df in generator.synthetic_data.items():
            csv_file = generator.output_dir / f"{table_name}.csv"
            file_status = "[+]" if csv_file.exists() else "[-]"
            print(f"  {file_status} {table_name}: {len(df):,} rows -> {table_name}.csv")
        
        print("\nGeneration complete.")
        print("\n" + "="*60 + "\n")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print(f"\nExpected files in {args.models_dir}/ directory:")
        print(f"  {args.model}_schema.json")
        print(f"  {args.model}_seed_data.json")
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        return
    except Exception as e:
        print(f"Error: {str(e)}")
        print("\nIf you need more detailed error information, please contact support.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        print("Please check your input parameters and try again.")
        print("If the problem persists, contact support.")
