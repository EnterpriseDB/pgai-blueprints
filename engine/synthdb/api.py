"""
SynthDB API — Thin FastAPI wrapper around edb-synthdb.py
Provides REST endpoints for the Synthetic Data tab in dbox UI.

For demonstration purposes only.
"""

import json
import os
import csv
import io
import shutil
import subprocess
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="SynthDB API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MODELS_DIR = Path("/app/models")
UPLOADS_DIR = Path("/app/uploads")
OUTPUT_DIR = Path("/app/output")

UPLOADS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/models")
def list_models():
    """List all available models (built-in + uploaded)."""
    models = []
    for d in [MODELS_DIR, UPLOADS_DIR]:
        if not d.exists():
            continue
        schema_files = sorted(d.glob("*_schema.json"))
        for sf in schema_files:
            name = sf.name.replace("_schema.json", "")
            seed_file = sf.parent / f"{name}_seed_data.json"
            if not seed_file.exists():
                continue
            try:
                with open(sf) as f:
                    schema = json.load(f)
                with open(seed_file) as f:
                    seed = json.load(f)

                tables = schema.get("tables", {})
                relationships = schema.get("relationships", [])
                total_seed = sum(len(rows) for rows in seed.values() if isinstance(rows, list))

                table_info = {}
                for tname, tdef in tables.items():
                    cols = []
                    if tname in seed and len(seed[tname]) > 0:
                        cols = list(seed[tname][0].keys())
                    table_info[tname] = {
                        "columns": cols,
                        "primary_key": tdef.get("primary_key", ""),
                        "foreign_keys": tdef.get("foreign_keys", {}),
                        "seed_rows": len(seed.get(tname, []))
                    }

                models.append({
                    "name": name,
                    "description": schema.get("description", ""),
                    "source": "built-in" if d == MODELS_DIR else "uploaded",
                    "tables": table_info,
                    "relationships": relationships,
                    "total_seed_rows": total_seed
                })
            except Exception as e:
                continue
    return {"models": models}


@app.get("/api/models/{name}/preview")
def preview_model(name: str, limit: int = 25):
    """Preview seed data for a model (first N rows per table)."""
    schema_file, seed_file = _find_model(name)
    if not schema_file:
        raise HTTPException(404, f"Model '{name}' not found")

    with open(seed_file) as f:
        seed = json.load(f)

    preview = {}
    for table, rows in seed.items():
        if isinstance(rows, list):
            preview[table] = rows[:limit]
    return {"model": name, "preview": preview, "limit": limit}


@app.post("/api/models/upload")
async def upload_model(schema: UploadFile = File(...), seed_data: UploadFile = File(...)):
    """Upload a custom model (schema + seed data JSON files)."""
    errors = []

    # Read and validate schema
    try:
        schema_content = await schema.read()
        schema_json = json.loads(schema_content)
    except json.JSONDecodeError as e:
        errors.append(f"Schema file is not valid JSON: {e}")

    # Read and validate seed data
    try:
        seed_content = await seed_data.read()
        seed_json = json.loads(seed_content)
    except json.JSONDecodeError as e:
        errors.append(f"Seed data file is not valid JSON: {e}")

    if errors:
        return JSONResponse({"valid": False, "errors": errors}, status_code=400)

    # Validate schema structure
    validation = _validate_model(schema_json, seed_json)
    if not validation["valid"]:
        return JSONResponse(validation, status_code=400)

    # Save to uploads directory
    name = schema_json.get("model_name", "custom")
    with open(UPLOADS_DIR / f"{name}_schema.json", "w") as f:
        json.dump(schema_json, f, indent=2)
    with open(UPLOADS_DIR / f"{name}_seed_data.json", "w") as f:
        json.dump(seed_json, f, indent=2)

    return {"valid": True, "name": name, "message": f"Model '{name}' uploaded successfully"}


@app.post("/api/models/upload-csv")
async def upload_csv(files: list[UploadFile] = File(...)):
    """Upload CSV files — auto-detect schema from headers and naming patterns."""
    tables = {}
    schema_tables = {}

    for f in files:
        content = await f.read()
        table_name = f.filename.replace(".csv", "").lower()
        reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
        rows = list(reader)
        if not rows:
            continue

        columns = list(rows[0].keys())
        tables[table_name] = rows

        # Auto-detect PK and FKs
        pk = None
        fks = {}
        for col in columns:
            if col == f"{table_name[:-1]}_id" or col == f"{table_name}_id" or col == "id":
                pk = col
            elif col.endswith("_id") and col != pk:
                ref_table = col.replace("_id", "") + "s"
                if ref_table != table_name:
                    fks[col] = f"{ref_table}.{col}"

        if not pk:
            pk = columns[0]

        schema_tables[table_name] = {"primary_key": pk}
        if fks:
            schema_tables[table_name]["foreign_keys"] = fks

    # Build relationships from FKs
    relationships = []
    for tname, tdef in schema_tables.items():
        for fk_col, ref in tdef.get("foreign_keys", {}).items():
            parent = ref.split(".")[0]
            if parent in schema_tables:
                relationships.append({"parent": parent, "child": tname})

    name = "csv_upload"
    schema_json = {
        "model_name": name,
        "description": "Auto-detected from CSV upload",
        "tables": schema_tables,
        "relationships": relationships
    }

    # Save
    with open(UPLOADS_DIR / f"{name}_schema.json", "w") as f:
        json.dump(schema_json, f, indent=2)
    with open(UPLOADS_DIR / f"{name}_seed_data.json", "w") as f:
        json.dump(tables, f, indent=2)

    return {
        "valid": True,
        "name": name,
        "tables": {t: {"columns": list(rows[0].keys()) if rows else [], "rows": len(rows)} for t, rows in tables.items()},
        "schema": schema_json
    }


@app.post("/api/generate")
def generate(
    model: str,
    scale: Optional[float] = None,
    total_rows: Optional[int] = None,
    output: str = "csv",
    db_type: Optional[str] = None,
    db_conn: Optional[str] = None,
    db_mode: str = "append",
    recreate_tables: bool = False
):
    """Generate synthetic data using edb-synthdb."""
    schema_file, seed_file = _find_model(model)
    if not schema_file:
        raise HTTPException(404, f"Model '{model}' not found")

    # Build command
    cmd = ["python", "edb-synthdb.py", "--model", model]
    cmd.extend(["--models-dir", str(schema_file.parent)])
    cmd.extend(["--output-dir", str(OUTPUT_DIR / model)])

    if scale:
        cmd.extend(["--scale", str(min(scale, 100))])
    elif total_rows:
        cmd.extend(["--total-rows", str(min(total_rows, 1000000))])
    else:
        cmd.extend(["--scale", "1"])

    if output == "db" and db_type and db_conn:
        cmd.extend(["--load-db", db_type, "--conn", db_conn, "--db-mode", db_mode])
        if recreate_tables:
            cmd.append("--recreate-tables")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, cwd="/app")
        if result.returncode != 0:
            return JSONResponse({
                "success": False,
                "error": result.stderr or result.stdout
            }, status_code=500)

        # Collect output info
        output_path = OUTPUT_DIR / model
        files = []
        if output_path.exists():
            for f in sorted(output_path.glob("*.csv")):
                rows = sum(1 for _ in open(f)) - 1
                files.append({"table": f.stem, "file": f.name, "rows": rows, "size": f.stat().st_size})

        return {
            "success": True,
            "model": model,
            "output": output,
            "files": files,
            "log": result.stdout[-2000:] if result.stdout else ""
        }
    except subprocess.TimeoutExpired:
        return JSONResponse({"success": False, "error": "Generation timed out after 5 minutes"}, status_code=500)


@app.get("/api/generate/{model}/preview")
def preview_generated(model: str, table: str, limit: int = 25):
    """Preview generated CSV data."""
    csv_path = OUTPUT_DIR / model / f"{table}.csv"
    if not csv_path.exists():
        raise HTTPException(404, f"Generated data for '{model}/{table}' not found")

    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            rows.append(row)
    return {"table": table, "rows": rows, "limit": limit}


@app.get("/api/generate/{model}/download")
def download_generated(model: str):
    """Download generated CSV files as zip."""
    output_path = OUTPUT_DIR / model
    if not output_path.exists():
        raise HTTPException(404, f"No generated data for '{model}'")

    zip_path = OUTPUT_DIR / f"{model}_synthetic_data"
    shutil.make_archive(str(zip_path), "zip", str(output_path))
    return FileResponse(f"{zip_path}.zip", filename=f"{model}_synthetic_data.zip", media_type="application/zip")


@app.post("/api/validate")
async def validate_model(schema: UploadFile = File(...), seed_data: UploadFile = File(...)):
    """Validate model files without saving."""
    try:
        schema_json = json.loads(await schema.read())
        seed_json = json.loads(await seed_data.read())
    except json.JSONDecodeError as e:
        return {"valid": False, "errors": [f"Invalid JSON: {e}"]}

    return _validate_model(schema_json, seed_json)


def _find_model(name):
    """Find model schema and seed files in models or uploads dir."""
    for d in [MODELS_DIR, UPLOADS_DIR]:
        sf = d / f"{name}_schema.json"
        sd = d / f"{name}_seed_data.json"
        if sf.exists() and sd.exists():
            return sf, sd
    return None, None


def _validate_model(schema, seed):
    """Validate schema + seed data structure."""
    errors = []

    if "tables" not in schema:
        errors.append("Schema missing 'tables' field")
        return {"valid": False, "errors": errors}

    tables = schema.get("tables", {})

    # Check each table has primary_key
    for tname, tdef in tables.items():
        if "primary_key" not in tdef:
            errors.append(f"Table '{tname}' missing 'primary_key'")

    # Check FK references
    for tname, tdef in tables.items():
        for fk_col, ref in tdef.get("foreign_keys", {}).items():
            ref_table = ref.split(".")[0] if "." in ref else ref
            if ref_table not in tables:
                errors.append(f"Table '{tname}' FK '{fk_col}' references '{ref_table}' which doesn't exist")

    # Check seed data matches schema
    for tname in tables:
        if tname not in seed:
            errors.append(f"Seed data missing table '{tname}'")
        elif not isinstance(seed[tname], list) or len(seed[tname]) == 0:
            errors.append(f"Seed data for '{tname}' must be a non-empty array")

    # Check FK values reference valid parent IDs
    for tname, tdef in tables.items():
        for fk_col, ref in tdef.get("foreign_keys", {}).items():
            ref_parts = ref.split(".")
            if len(ref_parts) == 2:
                parent_table, parent_col = ref_parts
                if parent_table in seed and tname in seed:
                    parent_ids = {row.get(parent_col) for row in seed[parent_table]}
                    for row in seed[tname]:
                        if row.get(fk_col) not in parent_ids:
                            errors.append(f"Table '{tname}' row has {fk_col}={row.get(fk_col)} not found in {parent_table}.{parent_col}")
                            break

    if errors:
        return {"valid": False, "errors": errors}

    total_seed = sum(len(rows) for rows in seed.values() if isinstance(rows, list))
    return {
        "valid": True,
        "tables": len(tables),
        "relationships": len(schema.get("relationships", [])),
        "total_seed_rows": total_seed
    }
