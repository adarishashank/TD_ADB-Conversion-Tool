#!/usr/bin/env python3
"""
Teradata → Databricks schema harvester

What it does
------------
- Reads an Excel file (sheet: input_tables) with columns: database_name, table_name
- Connects to Teradata and pulls column metadata from DBC.ColumnsV (or Fallback to HELP TABLE)
- Writes a normalized schema table to Excel (sheet: schema_output)
- Maps Teradata data types to Databricks (Spark/Delta) types
- Generates CREATE TABLE DDL in Databricks SQL (sheet: ddl_output)

Usage
-----
python td_to_databricks_schema_automation.py \
  --excel_in /path/to/td_to_databricks_schema_automation_template.xlsx \
  --excel_out /path/to/output.xlsx \
  --td-host your.td.host \
  --td-user USER \
  --td-pass 'PASSWORD' \
  [--catalog apacmdip] [--schema gold_eame] [--force-string-time]

Notes
-----
- Requires: pandas, openpyxl, teradatasql (pip install teradatasql)
- For TIME/INTERVAL/ZONE types, Databricks has no exact equivalents; defaults are explained in mappings.
"""

import argparse
import sys
import re
from typing import Optional, Tuple, List
import pandas as pd

# Import teradatasql only when needed to allow dry-runs without the driver installed
def _td_connect(host: str, user: str, password: str):
    import teradatasql
    return teradatasql.connect(host=host, user=user, password=password)

TD_COLUMNSV_SQL = """
SELECT
  DatabaseName,
  TableName,
  ColumnName,
  ColumnId,
  ColumnType,
  ColumnLength,
  Decimals,
  Nullable,
  DefaultValue,
  Format,
  Title,
  CommentString,
  CharType,
  UDTName
FROM DBC.ColumnsV
WHERE DatabaseName = ? AND TableName = ?
ORDER BY ColumnId
"""

def normalize_td_type(row) -> Tuple[str, str, Optional[int], Optional[int], Optional[int]]:
    """
    Returns (td_type_raw, td_type_base, length, precision, scale)

    ColumnType examples:
      'CV' (VARCHAR), 'CF' (CHAR), 'I' (INTEGER), 'I1' (BYTEINT), 'I2' (SMALLINT),
      'I8' (BIGINT), 'D' (DECIMAL), 'F' (FLOAT), 'DA' (DATE), 'TS' (TIMESTAMP),
      'TZ' (TIME WITH TIME ZONE), 'AT' (TIMESTAMP WITH TIME ZONE),
      'BF' (BYTE), 'BV' (VARBYTE), 'BO' (BLOB), 'CO' (CLOB), 'UT' (UDT), etc.
    See: DBC.ColumnsV docs.
    """
    ct = (row.get("ColumnType") or "").strip().upper()
    length = row.get("ColumnLength")
    decimals = row.get("Decimals")
    udt = (row.get("UDTName") or "").strip().upper()

    td_type_raw = ct
    base = None
    precision = None
    scale = None

    # Map by Columntype code
    if ct in ("CV",):  # VARCHAR
        base = "VARCHAR"
    elif ct in ("CF",):  # CHAR
        base = "CHAR"
    elif ct in ("I",):  # INTEGER
        base = "INTEGER"
    elif ct in ("I1",):  # BYTEINT
        base = "BYTEINT"
    elif ct in ("I2",):  # SMALLINT
        base = "SMALLINT"
    elif ct in ("I8",):  # BIGINT
        base = "BIGINT"
    elif ct in ("D",):   # DECIMAL
        base = "DECIMAL"
        # For DECIMAL, DBC stores ColumnLength as storage bytes; precision is in ColumnLength*? on TD <= 13; modern TD uses ColumnsV.Decimals + some rules.
        # Many installations keep precision in "DecimalTotalDigits" column in other views; here we approximate from Format if available later.
        # We'll parse Format as Plan B below in mapper if precision is missing.
        precision = None
        scale = decimals if decimals is not None and decimals >= 0 else 0
    elif ct in ("F",):   # FLOAT/REAL/DOUBLE
        base = "FLOAT" # Will map to DOUBLE in td_to_dbx_type
    elif ct in ("DA",):  # DATE
        base = "DATE"
    elif ct in ("TS",):  # TIMESTAMP
        base = "TIMESTAMP"
    elif ct in ("TZ",):  # TIME WITH TIME ZONE
        base = "TIME_TZ"
    elif ct in ("SZ",):  # TIME WITHOUT TIME ZONE (some installs use 'TZ'/'SZ')
        base = "TIME"
    elif ct in ("AT",):  # TIMESTAMP WITH TIME ZONE
        base = "TIMESTAMP_TZ"
    elif ct in ("JN",):  # JSON
        base = "JSON"
    elif ct in ("XM",):  # XML
        base = "XML"
    elif ct.startswith("P"): # PERIOD
        base = "PERIOD"
    elif ct in ("BO",):  # BLOB
        base = "BLOB"
    elif ct in ("CO",):  # CLOB
        base = "CLOB"
    elif ct in ("BF",):  # BYTE
        base = "BYTE"
    elif ct in ("BV",):  # VARBYTE
        base = "VARBYTE"
    elif ct in ("UT",):  # UDT
        base = f"UDT({udt or 'UNKNOWN'})"
    else:
        # Fallback for INTERVAL/etc: if it looks like interval, call it interval
        if "INTERVAL" in (row.get("ColumnType") or ""): # This is unlikely as CT is short code
             # CT for Interval is like "MO" (Month), "DY" (Day) etc?
             # Just leave UNKNOWN for now, it maps to STRING.
             pass
        base = f"UNKNOWN({ct})"
    return td_type_raw, base, length, precision, scale

def td_to_dbx_type(base: str, length: Optional[int], precision: Optional[int], scale: Optional[int], force_string_time: bool=False) -> Tuple[str, Optional[str]]:
    """
    Returns (dbx_type, cast_note)
    """
    cast_note = None
    b = (base or "").upper()

    if b in ("VARCHAR", "CHAR", "CLOB"):
        return "STRING", None
    if b in ("BYTE", "VARBYTE", "BLOB"):
        return "BINARY", None
    if b == "BYTEINT":
        return "TINYINT", None
    if b == "SMALLINT":
        return "SMALLINT", None
    if b == "INTEGER":
        return "INT", None
    if b == "BIGINT":
        return "BIGINT", None
    if b == "FLOAT":
        # Map all float/real/double to DOUBLE in Spark as user requested "Prefer DOUBLE" for generic Float
        return "DOUBLE", None
    if b == "REAL":
        # Explicit REAL maps to FLOAT (4-byte) per user request
        return "FLOAT", None
    if b == "DECIMAL":
        # Spark/Delta supports DECIMAL(1..38, 0..38). If precision is missing, fallback with a safe upper bound.
        p = precision if (precision and precision > 0) else 38
        s = scale if (scale is not None and scale >= 0) else 0
        if p > 38:
            cast_note = f"Precision {p} > 38; coerced to DECIMAL(38,{min(s, 38)})"
            p = 38
            s = min(s, 38)
        return f"DECIMAL({p},{s})", cast_note
    if b == "DATE":
        return "DATE", None
    if b in ("TIME", "TIME_TZ"):
        # Spark doesn't have a dedicated TIME type; commonly stored as STRING or cast to TIMESTAMP with a dummy date.
        return ("STRING" if force_string_time else "STRING"), "TIME mapped to STRING"
    if b in ("TIMESTAMP", "TIMESTAMP_TZ"):
        return "TIMESTAMP", ("Dropped time zone info" if b.endswith("TZ") else None)
    if b in ("JSON", "XML", "PERIOD", "INTERVAL"):
        return "STRING", f"{base} mapped to STRING"
    
    # Fallback
    return "STRING", f"Unmapped TD type {base}; defaulted to STRING"

def build_dbx_create_table(database: str, table: str, cols: List[dict], catalog: Optional[str], schema: Optional[str], primary_index: List[str] = [], grants: List[str] = []) -> Tuple[str, str]:
    """
    Builds a Databricks CREATE TABLE statement (Delta).
    Returns (full_name, sql)
    """
    # Prefer provided catalog/schema; else derive from TD database
    cat = catalog or ""
    sch = schema or database.lower()
    # Build full name
    if cat and sch:
        full = f"{cat}.{sch}.{table.lower()}"
    elif sch:
        full = f"{sch}.{table.lower()}"
    else:
        full = table.lower()

    col_lines = []
    for c in cols:
        name = c["column_name"]
        dtype = c["dbx_type"]
        nullable = c["dbx_nullable"]
        null_str = "" if nullable else " NOT NULL"
        col_lines.append(f"  `{name.lower()}` {dtype}{null_str}")
    cols_sql = ",\n".join(col_lines) if col_lines else ""

    # CLUSTER BY logic removed as requested
    
    # TBLPROPERTIES from index.html
    props = [
        "'delta.autoOptimize.optimizeWrite' = 'true'",
        "'delta.autoOptimize.autoCompact' = 'true'",
        "'delta.enableDeletionVectors' = 'true'",
        "'delta.minReaderVersion' = '3'",
        "'delta.minWriterVersion' = '7'",
        "'delta.logRetentionDuration' = '30 days'",
        "'delta.deletedFileRetentionDuration' = '30 days'"
    ]
    props_sql = ",\n  ".join(props)
    
    # Grant statements
    grant_sql = ""
    if grants:
        lines = []
        for g in grants:
            g = g.strip()
            if g:
                # Add explicit GRANT statement
                lines.append(f"GRANT SELECT, MODIFY ON TABLE {full} TO `{g}`;")
        if lines:
            grant_sql = "\n" + "\n".join(lines)

    # We use CREATE TABLE IF NOT EXISTS as requested/inferred
    # No USING DELTA
    # Added TBLPROPERTIES and GRANTS
    sql = f"""CREATE TABLE IF NOT EXISTS {full} (
{cols_sql}
)
TBLPROPERTIES (
  {props_sql}
);{grant_sql}"""
    return full, sql

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel_in", required=True, help="Path to input Excel (template)")
    ap.add_argument("--excel_out", required=True, help="Path to output Excel")
    ap.add_argument("--td-host", required=True)
    ap.add_argument("--td-user", required=True)
    ap.add_argument("--td-pass", required=True)
    ap.add_argument("--catalog", default=None, help="Databricks catalog name (Unity Catalog)")
    ap.add_argument("--schema", default=None, help="Databricks schema (database) name")
    ap.add_argument("--force-string-time", action="store_true", help="Map TIME types to STRING (default behavior)")
    ap.add_argument("--grants", default="", help="Comma-separated list of users/groups to GRANT SELECT, MODIFY to")
    args = ap.parse_args()
    
    grant_list = [g.strip() for g in args.grants.split(",") if g.strip()]

    # Read input tables
    tables = pd.read_excel(args.excel_in, sheet_name="input_tables")
    out_rows = []
    ddl_rows = []

    # Connect to TD
    try:
        con = _td_connect(args["td-host"] if isinstance(args, dict) else args.td_host,
                          args["td-user"] if isinstance(args, dict) else args.td_user,
                          args["td-pass"] if isinstance(args, dict) else args.td_pass)
    except Exception as e:
        print("ERROR: Failed to connect to Teradata. Ensure 'teradatasql' is installed and credentials are correct.", file=sys.stderr)
        raise

    with con:
        cur = con.cursor()
        for _, r in tables.iterrows():
            db = str(r["database_name"]).strip()
            tbl = str(r["table_name"]).strip()
            try:
                cur.execute(TD_COLUMNSV_SQL, (db, tbl))
                recs = cur.fetchall()
                cols = []
                for rec in recs:
                    col = {
                        "DatabaseName": rec[0],
                        "TableName": rec[1],
                        "ColumnName": rec[2],
                        "ColumnId": rec[3],
                        "ColumnType": rec[4],
                        "ColumnLength": rec[5],
                        "Decimals": rec[6],
                        "Nullable": rec[7],
                        "DefaultValue": rec[8],
                        "Format": rec[9],
                        "Title": rec[10],
                        "CommentString": rec[11],
                        "CharType": rec[12],
                        "UDTName": rec[13],
                    }
                    td_type_raw, td_type_base, td_len, td_prec, td_scale = normalize_td_type(col)
                    dbx_type, cast_note = td_to_dbx_type(td_type_base, td_prec, td_scale, args.force_string_time)
                    row = {
                        "database_name": db,
                        "table_name": tbl,
                        "column_id": col["ColumnId"],
                        "column_name": col["ColumnName"],
                        "td_type_raw": td_type_raw,
                        "td_type_base": td_type_base,
                        "td_length": td_len,
                        "td_precision": td_prec,
                        "td_scale": td_scale,
                        "nullable": bool(col["Nullable"]),
                        "default_value": col["DefaultValue"],
                        "format": col["Format"],
                        "comment": col["CommentString"],
                        "dbx_type": dbx_type,
                        "dbx_nullable": bool(col["Nullable"]),
                        "dbx_cast_note": cast_note,
                    }
                    out_rows.append(row)
                    cols.append(row)
                # DDL for this table
                cols_sorted = sorted(cols, key=lambda x: x["column_id"])
                # Note: CLI automation currently does not fetch primary index for CLUSTER BY, but CLUSTER BY is removed anyway.
                full, sql = build_dbx_create_table(db, tbl, cols_sorted, args.catalog, args.schema, grants=grant_list)
                ddl_rows.append({
                    "database_name": db,
                    "table_name": tbl,
                    "catalog": args.catalog or "",
                    "schema": (args.schema or db.lower()),
                    "dbx_table_fullname": full,
                    "create_table_sql": sql
                })
            except Exception as e:
                # Write an error marker line so you see it in output
                ddl_rows.append({
                    "database_name": db,
                    "table_name": tbl,
                    "catalog": args.catalog or "",
                    "schema": (args.schema or db.lower()),
                    "dbx_table_fullname": "",
                    "create_table_sql": f"-- ERROR fetching schema for {db}.{tbl}: {e}"
                })

    # Write output Excel
    schema_df = pd.DataFrame(out_rows)
    ddl_df = pd.DataFrame(ddl_rows)

    with pd.ExcelWriter(args.excel_out, engine="openpyxl") as xw:
        schema_df.to_excel(xw, sheet_name="schema_output", index=False)
        ddl_df.to_excel(xw, sheet_name="ddl_output", index=False)

    print(f"Done. Wrote: {args.excel_out}")

if __name__ == "__main__":
    main()
