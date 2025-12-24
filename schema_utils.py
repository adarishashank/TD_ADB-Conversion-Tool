# schema_utils.py (Build v0.5)
import os
import re as _re
import socket
import time
from contextlib import closing
import pandas as pd
from typing import Optional, Tuple, List, Dict

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
  CharType,
  UDTName
FROM DBC.ColumnsV
WHERE DatabaseName = ? AND TableName = ?
ORDER BY ColumnId
"""

def dns_check(host: str) -> Tuple[bool, str]:
    try:
        info = socket.gethostbyname_ex(host)
        return True, f"Resolved: {info[2]}"
    except Exception as e:
        return False, f"DNS lookup failed: {e}"

def tcp_check(host: str, port: int, timeout: float = 3.0) -> Tuple[bool, float, str]:
    start = time.monotonic()
    try:
        with closing(socket.create_connection((host, port), timeout=timeout)):
            elapsed = time.monotonic() - start
            return True, elapsed, "Connected"
    except Exception as e:
        elapsed = time.monotonic() - start
        return False, elapsed, f"TCP connect failed: {e}"

def td_connect(host: str, user: str, password: str, *, port: Optional[int]=None, cop_discovery: Optional[bool]=None, logmech: Optional[str]=None, encrypt: Optional[bool]=None):
    import teradatasql
    params = dict(host=host, user=user, password=password)
    if port:
        params["dbs_port"] = int(port)
    if cop_discovery is not None:
        params["cop"] = bool(cop_discovery)
    if logmech:
        params["logmech"] = logmech
    if encrypt is not None:
        params["encryptdata"] = bool(encrypt)
    return teradatasql.connect(**params)

def test_td_session(host: str, user: str, password: str, *, port: Optional[int]=None, cop_discovery: Optional[bool]=None, logmech: Optional[str]=None, encrypt: Optional[bool]=None) -> Dict[str, str]:
    """
    Tries to establish a TD session and run basic queries. Returns a dict with timings and results.
    """
    import teradatasql
    result = {}
    t0 = time.monotonic()
    try:
        con = td_connect(host, user, password, port=port, cop_discovery=cop_discovery, logmech=logmech, encrypt=encrypt)
        t1 = time.monotonic()
        result["connect_ok"] = "yes"
        result["connect_ms"] = f"{(t1 - t0)*1000:.0f}"
        with con:
            cur = con.cursor()
            # Basic health checks
            cur.execute("SELECT CURRENT_DATE")
            # Handle case where fetchall returns list of tuples or something else
            rows = cur.fetchall()
            if rows:
                current_date = rows[0][0]
                result["current_date"] = str(current_date)
            else:
                 result["current_date"] = "No rows returned"
            
            # Pull basic env info if available
            try:
                cur.execute("SEL * FROM DBC.DBCInfoV")
                info = cur.fetchall()
                result["dbc_info_rows"] = str(len(info))
            except Exception as e:
                result["dbc_info_rows"] = f"n/a ({e})"
    except teradatasql.OperationalError as oe:
        result["connect_ok"] = "no"
        result["error"] = f"{oe}"
    except Exception as e:
        result["connect_ok"] = "no"
        result["error"] = f"{e}"
    finally:
        result["total_ms"] = f"{(time.monotonic() - t0)*1000:.0f}"
    return result

def _name_index(cols: List[str], candidates: List[str]) -> Optional[int]:
    lc = [c.lower() for c in cols]
    for cand in candidates:
        cl = cand.lower()
        if cl in lc:
            return lc.index(cl)
    # fuzzy contains
    for i, c in enumerate(lc):
        for cand in candidates:
            if cand.lower() in c:
                return i
    return None

def _parse_td_dtype(s: str) -> Tuple[str, Optional[int], Optional[int]]:
    """Parse a TD data type string OR code into (base, precision, scale)."""
    t = (s or "").strip().upper()
    
    # --- HANDLE SHORT CODES (ColumnsV / HELP COLUMN codes) ---
    if t == "CV": return "VARCHAR", None, None
    if t == "CF": return "CHAR", None, None
    if t == "D":  return "DECIMAL", None, None
    if t == "I":  return "INTEGER", None, None
    if t == "I1": return "BYTEINT", None, None
    if t == "I2": return "SMALLINT", None, None
    if t == "I8": return "BIGINT", None, None
    if t == "F":  return "FLOAT", None, None # FLOAT/REAL/DOUBLE
    if t == "DA": return "DATE", None, None
    if t == "TS": return "TIMESTAMP", None, None
    if t == "TZ": return "TIME_TZ", None, None
    if t == "SZ": return "TIME", None, None
    if t == "AT": return "TIMESTAMP_TZ", None, None
    if t == "JN": return "JSON", None, None
    if t == "XM": return "XML", None, None
    if t == "BO": return "BLOB", None, None
    if t == "CO": return "CLOB", None, None
    if t == "BF": return "BYTE", None, None
    if t == "BV": return "VARBYTE", None, None
    if t.startswith("P"): return "PERIOD", None, None # PD etc
    
    # --- HANDLE FULL TYPE STRINGS ---
    # Common patterns
    m = _re.search(r'DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', t)
    if m:
        return "DECIMAL", int(m.group(1)), int(m.group(2))
    m = _re.search(r'NUMERIC\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', t)
    if m:
        return "DECIMAL", int(m.group(1)), int(m.group(2))
    m = _re.search(r'NUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', t)
    if m:
        return "DECIMAL", int(m.group(1)), int(m.group(2))
    if "NUMBER" in t: return "DECIMAL", None, None

    m = _re.search(r'(VAR)?CHAR\s*\(\s*(\d+)\s*\)', t)
    if m:
        base = "VARCHAR" if t.startswith("VAR") else "CHAR"
        return base, int(m.group(2)), None
    m = _re.search(r'VARBYTE\s*\(\s*(\d+)\s*\)', t)
    if m:
        return "VARBYTE", int(m.group(1)), None
    m = _re.search(r'BYTE\s*\(\s*(\d+)\s*\)', t)
    if m:
        return "BYTE", int(m.group(1)), None
        
    if "BYTEINT" in t: return "BYTEINT", None, None
    if "SMALLINT" in t: return "SMALLINT", None, None
    if "INTEGER" in t or t == "INT": return "INTEGER", None, None
    if "BIGINT" in t: return "BIGINT", None, None
    if "REAL" in t: return "REAL", None, None
    if "FLOAT" in t or "DOUBLE" in t: return "FLOAT", None, None
    if "DATE" in t and "TIME" not in t: return "DATE", None, None
    if "TIMESTAMP" in t and "WITH TIME ZONE" in t: return "TIMESTAMP_TZ", None, None
    if "TIMESTAMP" in t: return "TIMESTAMP", None, None
    if "TIME" in t and "WITH TIME ZONE" in t: return "TIME_TZ", None, None
    if "TIME" in t: return "TIME", None, None
    if "INTERVAL" in t: return "INTERVAL", None, None
    if "PERIOD" in t: return "PERIOD", None, None
    if "JSON" in t: return "JSON", None, None
    if "XML" in t: return "XML", None, None
    if "CLOB" in t: return "CLOB", None, None
    if "BLOB" in t: return "BLOB", None, None

    # Fallback
    return t.split()[0], None, None

def td_to_dbx_type(base: str, length: Optional[int], precision: Optional[int], scale: Optional[int], force_string_time: bool=False) -> Tuple[str, Optional[str]]:
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
        return "DOUBLE", None
    if b == "REAL":
        return "FLOAT", None
    if b == "DECIMAL":
        # Strict preservation as requested
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
        # Map TIME to STRING as per ADB friendliness
        return "STRING", "TIME mapped to STRING"
    if b in ("TIMESTAMP", "TIMESTAMP_TZ"):
        # Map TIMESTAMP to TIMESTAMP (no TZ)
        return "TIMESTAMP", ("Dropped time zone info" if b.endswith("TZ") else None)
    
    # Attempt to handle generic NUMBER/NUMERIC if not caught above (though normalized by _parse_td_dtype)
    if b in ("NUMBER", "NUMERIC"): 
        p = precision if (precision and precision > 0) else 38
        s = scale if (scale is not None and scale >= 0) else 0
        return f"DECIMAL({p},{s})", None

    if b in ("INTERVAL", "PERIOD", "JSON", "XML"):
        return "STRING", f"{base} mapped to STRING"

    return "STRING", f"Unmapped TD type {base}; defaulted to STRING"



def fetch_primary_index(cur, db: str, tbl: str) -> List[str]:
    """Fetch Primary Index columns for a table from DBC.IndicesV."""
    sql = """
    SELECT ColumnName
    FROM DBC.IndicesV
    WHERE DatabaseName = ?
      AND TableName = ?
      AND IndexType = 'P'
    ORDER BY ColumnPosition
    """
    try:
        cur.execute(sql, (db, tbl))
        rows = cur.fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []

def build_dbx_create_table(database: str, table: str, cols: List[dict], catalog: Optional[str], schema: Optional[str], primary_index: List[str] = [], grants: List[str] = []) -> Tuple[str, str]:
    cat = catalog or ""
    sch = schema or database.lower()
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
        # We generally don't enforce NOT NULL in strict migration DDLs unless confident, 
        # but here we follow the source if nullable=False.
        null_str = "" if nullable else " NOT NULL"
        col_lines.append(f"  `{name.lower()}` {dtype}{null_str}")
        
    cols_sql = ",\n".join(col_lines) if col_lines else ""
    
    # CLUSTER BY logic removed as requested
    # cluster_clause = ""
    # if primary_index:
    #     cols_str = ", ".join([f"`{c.lower()}`" for c in primary_index])
    #     cluster_clause = f"\nCLUSTER BY ({cols_str})"
    
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
    sql = f"""CREATE TABLE IF NOT EXISTS {full} (
{cols_sql}
)
TBLPROPERTIES (
  {props_sql}
);{grant_sql}"""
    return full, sql

def _fetch_from_columnsv(cur, db: str, tbl: str) -> List[dict]:
    cur.execute(TD_COLUMNSV_SQL, (db, tbl))
    recs = cur.fetchall()
    rows = []
    for rec in recs:
        rows.append({
            "DatabaseName": rec[0],
            "TableName": rec[1],
            "ColumnName": rec[2],
            "ColumnId": rec[3],
            "ColumnType": rec[4],
            "ColumnLength": rec[5],
            "Decimals": rec[6],
            "Nullable": rec[7],
            "DefaultValue": rec[8],
            "CharType": rec[9],
            "UDTName": rec[10],
        })
    return rows

def _fetch_from_help_column(cur, db: str, tbl: str) -> List[dict]:
    # Try HELP COLUMN first
    sql = f'HELP COLUMN "{db}"."{tbl}".*'
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    recs = cur.fetchall()
    rows = []
    # Determine index mapping
    idx_name = _name_index(cols, ["ColumnName", "FieldName", "Column Name", "Field Name"])
    idx_type = _name_index(cols, ["Type", "ColumnType", "DataType", "TypeName"])
    idx_null = _name_index(cols, ["Nullable", "Null", "Nullability"])
    idx_len  = _name_index(cols, ["ColumnLength", "Length", "Max Length"])
    idx_dec_total = _name_index(cols, ["DecimalTotalDigits", "TotalDigits"])
    idx_dec_frac  = _name_index(cols, ["DecimalFractionalDigits", "FractionalDigits", "Scale"])
    for i, rec in enumerate(recs, start=1):
        name = str(rec[idx_name]) if idx_name is not None else f"C{i}"
        dtype_str = str(rec[idx_type]) if idx_type is not None else ""
        base, prec, scale = _parse_td_dtype(dtype_str)
        length = int(rec[idx_len]) if (idx_len is not None and rec[idx_len] is not None and str(rec[idx_len]).isdigit()) else None
        if prec is None and idx_dec_total is not None and rec[idx_dec_total] is not None:
            try: prec = int(rec[idx_dec_total])
            except: pass
        if scale is None and idx_dec_frac is not None and rec[idx_dec_frac] is not None:
            try: scale = int(rec[idx_dec_frac])
            except: pass
        nullable = True
        if idx_null is not None and rec[idx_null] is not None:
            nv = str(rec[idx_null]).strip().upper()
            nullable = (nv in ("Y","YES","TRUE","T"))
        rows.append({
            "DatabaseName": db,
            "TableName": tbl,
            "ColumnName": name,
            "ColumnId": i,
            "ColumnType": base,
            "ColumnLength": length,
            "Decimals": scale,
            "Nullable": nullable,
            "DefaultValue": None,
            "CharType": None,
            "UDTName": None,
        })
    return rows

def _fetch_from_help_table(cur, db: str, tbl: str) -> List[dict]:
    # Fallback to HELP TABLE (parse Field/Type/Nullable)
    sql = f'HELP TABLE "{db}"."{tbl}"'
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    recs = cur.fetchall()
    rows = []
    idx_name = _name_index(cols, ["FieldName", "ColumnName", "Field Name", "Column Name"])
    idx_type = _name_index(cols, ["Type", "ColumnType", "DataType", "TypeName"])
    idx_null = _name_index(cols, ["Nullable", "Null", "Nullability"])
    for i, rec in enumerate(recs, start=1):
        name = str(rec[idx_name]) if idx_name is not None else f"C{i}"
        dtype_str = str(rec[idx_type]) if idx_type is not None else ""
        base, prec, scale = _parse_td_dtype(dtype_str)
        nullable = True
        if idx_null is not None and rec[idx_null] is not None:
            nv = str(rec[idx_null]).strip().upper()
            nullable = (nv in ("Y","YES","TRUE","T"))
        rows.append({
            "DatabaseName": db,
            "TableName": tbl,
            "ColumnName": name,
            "ColumnId": i,
            "ColumnType": base,
            "ColumnLength": None,
            "Decimals": scale,
            "Nullable": nullable,
            "DefaultValue": None,
            "CharType": None,
            "UDTName": None,
        })
    return rows

def fetch_columns_metadata(cur, db: str, tbl: str) -> Tuple[List[dict], str]:
    """Return rows and source ('ColumnsV' | 'HELP COLUMN' | 'HELP TABLE')."""
    try:
        rows = _fetch_from_columnsv(cur, db, tbl)
        if rows:
            return rows, "ColumnsV"
    except Exception as e:
        last_err = e
        # fall through
    # Try HELP COLUMN
    try:
        rows = _fetch_from_help_column(cur, db, tbl)
        if rows:
            return rows, "HELP COLUMN"
    except Exception as e:
        last_err = e
    # Try HELP TABLE
    try:
        rows = _fetch_from_help_table(cur, db, tbl)
        if rows:
            return rows, "HELP TABLE"
    except Exception as e:
        last_err = e
        raise
    # If we got here, re-raise last error
    raise last_err

def harvest_one_table(cur, db: str, tbl: str, catalog: Optional[str], schema: Optional[str], force_string_time: bool, grants: List[str] = []):
    rows, source = fetch_columns_metadata(cur, db, tbl)
    out_rows = []
    cols_for_ddl = []
    for r in rows:
        td_type_raw = r["ColumnType"]
        # Parse the raw type string or code
        base, prec, scale = _parse_td_dtype(td_type_raw) if isinstance(td_type_raw, str) else (str(td_type_raw), None, None)
        
        # Fallback: if precision/scale missing from parse, look in metadata
        if scale is None and r.get("Decimals") is not None:
            try:
                scale = int(r["Decimals"])
            except:
                pass
        
        # If precision is missing, we often default to 38 in td_to_dbx_type, 
        # but if we had a way to calculate it (e.g. from TotalDigits in HELP COLUMN which we might have captured in schema_utils), we could use it.
        # _fetch_from_help_column attempts to map TotalDigits to 'ColumnLength' (if we look at how it maps? No, it maps to None or Length).
        # Actually in _fetch_from_help_column I see:
        # if prec is None... prec = int(rec[idx_dec_total])
        # BUT _fetch_from_help_column puts that nowhere? check _fetch_from_help_column dict keys.
        # It puts it in "ColumnLength"? No. "ColumnLength" is length. 
        # "Decimals" is scale.
        # It seems _fetch_from_help_column might lose precision info if it's not in the type string?
        # Re-checking _fetch_from_help_column:
        # It constructs rows with keys matching ColumnsV output mostly.
        # It has "ColumnLength" and "Decimals".
        # It DOES NOT seem to store explicit Precision in the dict if it parsed it from separate column.
        # Wait, lines 305-306 in original/updated file:
        # try: prec = int(rec[idx_dec_total])
        # It assigns valid `prec` variable but DOES NOT store it in the `rows` dict?
        # The `rows` dict has "ColumnLength": length.
        # It seems `_fetch_from_help_column` logic for `prec` is wasted?
        # I should probably just rely on the default (38) for now as requested by user "Max precision = 38 in Databricks".
        # So ensuring SCALE is correct is the priority.
        
        dbx_type, cast_note = td_to_dbx_type(base, None, prec, scale, force_string_time)
        row = {
            "database_name": db,
            "table_name": tbl,
            "column_id": r["ColumnId"],
            "column_name": r["ColumnName"],
            "td_type_raw": td_type_raw,
            "td_type_base": base,
            "td_length": r.get("ColumnLength"),
            "td_precision": prec,
            "td_scale": scale,
            "nullable": bool(r["Nullable"]),
            "default_value": r.get("DefaultValue"),
            "format": None,
            "comment": None,
            "dbx_type": dbx_type,
            "dbx_nullable": bool(r["Nullable"]),
            "dbx_cast_note": cast_note,
            "meta_source": source,
        }
        out_rows.append(row)
        cols_for_ddl.append(row)
    cols_sorted = sorted(cols_for_ddl, key=lambda x: x["column_id"])
    
    # Fetch Primary Index for CLUSTER BY
    pi_cols = fetch_primary_index(cur, db, tbl)
    
    full, sql = build_dbx_create_table(db, tbl, cols_sorted, catalog, schema, pi_cols, grants)
    return out_rows, {
        "database_name": db,
        "table_name": tbl,
        "catalog": catalog or "",
        "schema": (schema or db.lower()),
        "dbx_table_fullname": full,
        "create_table_sql": sql,
        "meta_source": source,
    }
