# app.py
import io
import pandas as pd
import streamlit as st
from schema_utils import td_connect, harvest_one_table, dns_check, tcp_check, test_td_session, TD_COLUMNSV_SQL

st.set_page_config(page_title="TD → Databricks Schema Harvester", layout="wide")

# --- Authentication removed ---
# The previous demo authentication has been removed to allow direct access.
if "auth" not in st.session_state:
    st.session_state.auth = True

# --- Header ---
st.title("Teradata → Databricks Schema Harvester")
st.caption("Upload your table list, enter Teradata connection details, and generate schema + Databricks DDL.")

# --- Inputs ---
with st.expander("Teradata Connection", expanded=True):
    st.caption("If hostname DNS fails, try: connect via VPN, use FQDN, or enter IP / comma-separated COP list.")
    col1, col2, col3 = st.columns(3)
    with col1:
        td_host = st.text_input("Teradata Host", placeholder="td-hostname.yourdomain.com")
    with col2:
        td_user = st.text_input("Teradata User")
    with col3:
        td_pass = st.text_input("Teradata Password", type="password")

with st.expander("Advanced (Ports, COP, Security)", expanded=False):
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        td_port = st.number_input("DBS Port", value=0, help="Usually 1025 or 1026; 0 = driver default")
    with c2:
        cop_discovery = st.selectbox("COP Discovery", ["Default", "Disable", "Enable"], index=0)
    with c3:
        logmech = st.text_input("LOGMECH (optional)", placeholder="e.g., LDAP, TD2")
    with c4:
        encrypt = st.checkbox("Encrypt Data", value=True)

    st.caption("Host can be a comma-separated list: host1,host2,10.1.2.3")

with st.expander("Databricks Target", expanded=True):
    c1, c2, c3 = st.columns(3)
    with c1:
        # Environment selection
        env = st.selectbox("Target Environment", ["Dev", "QA", "Prod"], index=0)
        # Map environment to catalog
        env_map = {
            "Dev": "uc_dev_apac_mdip_01",
            "QA": "uc_qa_apac_mdip_01",
            "Prod": "uc_prod_apac_mdip_01"
        }
        catalog = env_map.get(env, "uc_dev_apac_mdip_01")
        st.info(f"Catalog: {catalog}")
    with c2:
        schema = st.text_input("Schema / Database (optional)", value="")
    with c3:
        grant_input = st.text_input("Grant Access To (Comma-separated Email/Groups)", placeholder="user@example.com, group_name")

force_time_string = st.checkbox("Map TIME types to STRING (recommended)", value=True)

st.markdown("---")

tab_in, tab_out = st.tabs(["① Input", "② Output"])

with tab_in:
    st.subheader("Upload Input Excel (from template)")
    up = st.file_uploader("Excel with sheet: input_tables (columns: database_name, table_name)", type=["xlsx"])
    st.caption("Tip: Use the template provided. Make sure the sheet is named **input_tables**.")
with st.expander("Debug: Current ColumnsV SQL"):
    st.code(TD_COLUMNSV_SQL, language="sql")

    st.subheader("Output Mode")
    out_mode = st.radio("Choose output destination", ["Download new Excel", "Update an uploaded Excel (add/replace sheets)"], index=0)

    existing_excel = None
    if out_mode == "Update an uploaded Excel (add/replace sheets)":
        existing_excel = st.file_uploader("Upload existing Excel to update (we will add/replace sheets schema_output & ddl_output)", type=["xlsx"], key="existing")

run = st.button("Run Harvest", type="primary", use_container_width=True, disabled=up is None or not td_host or not td_user or not td_pass)

test_only = st.button("Test Connection Only", use_container_width=True, disabled=not td_host or not td_user or not td_pass)

with tab_out:
    st.subheader("Results")
    result_placeholder = st.empty()
    diag_placeholder = st.container()

if test_only:
    try:
        with st.spinner("Running diagnostics..."):
            # Parse first host for checks
            first_host = td_host.split(",")[0].strip()
            ok_dns, dns_msg = dns_check(first_host)
            st.info(f"DNS check for {first_host}: {dns_msg}")

            # Try TCP to ports
            ports_to_try = []
            if "td_port" in locals() and td_port:
                ports_to_try.append(int(td_port))
            else:
                ports_to_try += [1025, 1026]  # common defaults

            rows = []
            for p in ports_to_try:
                ok_tcp, elapsed, note = tcp_check(first_host, p, timeout=3.0)
                rows.append({"host": first_host, "port": p, "reachable": ok_tcp, "ms": int(elapsed*1000), "note": note})
            st.dataframe(rows, use_container_width=True)

            # Try a minimal TD session
            cop_val = None if cop_discovery == "Default" else (False if cop_discovery == "Disable" else True)
            port_val = int(td_port) if td_port else None
            enc_val = True if encrypt else False
            result = test_td_session(td_host, td_user, td_pass, port=port_val, cop_discovery=cop_val, logmech=(logmech or None), encrypt=enc_val)
            st.write("**Session Test Result**", result)
    except Exception as e:
        st.error(f"Diagnostics failed: {e}")

with tab_out:
    st.subheader("Results")
    result_placeholder = st.empty()

if run:
    try:
        # Read input tables
        tables = pd.read_excel(up, sheet_name="input_tables")
        if not set(["database_name","table_name"]).issubset(tables.columns.str.lower()):
            st.error("The input sheet must have 'database_name' and 'table_name' columns.")
            st.stop()
        # Normalize column names (case-insensitive)
        tables.columns = tables.columns.str.lower()

        # Connect
        with st.spinner("Connecting to Teradata..."):
            # DNS preflight
            if td_host:
                # Only check the first token (in case of comma-separated host list)
                first_host = td_host.split(",")[0].strip()
                ok, msg = dns_check(first_host)
                st.info(f"DNS check for {first_host}: {msg}")
            # Build args
            port_val = int(td_port) if td_port else None
            cop_val = None if cop_discovery == "Default" else (False if cop_discovery == "Disable" else True)
            enc_val = True if encrypt else False
            con = td_connect(td_host, td_user, td_pass, port=port_val, cop_discovery=cop_val, logmech=(logmech or None), encrypt=enc_val)

        out_rows = []
        ddl_rows = []

        with con:
            cur = con.cursor()
            progress = st.progress(0, text="Fetching metadata...")
            total = len(tables)
            for i, r in tables.iterrows():
                db = str(r["database_name"]).strip()
                tbl = str(r["table_name"]).strip()
                try:
                    # Parse grants
                    grant_list = [x.strip() for x in (grant_input or "").split(",") if x.strip()]
                    rows, ddl = harvest_one_table(cur, db, tbl, catalog or None, schema or None, force_time_string, grants=grant_list)
                    out_rows.extend(rows)
                    ddl_rows.append(ddl)
                except Exception as e:
                    ddl_rows.append({
                        "database_name": db, "table_name": tbl, "catalog": catalog or "",
                        "schema": (schema or db.lower()), "dbx_table_fullname": "",
                        "create_table_sql": f"-- ERROR fetching schema for {db}.{tbl}: {e}"
                    })
                progress.progress(int((i+1)/total*100), text=f"Processed {i+1}/{total}: {db}.{tbl}")
            progress.empty()

        schema_df = pd.DataFrame(out_rows)
        ddl_df = pd.DataFrame(ddl_rows)
        
        # Combine all DDLs into one script
        all_sql = "\n\n".join([str(r.get("create_table_sql", "")) for r in ddl_rows])
        all_sql_bytes = all_sql.encode("utf-8")

        # Prepare output
        if out_mode == "Download new Excel":
            out_buf = io.BytesIO()
            with pd.ExcelWriter(out_buf, engine="openpyxl") as xw:
                schema_df.to_excel(xw, sheet_name="schema_output", index=False)
                ddl_df.to_excel(xw, sheet_name="ddl_output", index=False)
            st.success("Harvest complete. Download your file below.")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("Download Excel", data=out_buf.getvalue(), file_name="td_schema_mapped.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with c2:
                st.download_button("Download .sql Script", data=all_sql_bytes, file_name="all_schemas.sql", mime="text/plain")
            result_placeholder.dataframe(schema_df.head(100))
        else:
            # Update user-uploaded Excel
            if existing_excel is None:
                st.error("You selected 'Update an uploaded Excel' but did not upload one. Please upload the existing Excel or switch to 'Download new Excel'.")
                st.stop()
            in_bytes = existing_excel.read()
            bio = io.BytesIO(in_bytes)
            out_buf = io.BytesIO()
            with pd.ExcelWriter(out_buf, engine="openpyxl", mode="w") as xw:
                # Load original
                orig = pd.ExcelFile(bio)
                # Copy all original sheets except the two we will replace
                for sheet in orig.sheet_names:
                    if sheet in ("schema_output","ddl_output"):
                        continue
                    df = pd.read_excel(orig, sheet_name=sheet)
                    df.to_excel(xw, sheet_name=sheet, index=False)
                # Write/replace our outputs
                schema_df.to_excel(xw, sheet_name="schema_output", index=False)
                ddl_df.to_excel(xw, sheet_name="ddl_output", index=False)
            st.success("Harvest complete. Download your updated Excel below.")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("Download Updated Excel", data=out_buf.getvalue(), file_name="td_schema_updated.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with c2:
                st.download_button("Download .sql Script", data=all_sql_bytes, file_name="all_schemas.sql", mime="text/plain")
            result_placeholder.dataframe(schema_df.head(100))

    except Exception as e:
        st.error(f"Failed: {e}")
        st.exception(e)
