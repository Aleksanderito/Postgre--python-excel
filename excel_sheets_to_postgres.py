import os
import re
import pandas as pd
from sqlalchemy import create_engine, text

EXCEL_PATH = r"C:\BD_config\before_excel\test.xlsx"
TARGET_SCHEMA = "public"
IF_EXISTS = "replace"  # replace | truncate | append

# ===== Подключение к Postgres =====
def get_engine_from_env():
    host = os.getenv("PGHOST", "10.99.250.187")
    port = os.getenv("PGPORT", "34280")
    db   = os.getenv("PGDATABASE", "maghmilog")
    user = os.getenv("PGUSER", "GoLang")
    pwd  = os.getenv("PGPASSWORD", "1234")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")

# /////////// LOGS /////////////////

def ensure_logs_table(engine, schema, if_exists: str):
    """Создать таблицу logs со строгими типами и sequence для id."""
    table_name = "logs"
    full_table = quote_ident(schema, table_name)
    seq_name = f"{table_name}_id_seq"
    full_seq = quote_ident(schema, seq_name)
    with engine.begin() as conn:
        if if_exists == "replace":
            conn.execute(text(f"DROP TABLE IF EXISTS {full_table} CASCADE;"))
        # sequence для id
        conn.execute(text(f"""
            CREATE SEQUENCE IF NOT EXISTS {full_seq}
                INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;
        """))
        # строгая схема таблицы (как на скриншоте)
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                area          integer,
                type_device   integer,
                device_id     integer,
                type_alarm    integer,
                message       integer,
                date_time_in  text,
                date_time_out text,
                bit_value     text,
                id            integer PRIMARY KEY DEFAULT nextval('{full_seq}')
            );
        """))
        if if_exists == "truncate":
            conn.execute(text(f"TRUNCATE TABLE {full_table};"))
        conn.execute(text(f"ALTER SEQUENCE {full_seq} OWNED BY {full_table}.id;"))

def insert_logs_rows(engine, schema, df: pd.DataFrame, mode: str):
    """Вставка в logs. В replace/truncate назначаем id=1..N по порядку, в append — id через sequence."""
    table_name = "logs"
    full_table = quote_ident(schema, table_name)

    # ожидаем колонки ровно под схему (можем мягко нормализовать имена)
    want = ["area","type_device","device_id","type_alarm","message",
            "date_time_in","date_time_out","bit_value"]
    # нормализация: приводим имена к snake_case и выравниваем под want
    rename = {c: sanitize_ident(str(c)) for c in df.columns}
    df = df.rename(columns=rename)
    missing = [c for c in want if c not in df.columns]
    if missing:
        raise SystemExit(f"Лист logs: отсутствуют колонки {missing}. Ожидаю: {want}")

    df = df[want].where(pd.notnull, None)

    with engine.begin() as conn:
        if mode in ("replace","truncate"):
            # id по порядку
            df = df.copy()
            df.insert(0, "id", range(1, len(df)+1))
            cols = ["id"] + want
            ph_cols = ", ".join(cols)
            values_rows, params = [], {}
            for i, row in enumerate(df.itertuples(index=False), 1):
                tuple_ph = []
                for j, col in enumerate(cols):
                    key = f":p_{i}_{j}"
                    tuple_ph.append(key)
                    params[f"p_{i}_{j}"] = getattr(row, col)
                values_rows.append("(" + ", ".join(tuple_ph) + ")")
            if values_rows:
                sql = f"INSERT INTO {full_table} ({ph_cols}) VALUES " + ", ".join(values_rows) + ";"
                conn.execute(text(sql), params)
            # выставим sequence = max(id)+1
            seq_name = f'{table_name}_id_seq'
            full_seq = quote_ident(schema, seq_name)
            conn.execute(text(f"""
                SELECT setval('{full_seq}', COALESCE((SELECT MAX(id) FROM {full_table}), 0) + 1, false);
            """))
        else:
            # append — без явного id, sequence назначит сама
            ph_cols = ", ".join(want)
            values_rows, params = [], {}
            for i, row in enumerate(df[want].itertuples(index=False), 1):
                tuple_ph = []
                for j, col in enumerate(want):
                    key = f":p_{i}_{j}"
                    tuple_ph.append(key)
                    params[f"p_{i}_{j}"] = getattr(row, col)
                values_rows.append("(" + ", ".join(tuple_ph) + ")")
            if values_rows:
                sql = f"INSERT INTO {full_table} ({ph_cols}) VALUES " + ", ".join(values_rows) + ";"
                conn.execute(text(sql), params)


# ===== Утилиты =====
def sanitize_ident(name: str) -> str:
    n = name.strip()
    n = re.sub(r"\s+", "_", n)
    n = re.sub(r"[^A-Za-z0-9_]", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    if not n:
        n = "col"
    return n.lower()

def quote_ident(schema: str | None, ident: str) -> str:
    if schema:
        return f'"{schema}".{ident}'
    return ident

# ===== Создание таблиц =====
def ensure_default_table(engine, schema, table_name, columns: list[str], if_exists: str):
    full_table = quote_ident(schema, table_name)
    seq_name = f"{table_name}_id_seq"
    full_seq = quote_ident(schema, seq_name)
    with engine.begin() as conn:
        if if_exists == "replace":
            conn.execute(text(f"DROP TABLE IF EXISTS {full_table} CASCADE;"))
        conn.execute(text(f"""
            CREATE SEQUENCE IF NOT EXISTS {full_seq}
                INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;
        """))
        cols_sql = ", ".join([f"{c} text" for c in columns])
        if cols_sql:
            cols_sql = ", " + cols_sql
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                id integer PRIMARY KEY DEFAULT nextval('{full_seq}')
                {cols_sql}
            );
        """))
        if if_exists == "truncate":
            conn.execute(text(f"TRUNCATE TABLE {full_table};"))
        conn.execute(text(f"ALTER SEQUENCE {full_seq} OWNED BY {full_table}.id;"))

def ensure_errors_table(engine, schema, table_name, if_exists: str):
    full_table = quote_ident(schema, table_name)
    with engine.begin() as conn:
        if if_exists == "replace":
            conn.execute(text(f"DROP TABLE IF EXISTS {full_table} CASCADE;"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                id   integer PRIMARY KEY,
                text text NOT NULL
            );
        """))
        if if_exists == "truncate":
            conn.execute(text(f"TRUNCATE TABLE {full_table};"))

# ===== Вставка строк =====
def reset_sequence(engine, schema, table_name):
    full_table = quote_ident(schema, table_name)
    seq_name = f"{table_name}_id_seq"
    full_seq = quote_ident(schema, seq_name)
    with engine.begin() as conn:
        conn.execute(text(f"""
            SELECT setval('{full_seq}', COALESCE((SELECT MAX(id) FROM {full_table}), 0) + 1, false);
        """))

def insert_default_rows(engine, schema, table_name, columns: list[str], df: pd.DataFrame, mode: str):
    full_table = quote_ident(schema, table_name)
    with engine.begin() as conn:
        if mode in ("replace", "truncate"):
            df = df.copy()
            df.insert(0, "id", range(1, len(df)+1))
            cols = ["id"] + columns
            ph_cols = ", ".join(cols)
            values_rows = []
            params = {}
            for i, row in enumerate(df.itertuples(index=False), 1):
                tuple_ph = []
                for j, col in enumerate(cols):
                    key = f":p_{i}_{j}"
                    tuple_ph.append(key)
                    params[f"p_{i}_{j}"] = getattr(row, col)
                values_rows.append("(" + ", ".join(tuple_ph) + ")")
            if values_rows:
                sql = f"INSERT INTO {full_table} ({ph_cols}) VALUES " + ", ".join(values_rows) + ";"
                conn.execute(text(sql), params)
            reset_sequence(engine, schema, table_name)
        else:
            cols = columns
            if cols:
                ph_cols = ", ".join(cols)
                values_rows = []
                params = {}
                for i, row in enumerate(df[cols].itertuples(index=False), 1):
                    tuple_ph = []
                    for j, col in enumerate(cols):
                        key = f":p_{i}_{j}"
                        tuple_ph.append(key)
                        params[f"p_{i}_{j}"] = getattr(row, col)
                    values_rows.append("(" + ", ".join(tuple_ph) + ")")
                if values_rows:
                    sql = f"INSERT INTO {full_table} ({ph_cols}) VALUES " + ", ".join(values_rows) + ";"
                    conn.execute(text(sql), params)
            else:
                for _ in range(len(df)):
                    conn.execute(text(f"INSERT INTO {full_table} DEFAULT VALUES;"))


# test string
# 
def insert_errors_rows(engine, schema, table_name, df_err: pd.DataFrame, if_exists: str):
    full_table = quote_ident(schema, table_name)
    cols = ["id", "text"]
    values_rows, params = [], {}
    for i, row in enumerate(df_err[cols].itertuples(index=False), 1):
        tuple_ph = []
        for j, col in enumerate(cols):
            key = f":p_{i}_{j}"
            tuple_ph.append(key)
            params[f"p_{i}_{j}"] = getattr(row, col)
        values_rows.append("(" + ", ".join(tuple_ph) + ")")
    if not values_rows:
        return
    values_sql = ", ".join(values_rows)
    with engine.begin() as conn:
        if if_exists in ("replace", "truncate"):
            sql = f"INSERT INTO {full_table} (id, text) VALUES {values_sql};"
        else:
            sql = (
                f"INSERT INTO {full_table} (id, text) VALUES {values_sql} "
                f"ON CONFLICT (id) DO UPDATE SET text = EXCLUDED.text;"
            )
        conn.execute(text(sql), params)

# ===== Главный цикл =====
def main():
    engine = get_engine_from_env()
    xls = pd.ExcelFile(EXCEL_PATH)
    for sheet in xls.sheet_names:
        df_raw = pd.read_excel(EXCEL_PATH, sheet_name=sheet, dtype=object).where(pd.notnull, None)
        raw_cols = list(df_raw.columns)
        norm_cols = [sanitize_ident(str(c)) for c in raw_cols]

        # 1) ОСОБЫЙ СЛУЧАЙ: лист 'logs' → строгая схема и типы
        if sanitize_ident(sheet) == "logs":
            print(f"\n[Sheet] {sheet} (logs) → {TARGET_SCHEMA}.logs ({len(df_raw)} rows)")
            ensure_logs_table(engine, TARGET_SCHEMA, IF_EXISTS)
            insert_logs_rows(engine, TARGET_SCHEMA, df_raw, mode=IF_EXISTS)
            print(f"[OK] Loaded {len(df_raw)} rows into {TARGET_SCHEMA}.logs")
            continue

        # 2) ОСОБЫЙ СЛУЧАЙ: справочник ошибок (id+text) — оставьте вашу ветку как раньше
        if set(norm_cols) == {"id", "text"} and len(norm_cols) == 2:
            rename_map = {raw_cols[i]: norm_cols[i] for i in range(len(raw_cols))}
            df_err = df_raw.rename(columns=rename_map)[["id", "text"]]
            table_name = sanitize_ident(sheet)
            print(f"\n[Sheet] {sheet} (errors) → {TARGET_SCHEMA}.{table_name} ({len(df_err)} rows)")
            ensure_errors_table(engine, TARGET_SCHEMA, table_name, if_exists=IF_EXISTS)
            insert_errors_rows(engine, TARGET_SCHEMA, table_name, df_err, if_exists=IF_EXISTS)
            print(f"[OK] Loaded {len(df_err)} rows into {TARGET_SCHEMA}.{table_name} (id from Excel)")
            continue

        # 3) Обычные листы (id через sequence) — оставьте ваш действующий код
        cols, rename_map = [], {}
        for c in raw_cols:
            sc = sanitize_ident(str(c))
            if sc == "id":
                continue
            cols.append(sc)
            rename_map[c] = sc
        df = df_raw[[c for c in raw_cols if sanitize_ident(str(c)) != "id"]].rename(columns=rename_map)

        table_name = sanitize_ident(sheet)
        print(f"\n[Sheet] {sheet} → {TARGET_SCHEMA}.{table_name} ({len(df)} rows)")
        ensure_default_table(engine, TARGET_SCHEMA, table_name, cols, if_exists=IF_EXISTS)
        insert_default_rows(engine, TARGET_SCHEMA, table_name, cols, df, mode=IF_EXISTS)
        print(f"[OK] Loaded {len(df)} rows into {TARGET_SCHEMA}.{table_name}")
if __name__ == "__main__":
    main()
