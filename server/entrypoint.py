"""DuckDB + rdf server: serve mounted data files over Quack, with SPARQL built in.

Reads /config/server.toml (see server.example.toml), exposes what is mounted
under /data as views and attached databases, sets the SPARQL defaults
(mapping, ontology, typed results), restricts file access to the data and
config directories, and serves the database over DuckDB's Quack protocol.
Quack listens on localhost; the container's nginx terminates TLS in front of it.
"""
import glob
import os
import re
import signal
import sys
import time
import tomllib

import duckdb

CONFIG = os.environ.get("RDF_SERVER_CONFIG", "/config/server.toml")
EXTENSION = os.environ.get("RDF_EXTENSION", "/opt/duckdb/rdf.duckdb_extension")
QUACK_PORT = int(os.environ.get("QUACK_PORT", "9494"))


def log(msg):
    print(f"[rdf-server] {msg}", flush=True)


def ident(name):
    """A safe SQL identifier: the file stem with anything unusual replaced."""
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not cleaned or cleaned[0].isdigit():
        cleaned = "t_" + cleaned
    return '"' + cleaned + '"'


def literal(text):
    return "'" + str(text).replace("'", "''") + "'"


def load_config():
    if not os.path.exists(CONFIG):
        log(f"no {CONFIG}; using defaults (auto-discovery of /data only)")
        return {}
    with open(CONFIG, "rb") as f:
        return tomllib.load(f)


def connect(cfg):
    settings = cfg.get("duckdb", {})
    config = {"allow_unsigned_extensions": "true", "autoinstall_known_extensions": "false",
              "autoload_known_extensions": "true"}
    for key in ("threads", "memory_limit", "temp_directory"):
        if key in settings:
            config[key] = str(settings[key])
    con = duckdb.connect(":memory:", config=config)
    con.execute(f"LOAD {literal(EXTENSION)}")
    for ext in ("quack", "parquet", "iceberg", "sqlite_scanner"):
        con.execute(f"LOAD {ext}")
    return con


def discover(con, data_dir, cfg):
    """Expose files under data_dir. Returns a list of (kind, name, source)."""
    exposed = []
    auto = cfg.get("data", {}).get("auto_discover", True)
    if auto and os.path.isdir(data_dir):
        for entry in sorted(os.listdir(data_dir)):
            path = os.path.join(data_dir, entry)
            stem, ext = os.path.splitext(entry)
            if os.path.isdir(path) and os.path.isdir(os.path.join(path, "metadata")):
                # Read the table's newest snapshot. Not every writer leaves a
                # version-hint.text, and DuckDB will not guess one, so name the newest
                # metadata file explicitly. allow_moved_paths: a mounted table's
                # absolute paths rarely match where it sits in the container.
                metadata = os.path.join(path, "metadata")
                version = ""
                if not os.path.exists(os.path.join(metadata, "version-hint.text")):
                    candidates = sorted(glob.glob(os.path.join(metadata, "*.metadata.json")))
                    if not candidates:
                        log(f"  skipped {entry}: metadata/ has no *.metadata.json")
                        continue
                    newest = os.path.basename(candidates[-1])[: -len(".metadata.json")]
                    version = f", version = {literal(newest)}"
                con.execute(f"CREATE VIEW {ident(entry)} AS SELECT * FROM "
                            f"iceberg_scan({literal(path)}{version}, allow_moved_paths = true)")
                exposed.append(("iceberg", entry, path + (f" @ {newest}" if version else "")))
            elif os.path.isdir(path) and glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True):
                pattern = os.path.join(path, "**", "*.parquet")
                con.execute(f"CREATE VIEW {ident(entry)} AS SELECT * FROM read_parquet({literal(pattern)})")
                exposed.append(("parquet", entry, pattern))
            elif ext == ".parquet":
                con.execute(f"CREATE VIEW {ident(stem)} AS SELECT * FROM read_parquet({literal(path)})")
                exposed.append(("parquet", stem, path))
            elif ext in (".sqlite", ".sqlite3", ".db"):
                con.execute(f"ATTACH {literal(path)} AS {ident(stem)} (TYPE sqlite, READ_ONLY)")
                exposed.append(("sqlite", stem, path))
            elif ext == ".duckdb":
                con.execute(f"ATTACH {literal(path)} AS {ident(stem)} (READ_ONLY)")
                exposed.append(("duckdb", stem, path))
    # Explicit views win over discovered ones of the same name.
    for view in cfg.get("views", []):
        con.execute(f"CREATE OR REPLACE VIEW {ident(view['name'])} AS {view['sql']}")
        exposed.append(("view", view["name"], view["sql"]))
    # Tables of attached databases also as plain names, so a mapping can name them.
    if cfg.get("data", {}).get("expose_attached_tables", True):
        taken = {r[0] for r in con.sql("SELECT table_name FROM duckdb_tables() WHERE database_name = 'memory' "
                                       "UNION SELECT view_name FROM duckdb_views() WHERE database_name = 'memory'").fetchall()}
        for kind, name, _ in list(exposed):
            if kind not in ("sqlite", "duckdb"):
                continue
            db = ident(name).strip('"')
            for (table,) in con.sql(f"SELECT table_name FROM duckdb_tables() WHERE database_name = {literal(db)}").fetchall():
                if table in taken:
                    log(f"  skipped {db}.{table}: a view named {table} already exists")
                    continue
                con.execute(f"CREATE VIEW {ident(table)} AS SELECT * FROM {ident(db)}.{ident(table)}")
                taken.add(table)
                exposed.append(("table", table, f"{db}.{table}"))
    return exposed


def configure_sparql(con, cfg):
    rdf = cfg.get("rdf", {})
    if rdf.get("typed_results", True):
        con.execute("SET GLOBAL sparql_typed_results = true")
    if rdf.get("ontology"):
        con.execute(f"SET GLOBAL sparql_ontology = {literal(rdf['ontology'])}")
    if rdf.get("mapping"):
        # A macro so clients write sparql('...') instead of repeating the mapping path.
        mapping = literal(rdf["mapping"])
        con.execute(f"CREATE MACRO sparql(q) AS TABLE SELECT * FROM execute_sparql(q, {mapping})")
        con.execute(f"CREATE MACRO sparql_sql(q) AS sparql_to_sql(q, {mapping})")
    return rdf


def lock_down(con, data_dir, config_dir):
    """Only the mounted directories stay readable; no settings can change afterwards."""
    dirs = [d.rstrip("/") + "/" for d in (data_dir, config_dir)]
    con.execute(f"SET allowed_directories = [{', '.join(literal(d) for d in dirs)}]")
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")


def main():
    cfg = load_config()
    data_dir = cfg.get("data", {}).get("directory", "/data")
    config_dir = os.path.dirname(CONFIG) or "/config"
    token = os.environ.get("QUACK_TOKEN") or cfg.get("server", {}).get("token")
    if not token:
        sys.exit("[rdf-server] set QUACK_TOKEN (or server.token in the config); clients need it to connect")

    con = connect(cfg)
    exposed = discover(con, data_dir, cfg)
    for kind, name, source in exposed:
        log(f"  {kind:8s} {name}  <- {source}")
    rdf = configure_sparql(con, cfg)
    log(f"mapping: {rdf.get('mapping', '(none: pass it to execute_sparql)')}")
    log(f"ontology: {rdf.get('ontology', '(none)')}; typed results: {rdf.get('typed_results', True)}")
    if cfg.get("server", {}).get("lock_down", True):
        lock_down(con, data_dir, config_dir)

    con.execute(f"CALL quack_serve('quack:localhost:{QUACK_PORT}', token = {literal(token)})")
    log(f"serving Quack on localhost:{QUACK_PORT} (TLS proxy in front, see nginx.conf)")

    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    signal.signal(signal.SIGINT, lambda *_: stop.append(1))
    while not stop:
        time.sleep(1)
    log("stopping")


if __name__ == "__main__":
    main()
