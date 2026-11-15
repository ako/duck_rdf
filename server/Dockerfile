# DuckDB + rdf server image: serves mounted Parquet, Iceberg, DuckDB and SQLite
# files over DuckDB's Quack protocol, with SPARQL (execute_sparql) built in.
#
# Build context: this directory, after `./stage.sh` has put the extensions in
# dist/ (the rdf extension built against DuckDB 2.0.0, and the official core
# extensions it uses). See README.md.
FROM python:3.13-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends nginx openssl libxml2 libcurl4t64 \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir duckdb==2.0.0

# Extensions are baked in, so the container never downloads anything.
COPY dist/extensions/ /root/.duckdb/extensions/
COPY dist/rdf.duckdb_extension /opt/duckdb/rdf.duckdb_extension
COPY entrypoint.py nginx.conf start.sh /opt/rdf-server/
RUN chmod +x /opt/rdf-server/start.sh \
 && python3 -c "import duckdb; c = duckdb.connect(config={'allow_unsigned_extensions': 'true'}); \
c.execute(\"LOAD '/opt/duckdb/rdf.duckdb_extension'\"); \
[c.execute(f'LOAD {e}') for e in ('quack', 'httpfs', 'parquet', 'iceberg', 'sqlite_scanner')]; print('extensions load')"

VOLUME ["/data", "/config"]
EXPOSE 9443
ENTRYPOINT ["/opt/rdf-server/start.sh"]
