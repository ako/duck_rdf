# Extra extensions compiled into the duckgraphlake server binary, on top of
# ../extension_config.cmake (rdf). Pins match duckdb/.github/config/extensions.
duckdb_extension_load(json)
duckdb_extension_load(parquet)
duckdb_extension_load(icu)
duckdb_extension_load(core_functions)
duckdb_extension_load(quack
    GIT_URL https://github.com/duckdb/duckdb-quack
    GIT_TAG f96420ed46aee9c42064bdb99ddfd1230d6c0086
    SUBMODULES extension-ci-tools
)
duckdb_extension_load(sqlite_scanner
    GIT_URL https://github.com/duckdb/duckdb-sqlite
    GIT_TAG e223dc01493414cef6d5960744f1e048d59fc592
    SUBMODULES database-connector
)
