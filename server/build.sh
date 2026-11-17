#!/bin/sh
# Build the duckgraphlake binary into build/server/extension/rdf/duckgraphlake.
# Prerequisites as for the extension (see the root README); Ninja is used if present.
set -eu
cd "$(dirname "$0")/.."
GENERATOR=$(command -v ninja >/dev/null && echo "-G Ninja" || echo "")
cmake $GENERATOR -DCMAKE_BUILD_TYPE=Release -S duckdb -B build/server \
  -DEXTENSION_STATIC_BUILD=1 \
  -DDUCKDB_EXTENSION_CONFIGS="$PWD/extension_config.cmake;$PWD/server/extension_config.cmake" \
  -DRDF_BUILD_SERVER=ON -DOVERRIDE_GIT_DESCRIBE=v2.0.0 \
  -DBUILD_SHELL=FALSE -DBUILD_UNITTESTS=FALSE -DBUILD_EXTENSION_TEST_DEPS=none \
  -DENABLE_EXTENSION_AUTOLOADING=1 -DENABLE_EXTENSION_AUTOINSTALL=1 ${EXTRA_CMAKE_FLAGS:-}
cmake --build build/server --target duckgraphlake --parallel
echo "built build/server/extension/rdf/duckgraphlake"
