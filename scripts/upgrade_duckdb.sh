#!/usr/bin/env bash
set -e

DUCKDB_VERSION=0.4.0

curl -Lo libduckdb.zip https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-src.zip
unzip -u libduckdb.zip
rm libduckdb.zip
