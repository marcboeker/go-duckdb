# Contributing

## Upgrading DuckDB

To upgrade to a new version of DuckDB:

1. Create a new branch with the current version number in it. E.g. `v0.9.2`.
2. Change `DUCKDB_VERSION` in `Makefile` to match the version in the branch name.
3. Push the updated `Makefile` and create a PR.
4. Wait for Github Actions to pre-compile the static libraries in `deps`. They will be committed automatically to the branch.
5. If everything looks good, the PR will be merged.
