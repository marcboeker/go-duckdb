# Contributing

## Pull Requests

* Write [idiomatic Go](https://go.dev/doc/effective_go).
  * Commentary: comments for humans are formatted like so: `// Uppercase sentence. Single-space next sentence.`
* Are you adding a new feature - great! Add tests to cover your new functionality. go-duckdb aims to 
contain tests that are fast, cover a significant portion of the code, are comprehensible, and can serve as documentation.
* Pull requests will need to pass all continuous integration checks before merging.
* We reserve full and final discretion over whether we will merge a pull request or not. Adhering to these guidelines is not a complete guarantee that your pull request will be merged.

## Upgrading DuckDB

To upgrade to a new version of DuckDB:

1. Create a new branch with the current version number in it. E.g. `v0.9.0`.
2. Change `DUCKDB_VERSION` in `Makefile` to match the version in the branch name.
3. Push the updated `Makefile` and create a PR.
4. Wait for Github Actions to pre-compile the static libraries in `deps`. They will be committed automatically to the branch.
5. If everything looks good, the PR will be merged.