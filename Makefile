DUCKDB_REPO=https://github.com/duckdb/duckdb.git
DUCKDB_BRANCH=v0.10.0

.PHONY: install
install:
	go install .

.PHONY: examples
examples:
	go run examples/simple.go

.PHONY: test
test:
	go test -v -race -count=1 .

.PHONY: deps.header
deps.header:
	git clone -b ${DUCKDB_BRANCH} --depth 1 ${DUCKDB_REPO}
	cp duckdb/src/include/duckdb.h duckdb.h

.PHONY: duckdb
duckdb:
	rm -rf duckdb
	git clone -b ${DUCKDB_BRANCH} --depth 1 ${DUCKDB_REPO}

DUCKDB_COMMON_BUILD_FLAGS := BUILD_SHELL=0 BUILD_UNITTESTS=0 DUCKDB_PLATFORM=any

.PHONY: deps.darwin.amd64
deps.darwin.amd64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi

	cd duckdb && \
	CFLAGS="-target x86_64-apple-macos11 -O3" CXXFLAGS="-target x86_64-apple-macos11 -O3" ${DUCKDB_COMMON_BUILD_FLAGS} make bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/darwin_amd64/libduckdb.a

.PHONY: deps.darwin.arm64
deps.darwin.arm64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi

	cd duckdb && \
	CFLAGS="-target arm64-apple-macos11 -O3" CXXFLAGS="-target arm64-apple-macos11 -O3" ${DUCKDB_COMMON_BUILD_FLAGS}  make bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/darwin_arm64/libduckdb.a

.PHONY: deps.linux.amd64
deps.linux.amd64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi

	cd duckdb && \
	CFLAGS="-O3" CXXFLAGS="-O3" ${DUCKDB_COMMON_BUILD_FLAGS} make bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/linux_amd64/libduckdb.a

.PHONY: deps.linux.arm64
deps.linux.arm64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi

	cd duckdb && \
	CC="aarch64-linux-gnu-gcc" CXX="aarch64-linux-gnu-g++" CFLAGS="-O3" CXXFLAGS="-O3" ${DUCKDB_COMMON_BUILD_FLAGS} make bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/linux_arm64/libduckdb.a

.PHONY: deps.freebsd.amd64
deps.freebsd.amd64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "freebsd" ]; then echo "Error: must run build on freebsd"; false; fi

	cd duckdb && \
	CFLAGS="-O3" CXXFLAGS="-O3" ${DUCKDB_COMMON_BUILD_FLAGS} gmake bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/freebsd_amd64/libduckdb.a