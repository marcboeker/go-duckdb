DUCKDB_VERSION=0.9.2

.PHONY: install
install:
	go install .

.PHONY: examples
examples:
	go run examples/simple.go

.PHONY: test
test:
	go test -v -race -count=1 .

SRC_DIR := duckdb/src/amalgamation
FILES := $(wildcard $(SRC_DIR)/*)

.PHONY: deps.header
deps.header:
	git clone -b v${DUCKDB_VERSION} --depth 1 https://github.com/duckdb/duckdb.git
	cp duckdb/src/include/duckdb.h duckdb.h

.PHONY: deps.darwin.amd64
deps.darwin.amd64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi

	git clone -b v${DUCKDB_VERSION} --depth 1 https://github.com/duckdb/duckdb.git
	cd duckdb && \
	CFLAGS="-target x86_64-apple-macos11 -O3" CXXFLAGS="-target x86_64-apple-macos11 -O3" BUILD_SHELL=0 BUILD_UNITTESTS=0 make -j 2 && \
	mkdir -p lib && \
	for f in `find . -name '*.o'`; do cp $$f lib; done && \
	cd lib && \
	ar rvs ../libduckdb.a *.o && \
	cd .. && \
	mv libduckdb.a ../deps/darwin_amd64/libduckdb.a

.PHONY: deps.darwin.arm64
deps.darwin.arm64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi

	git clone -b v${DUCKDB_VERSION} --depth 1 https://github.com/duckdb/duckdb.git
	cd duckdb && \
	CFLAGS="-target arm64-apple-macos11 -O3" CXXFLAGS="-target arm64-apple-macos11 -O3" BUILD_SHELL=0 BUILD_UNITTESTS=0 make -j 2 && \
	mkdir -p lib && \
	for f in `find . -name '*.o'`; do cp $$f lib; done && \
	cd lib && \
	ar rvs ../libduckdb.a *.o && \
	cd .. && \
	mv libduckdb.a ../deps/darwin_arm64/libduckdb.a

.PHONY: deps.linux.amd64
deps.linux.amd64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi

	git clone -b v${DUCKDB_VERSION} --depth 1 https://github.com/duckdb/duckdb.git
	cd duckdb && \
	CFLAGS="-O3" CXXFLAGS="-O3" make -j 2 && \
	BUILD_SHELL=0 BUILD_UNITTESTS=0 make -j 2 && \
	mkdir -p lib && \
	for f in `find . -name '*.o'`; do cp $$f lib; done && \
	cd lib && \
	ar rvs ../libduckdb.a *.o && \
	cd .. && \
	mv libduckdb.a ../deps/linux_amd64/libduckdb.a

.PHONY: deps.linux.arm64
deps.linux.arm64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi

	git clone -b v${DUCKDB_VERSION} --depth 1 https://github.com/duckdb/duckdb.git
	cd duckdb && \
	CC="aarch64-linux-gnu-gcc" CXX="aarch64-linux-gnu-g++" CFLAGS="-O3" CXXFLAGS="-O3" BUILD_SHELL=0 BUILD_UNITTESTS=0 make -j 2 && \
	mkdir -p lib && \
	for f in `find . -name '*.o'`; do cp $$f lib; done && \
	cd lib && \
	ar rvs ../libduckdb.a *.o && \
	cd .. && \
	mv libduckdb.a ../deps/linux_arm64/libduckdb.a

.PHONY: deps.windows.amd64
deps.windows.amd64:
	git clone -b v${DUCKDB_VERSION} --depth 1 https://github.com/duckdb/duckdb.git
	cd duckdb && \
	mkdir -p build && \
	cmake -G "MinGW Makefiles" \
	-DENABLE_EXTENSION_AUTOLOADING=1 \
	-DENABLE_EXTENSION_AUTOINSTALL=1 \
	-DBUILD_EXTENSIONS=parquet \
	-DDUCKDB_EXTENSION_CONFIGS="./.github/config/bundled_extensions.cmake" \
	-DBUILD_SHELL=0 \
	-DBUILD_BENCHMARK=0 \
	-DBUILD_JDBC=0 \
	-DBUILD_TPCH=0 \
	-DBUILD_TPCDS=0 \
	-DBUILD_ODBC=0 \
	-DBUILD_PYTHON=0 \
	-DDISABLE_UNITY=1 \
	-DBUILD_AUTOCOMPLETE=1 \
	-DBUILD_HTTPFS=1 \
	-DBUILD_JSON=1 \
	-DBUILD_INET=1 \
	-DBUILD_FTS=1 \
	-DCMAKE_BUILD_TYPE=Release -B build && \
	cd build && \
	MAKEFLAGS=-j2 cmake --build . --config Release && \
	cd .. && \
	mkdir -p lib && \
	find ./build -name '*.obj' | xargs cp {} -t ./lib && \
	cd lib && \
	gcc-ar rvs libduckdb.a *.obj
	mv libduckdb.a ../../deps/windows_amd64/libduckdb.a
