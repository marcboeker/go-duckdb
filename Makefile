DUCKDB_REPO=https://github.com/duckdb/duckdb.git
DUCKDB_BRANCH=v1.1.3

examples:
	go run examples/appender/main.go
	go run examples/json/main.go
	go run examples/scalar_udf/main.go
	go run examples/simple/main.go
	go run examples/table_udf/main.go
	go run examples/table_udf_parallel/main.go

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

DUCKDB_COMMON_BUILD_FLAGS := BUILD_SHELL=0 BUILD_UNITTESTS=0 DUCKDB_PLATFORM=any ENABLE_EXTENSION_AUTOLOADING=1 ENABLE_EXTENSION_AUTOINSTALL=1 BUILD_EXTENSIONS="json"

.PHONY: deps.freebsd.amd64
deps.freebsd.amd64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "freebsd" ]; then echo "Error: must run build on freebsd"; false; fi
	mkdir -p deps/freebsd_amd64

	cd duckdb && \
	CFLAGS="-O3" CXXFLAGS="-O3" ${DUCKDB_COMMON_BUILD_FLAGS} gmake bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/freebsd_amd64/libduckdb.a

duplicate.mapping:
	cp mapping.go mapping_${OS_ARCH}.go && \
  	sed -i '' 's:duckdb-go-bindings:duckdb-go-bindings/${OS_ARCH}:g' mapping_${OS_ARCH}.go && \
  	sed -i '' 's:duckdb_use_lib:!duckdb_use_lib:g' mapping_${OS_ARCH}.go && \
  	sed -i '' 's:duckdb_use_static_lib:!duckdb_use_static_lib:g' mapping_${OS_ARCH}.go && \
  	sed -i '' 's:\|:\&:g' mapping_${OS_ARCH}.go

duplicate.arrow.mapping:
	cp arrow_mapping.go arrow_mapping_${OS_ARCH}.go && \
  	sed -i '' 's:duckdb-go-bindings:duckdb-go-bindings/${OS_ARCH}:g' arrow_mapping_${OS_ARCH}.go && \
  	sed -i '' 's:duckdb_use_lib:!duckdb_use_lib:g' arrow_mapping_${OS_ARCH}.go && \
  	sed -i '' 's:duckdb_use_static_lib:!duckdb_use_static_lib:g' arrow_mapping_${OS_ARCH}.go && \
  	sed -i '' 's:\|:\&:g' arrow_mapping_${OS_ARCH}.go && \
  	sed -i '' 's:(!:!:g' arrow_mapping_${OS_ARCH}.go && \
  	sed -i '' 's:b):b:g' arrow_mapping_${OS_ARCH}.go

test.dynamic.lib:
	mkdir dynamic-dir && \
	cd dynamic-dir && \
	curl -OL https://github.com/duckdb/duckdb/releases/download/${VERSION}/${FILENAME}.zip && \
	unzip ${FILENAME}.zip

test.static.lib.darwin.arm64:
	mkdir static-dir && \
	cd static-dir && \
	curl -OL https://github.com/duckdb/duckdb/releases/download/v1.2.0/static-lib-osx-arm64.zip && \
	unzip static-lib-osx-arm64.zip && \
	cp libduckdb_bundle.a libduckdb.a
