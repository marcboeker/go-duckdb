test.examples:
	go run examples/appender/main.go
	go run examples/json/main.go
	go run examples/scalar_udf/main.go
	go run examples/simple/main.go
	go run examples/table_udf/main.go
	go run examples/table_udf_parallel/main.go

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
