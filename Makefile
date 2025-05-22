test.examples:
	go run examples/appender/main.go
	go run examples/json/main.go
	go run examples/scalar_udf/main.go
	go run examples/simple/main.go
	go run examples/table_udf/main.go
	go run examples/table_udf_parallel/main.go

duplicate.mapping:
	cp mapping/mapping.go mapping/mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:duckdb-go-bindings:duckdb-go-bindings/${OS_ARCH}:g' mapping/mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:duckdb_use_lib:!duckdb_use_lib:g' mapping/mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:duckdb_use_static_lib:!duckdb_use_static_lib:g' mapping/mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:\|:\&:g' mapping/mapping_${FILE_SUFFIX}.go

duplicate.arrow.mapping:
	cp arrowmapping/arrow_mapping.go arrowmapping/arrow_mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:duckdb-go-bindings:duckdb-go-bindings/${OS_ARCH}:g' arrowmapping/arrow_mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:duckdb_use_lib:!duckdb_use_lib:g' arrowmapping/arrow_mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:duckdb_use_static_lib:!duckdb_use_static_lib:g' arrowmapping/arrow_mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:\|:\&:g' arrowmapping/arrow_mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:(!:!:g' arrowmapping/arrow_mapping_${FILE_SUFFIX}.go && \
  	sed -i '' 's:b):b:g' arrowmapping/arrow_mapping_${FILE_SUFFIX}.go

test.dynamic.lib:
	mkdir dynamic-dir && \
	cd dynamic-dir && \
	curl -OL https://github.com/duckdb/duckdb/releases/download/${VERSION}/${FILENAME}.zip && \
	unzip ${FILENAME}.zip

test.static.lib.darwin.arm64:
	mkdir static-dir && \
	cd static-dir && \
	curl -OL https://github.com/duckdb/duckdb/releases/download/${VERSION}/static-libs-osx-arm64.zip && \
	unzip static-libs-osx-arm64.zip && \
	cp libduckdb_bundle.a libduckdb.a
