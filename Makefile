DUCKDB_VERSION=0.9.0

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

.PHONY: deps.source
deps.source:
	git clone -b v${DUCKDB_VERSION} --depth 1 https://github.com/duckdb/duckdb.git
	cp duckdb/src/include/duckdb.h duckdb.h

.PHONY: deps.darwin.amd64
deps.darwin.amd64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi

	cd duckdb && \
	make -j 8 && \
	mkdir -p lib && \
	for f in `find . -name '*.o'`; do cp $$f lib; done && \
	cd lib && \
	ar rvs ../libduckdb.a *.o && \
	cd .. && \
	mv libduckdb.a ../deps/darwin_amd64/libduckdb.a

.PHONY: deps.darwin.arm64
deps.darwin.arm64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi

	cd duckdb && \
	make -j 8 && \
	mkdir -p lib && \
	for f in `find . -name '*.o'`; do cp $$f lib; done && \
	cd lib && \
	ar rvs ../libduckdb.a *.o && \
	cd .. && \
	mv libduckdb.a ../deps/darwin_arm64/libduckdb.a

.PHONY: deps.linux.amd64
deps.linux.amd64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi

	cd duckdb && \
	make -j 8 && \
	mkdir -p lib && \
	for f in `find . -name '*.o'`; do cp $$f lib; done && \
	cd lib && \
	ar rvs ../libduckdb.a *.o && \
	cd .. && \
	mv libduckdb.a ../deps/linux_amd64/libduckdb.a

.PHONY: deps.linux.arm64
deps.linux.arm64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi

	cd duckdb && \
	make -j 8 && \
	mkdir -p lib && \
	for f in `find . -name '*.o'`; do cp $$f lib; done && \
	cd lib && \
	ar rvs ../libduckdb.a *.o && \
	cd .. && \
	mv libduckdb.a ../deps/linux_arm64/libduckdb.a
