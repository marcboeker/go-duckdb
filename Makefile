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
	# We cannot download the tagged release as it thinks it is a dev release
	# and therefore has no extensions available. They are only available in
	# the tagged Git branch.
	# curl -Lo duckdb.zip https://github.com/duckdb/duckdb/archive/refs/tags/v${DUCKDB_VERSION}.zip
	# unzip -q -o duckdb.zip || true
	# mv duckdb-${DUCKDB_VERSION} duckdb
	# rm duckdb.zip

	git clone -b v${DUCKDB_VERSION} --depth 1 https://github.com/duckdb/duckdb.git
	cd duckdb/ && python3 scripts/amalgamation.py --extended --splits=10 && cd ..

	rm -rf lib
	mkdir lib
	cd duckdb/src/amalgamation && for f in *; do \
		echo '#ifdef GODUCKDB_FROM_SOURCE' > ../../../lib/$$f; cat $$f >> ../../../lib/$$f; echo '\n#endif' >> ../../../lib/$$f; \
	done

	cp duckdb/src/include/duckdb.h duckdb.h
	rm -rf duckdb

.PHONY: deps.darwin.amd64
deps.darwin.amd64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi
	cd lib && g++ -std=c++11 -O3 --target=x86_64-apple-macos11 -DGODUCKDB_FROM_SOURCE -DNDEBUG -c *.cpp || true
	ar rvs libduckdb.a lib/*.o
	mv libduckdb.a deps/darwin_amd64/libduckdb.a

.PHONY: deps.darwin.arm64
deps.darwin.arm64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi
	cd lib && g++ -std=c++11 -O3 --target=arm64-apple-macos11 -DGODUCKDB_FROM_SOURCE -DNDEBUG -c *.cpp || true
	ar rvs libduckdb.a lib/*.o
	mv libduckdb.a deps/darwin_arm64/libduckdb.a

.PHONY: deps.linux.amd64
deps.linux.amd64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi
	cd lib && g++ -std=c++11 -O3 -DGODUCKDB_FROM_SOURCE -DNDEBUG -c *.cpp || true
	ar rvs libduckdb.a lib/*.o
	mv libduckdb.a deps/linux_amd64/libduckdb.a

.PHONY: deps.linux.arm64
deps.linux.arm64:
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi
	cd lib && aarch64-linux-gnu-g++ -std=c++11 -O3 -DGODUCKDB_FROM_SOURCE -DNDEBUG -c *.cpp || true
	aarch64-linux-gnu-gcc-ar rvs libduckdb.a lib/*.o
	mv libduckdb.a deps/linux_arm64/libduckdb.a
