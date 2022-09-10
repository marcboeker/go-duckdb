DUCKDB_VERSION=0.5.0

.PHONY: install
install:
	go install .

.PHONY: examples
examples:
	go run examples/simple.go

.PHONY: test
test:
	go test -v -race -count=1 .

.PHONY: deps.source
deps.source:
	curl -Lo libduckdb.zip https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-src.zip
	unzip -o libduckdb.zip
	rm libduckdb.zip
	echo '#ifdef GODUCKDB_USE_SOURCE' > duckdb.hpp.tmp; cat duckdb.hpp >> duckdb.hpp.tmp; echo '\n#endif' >> duckdb.hpp.tmp; mv duckdb.hpp.tmp duckdb.hpp
	echo '#ifdef GODUCKDB_USE_SOURCE' > duckdb.cpp.tmp; cat duckdb.cpp >> duckdb.cpp.tmp; echo '\n#endif' >> duckdb.cpp.tmp; mv duckdb.cpp.tmp duckdb.cpp

.PHONY: deps.darwin.amd64
deps.darwin.amd64:
	g++ -std=c++11 -fPIC -g0 -O3 --target=x86_64-apple-macos11 -DGODUCKDB_USE_SOURCE -c duckdb.cpp
	ar rvs libduckdb.a duckdb.o
	mv libduckdb.a deps/darwin_amd64/libduckdb.a

.PHONY: deps.darwin.arm64
deps.darwin.arm64:
	g++ -std=c++11 -fPIC -g0 -O3 --target=arm64-apple-macos11 -DGODUCKDB_USE_SOURCE -c duckdb.cpp
	ar rvs libduckdb.a duckdb.o
	mv libduckdb.a deps/darwin_arm64/libduckdb.a

.PHONY: deps.linux.amd64
deps.linux.amd64:
	g++ -std=c++11 -fPIC -g0 -O3 -DGODUCKDB_USE_SOURCE -c duckdb.cpp
	ar rvs libduckdb.a duckdb.o
	mv libduckdb.a deps/linux_amd64/libduckdb.a
