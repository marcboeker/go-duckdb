package duckdb

type (
	tableSource interface {
		// ColumnInfos returns column information for each column of the table function.
		ColumnInfos() []ColumnInfo
		// Cardinality returns the cardinality information of the table function.
		// Optionally, if no cardinality exists, it may return nil.
		Cardinality() *CardinalityInfo
	}

	parallelTableSource interface {
		tableSource
		// Init the table source.
		// Additionally, it returns information for the parallelism-aware table source.
		Init() ParallelTableSourceInfo
		// NewLocalState returns a thread-local execution state.
		// It must return a pointer or a reference type for correct state updates.
		// go-duckdb does not prevent non-reference values.
		NewLocalState() any
	}

	sequentialTableSource interface {
		tableSource
		// Init the table source.
		Init()
	}

	// A RowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the RowTableSource, go-duckdb requests the rows.
	// It sequentially calls the FillRow method with a single thread.
	RowTableSource interface {
		sequentialTableSource
		// FillRow takes a Row and fills it with values.
		// Returns true, if there are more rows to fill.
		FillRow(Row) (bool, error)
	}

	// A ParallelRowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ParallelRowTableSource, go-duckdb requests the rows.
	// It simultaneously calls the FillRow method with multiple threads.
	// If ParallelTableSourceInfo.MaxThreads is greater than one, FillRow must use synchronisation
	// primitives to avoid race conditions.
	ParallelRowTableSource interface {
		parallelTableSource
		// FillRow takes a Row and fills it with values.
		// Returns true, if there are more rows to fill.
		FillRow(any, Row) (bool, error)
	}

	// A ChunkTableSource represents anything that produces rows in a vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ChunkTableSource, go-duckdb requests the rows.
	// It sequentially calls the FillChunk method with a single thread.
	ChunkTableSource interface {
		sequentialTableSource
		// FillChunk takes a Chunk and fills it with values.
		// Set the chunk size to 0 for end the function.
		FillChunk(DataChunk) error
	}

	// A ParallelChunkTableSource represents anything that produces rows in a vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ParallelChunkTableSource, go-duckdb requests the rows.
	// It simultaneously calls the FillChunk method with multiple threads.
	// If ParallelTableSourceInfo.MaxThreads is greater than one, FillChunk must use synchronization
	// primitives to avoid race conditions.
	ParallelChunkTableSource interface {
		parallelTableSource
		// FillChunk takes a Chunk and fills it with values.
		// Set the chunk size to 0 for end the function
		FillChunk(any, DataChunk) error
	}

	// parallelRowTSWrapper wraps a synchronous table source for a parallel context with nthreads=1
	parallelRowTSWrapper struct {
		s RowTableSource
	}

	// parallelChunkTSWrapper wraps a synchronous table source for a parallel context with nthreads=1
	parallelChunkTSWrapper struct {
		s ChunkTableSource
	}

	// ParallelTableSourceInfo contains information for initializing a parallelism-aware table source.
	ParallelTableSourceInfo struct {
		// MaxThreads is the maximum number of threads on which to run the table source function.
		// If set to 0, it uses DuckDB's default thread configuration.
		MaxThreads int
	}
)

// ParallelRow wrapper
func (s parallelRowTSWrapper) ColumnInfos() []ColumnInfo {
	return s.s.ColumnInfos()
}

func (s parallelRowTSWrapper) Cardinality() *CardinalityInfo {
	return s.s.Cardinality()
}

func (s parallelRowTSWrapper) Init() ParallelTableSourceInfo {
	s.s.Init()
	return ParallelTableSourceInfo{
		MaxThreads: 1,
	}
}

func (s parallelRowTSWrapper) NewLocalState() any {
	return struct{}{}
}

func (s parallelRowTSWrapper) FillRow(ls any, chunk Row) (bool, error) {
	return s.s.FillRow(chunk)
}

// ParallelChunk wrapper
func (s parallelChunkTSWrapper) ColumnInfos() []ColumnInfo {
	return s.s.ColumnInfos()
}

func (s parallelChunkTSWrapper) Cardinality() *CardinalityInfo {
	return s.s.Cardinality()
}

func (s parallelChunkTSWrapper) Init() ParallelTableSourceInfo {
	s.s.Init()
	return ParallelTableSourceInfo{
		MaxThreads: 1,
	}
}

func (s parallelChunkTSWrapper) NewLocalState() any {
	return struct{}{}
}

func (s parallelChunkTSWrapper) FillChunk(ls any, chunk DataChunk) error {
	return s.s.FillChunk(chunk)
}
