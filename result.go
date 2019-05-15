package duckdb

type result struct{}

func (r result) LastInsertId() (int64, error) {
	return 0, nil
}

func (r result) RowsAffected() (int64, error) {
	return 0, nil
}
