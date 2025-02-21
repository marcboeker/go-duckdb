package duckdb

import (
	"database/sql"
)

// ProfilingInfo is a recursive type containing metrics for each node in DuckDB's query plan.
// There are two types of nodes: the QUERY_ROOT and OPERATOR nodes.
// The QUERY_ROOT refers exclusively to the top-level node; its metrics are measured over the entire query.
// The OPERATOR nodes refer to the individual operators in the query plan.
type ProfilingInfo struct {
	// Metrics contains all key-value pairs of the current node.
	// The key represents the name and corresponds to the measured value.
	Metrics map[string]string
	// Children contains all children of the node and their respective metrics.
	Children []ProfilingInfo
}

// GetProfilingInfo obtains all available metrics set by the current connection.
func GetProfilingInfo(c *sql.Conn) (ProfilingInfo, error) {
	info := ProfilingInfo{}
	err := c.Raw(func(driverConn any) error {
		conn := driverConn.(*Conn)
		profilingInfo := apiGetProfilingInfo(conn.conn)
		if profilingInfo.Ptr == nil {
			return getError(errProfilingInfoEmpty, nil)
		}

		// Recursive tree traversal.
		info.getMetrics(profilingInfo)
		return nil
	})
	return info, err
}

func (info *ProfilingInfo) getMetrics(profilingInfo apiProfilingInfo) {
	m := apiProfilingInfoGetMetrics(profilingInfo)
	count := apiGetMapSize(m)
	info.Metrics = make(map[string]string, count)

	for i := uint64(0); i < count; i++ {
		key := apiGetMapKey(m, i)
		value := apiGetMapValue(m, i)

		keyStr := apiGetVarchar(key)
		valueStr := apiGetVarchar(value)
		info.Metrics[keyStr] = valueStr

		apiDestroyValue(&key)
		apiDestroyValue(&value)
	}
	apiDestroyValue(&m)

	childCount := apiProfilingInfoGetChildCount(profilingInfo)
	for i := uint64(0); i < childCount; i++ {
		profilingInfoChild := apiProfilingInfoGetChild(profilingInfo, i)
		childInfo := ProfilingInfo{}
		childInfo.getMetrics(profilingInfoChild)
		info.Children = append(info.Children, childInfo)
	}
}
