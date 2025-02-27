package duckdb

import (
	"database/sql"

	m "github.com/marcboeker/go-duckdb/mapping"
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
		profilingInfo := m.GetProfilingInfo(conn.conn)
		if profilingInfo.Ptr == nil {
			return getError(errProfilingInfoEmpty, nil)
		}

		// Recursive tree traversal.
		info.getMetrics(profilingInfo)
		return nil
	})
	return info, err
}

func (info *ProfilingInfo) getMetrics(profilingInfo m.ProfilingInfo) {
	metricsMap := m.ProfilingInfoGetMetrics(profilingInfo)
	count := m.GetMapSize(metricsMap)
	info.Metrics = make(map[string]string, count)

	for i := m.IdxT(0); i < count; i++ {
		key := m.GetMapKey(metricsMap, i)
		value := m.GetMapValue(metricsMap, i)

		keyStr := m.GetVarchar(key)
		valueStr := m.GetVarchar(value)
		info.Metrics[keyStr] = valueStr

		m.DestroyValue(&key)
		m.DestroyValue(&value)
	}
	m.DestroyValue(&metricsMap)

	childCount := m.ProfilingInfoGetChildCount(profilingInfo)
	for i := m.IdxT(0); i < childCount; i++ {
		profilingInfoChild := m.ProfilingInfoGetChild(profilingInfo, i)
		childInfo := ProfilingInfo{}
		childInfo.getMetrics(profilingInfoChild)
		info.Children = append(info.Children, childInfo)
	}
}
