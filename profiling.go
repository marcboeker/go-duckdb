package duckdb

import (
	"database/sql"

	"github.com/marcboeker/go-duckdb/mapping"
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
		profilingInfo := mapping.GetProfilingInfo(conn.conn)
		if profilingInfo.Ptr == nil {
			return getError(errProfilingInfoEmpty, nil)
		}

		// Recursive tree traversal.
		info.getMetrics(profilingInfo)
		return nil
	})

	return info, err
}

func (info *ProfilingInfo) getMetrics(profilingInfo mapping.ProfilingInfo) {
	metricsMap := mapping.ProfilingInfoGetMetrics(profilingInfo)
	count := mapping.GetMapSize(metricsMap)
	info.Metrics = make(map[string]string, count)

	for i := mapping.IdxT(0); i < count; i++ {
		key := mapping.GetMapKey(metricsMap, i)
		value := mapping.GetMapValue(metricsMap, i)

		keyStr := mapping.GetVarchar(key)
		valueStr := mapping.GetVarchar(value)
		info.Metrics[keyStr] = valueStr

		mapping.DestroyValue(&key)
		mapping.DestroyValue(&value)
	}
	mapping.DestroyValue(&metricsMap)

	// Extra info is not currently in the metric map
	extraInfo := mapping.ProfilingInfoGetValue(profilingInfo, "EXTRA_INFO")
	extraInfoStr := mapping.GetVarchar(extraInfo)
	info.Metrics["EXTRA_INFO"] = extraInfoStr
	mapping.DestroyValue(&extraInfo)

	childCount := mapping.ProfilingInfoGetChildCount(profilingInfo)
	for i := mapping.IdxT(0); i < childCount; i++ {
		profilingInfoChild := mapping.ProfilingInfoGetChild(profilingInfo, i)
		childInfo := ProfilingInfo{}
		childInfo.getMetrics(profilingInfoChild)
		info.Children = append(info.Children, childInfo)
	}
}
