package duckdb

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

type struct1 struct {
	I int32
	V string
}

type struct2 struct {
	I            int32
	INNER_STRUCT struct1
}

const (
	testAppenderTableNested = `
  CREATE TABLE test(
    id BIGINT,
	intList INT[],
    charList VARCHAR[],
    nestedIntList INT[][],
    s1 STRUCT(I INT, V VARCHAR)
  )`
)

func createAppenderNestedTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testAppenderTableNested)
	require.NoError(t, err)
	return &res
}

func createNestedIntSlice(i int32) [][]int32 {
	if i <= 0 {
		i = 1
	}
	var l [][]int32
	var j int32
	for j = 0; j < i; j++ {
		l = append(l, createIntSlice(rand.Int31n(3000)))
	}
	return l
}

func createIntSlice(i int32) []int32 {
	if i <= 0 {
		i = 1
	}
	var l []int32
	var j int32
	for j = 0; j < i; j++ {
		l = append(l, j)
	}
	return l
}

func createVarcharSlice(i int32) []string {
	if i <= 0 {
		i = 1
	}
	var l []string
	var j int32
	for j = 0; j < i; j++ {
		l = append(l, randString(rand.Intn(100)))
	}
	return l
}

func createStruct1(i int) struct1 {
	alphabet := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "v", "u", "w", "x", "y", "z"}
	letterI := i
	if letterI >= len(alphabet) {
		letterI = i % len(alphabet)
	}
	letter := alphabet[letterI]

	return struct1{I: int32(i), V: letter}
}

func TestNestedAppender(t *testing.T) {

	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	createAppenderNestedTable(db, t)
	defer db.Close()

	type dataRow struct {
		ID            int
		intList       []int32
		charList      []string
		nestedIntList [][]int32
		s1            struct1
		s2            struct2
	}
	randRow := func(i int) dataRow {

		return dataRow{
			ID:            i,
			intList:       createIntSlice(rand.Int31n(3000)),
			charList:      createVarcharSlice(rand.Int31n(3000)),
			nestedIntList: createNestedIntSlice(rand.Int31n(100)),
			s1:            createStruct1(i),
			s2:            struct2{I: int32(i), INNER_STRUCT: createStruct1(i)},
		}
	}
	rows := []dataRow{}
	for i := 0; i < 100; i++ {
		rows = append(rows, randRow(i))
	}

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)
	defer appender.Close()

	for _, row := range rows {
		err := appender.AppendRow(
			row.ID,
			row.intList,
			row.charList,
			row.nestedIntList,
			row.s1,
			row.s2,
		)
		require.NoError(t, err)
	}
	err = appender.Flush()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(), `
			SELECT  
			    id,
					intList,
					charList,
					nestedIntList,
					s1,
					s2
      FROM test
      ORDER BY id
      `)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		r := dataRow{}
		var intSlice []interface{}
		var charSlice []interface{}
		var nestedIntSlice []interface{}
		var s1 interface{}
		var s2 interface{}
		err := res.Scan(
			&r.ID,
			&intSlice,
			&charSlice,
			&nestedIntSlice,
			&s1,
			&s2,
		)
		require.NoError(t, err)
		r.intList = convertInterfaceToIntSlice(intSlice)
		r.charList = convertInterfaceToStringSlice(charSlice)
		r.nestedIntList = convertInterfaceToNestedIntSlice(nestedIntSlice)
		r.s1 = convertDuckDBStructToStruct1(s1)
		r.s2 = convertDuckDBStructToStruct2(s2)
		require.Equal(t, rows[i], r)
		i++
	}
	// Ensure that the number of fetched rows equals the number of inserted rows.
	require.Equal(t, i, 100)
}

func convertInterfaceToIntSlice(i []interface{}) []int32 {
	var l []int32
	for _, v := range i {
		l = append(l, v.(int32))
	}
	return l
}

func convertInterfaceToStringSlice(i []interface{}) []string {
	var l []string
	for _, v := range i {
		l = append(l, v.(string))
	}
	return l
}

func convertInterfaceToNestedIntSlice(i []interface{}) [][]int32 {
	var l [][]int32
	for _, v := range i {
		innerSlice := v.([]interface{})
		innerNestedIntSlice := make([]int32, len(innerSlice))
		for innerI, innerV := range innerSlice {
			innerNestedIntSlice[innerI] = innerV.(int32)
		}
		l = append(l, innerNestedIntSlice)
	}
	return l
}

func convertDuckDBStructToStruct1(i interface{}) struct1 {
	var s struct1
	innerStruct := i.(map[string]interface{})
	s.I = innerStruct["I"].(int32)
	s.V = innerStruct["V"].(string)
	return s
}

func convertDuckDBStructToStruct2(i interface{}) struct2 {
	var s struct2
	innerStruct := i.(map[string]interface{})
	s.I = innerStruct["I"].(int32)
	s.INNER_STRUCT = convertDuckDBStructToStruct1(innerStruct["INNER_STRUCT"])
	return s
}
