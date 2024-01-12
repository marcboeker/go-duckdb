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

type struct3 struct {
	I            int32
	INNER_STRUCT struct2
}

type b struct {
	C []string
}

type mix_s struct {
	A []int32
	B []b
}

const (
	testAppenderTableNested = `
  CREATE TABLE test(
    id BIGINT,
	intList INT[],
    charList VARCHAR[],
    nestedIntList INT[][],
    s1 STRUCT(I INT, V VARCHAR),
    s2 STRUCT(I INT, INNER_STRUCT STRUCT(I INT, V VARCHAR)),
    s3 STRUCT(I INT, INNER_STRUCT STRUCT(I INT, INNER_STRUCT STRUCT(I INT, V VARCHAR))),
	structList STRUCT(I INT, V VARCHAR)[],
	listStruct STRUCT(L INT[]),
   	listListList INT[][][],
   	mix STRUCT(A INT[], B STRUCT(C VARCHAR[])[])[]
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
	alphabet := []string{"A", "B", "C", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "v", "u", "w", "x", "y", "z"}
	letterI := i
	if letterI >= len(alphabet) {
		letterI = i % len(alphabet)
	}
	letter := alphabet[letterI]

	return struct1{I: int32(i), V: letter}
}

func createStructSlice(i int) []struct1 {
	if i <= 0 {
		i = 1
	}

	var l []struct1
	var j int
	for j = 0; j < i; j++ {
		l = append(l, createStruct1(j))
	}
	return l
}

func createNestedLLL(i int32) [][][]int32 {
	if i <= 0 {
		i = 1
	}

	var l [][][]int32
	var j int32
	for j = 0; j < i; j++ {
		l = append(l, createNestedIntSlice(j))
	}
	return l
}

func createB(i int32) b {
	if i <= 0 {
		i = 1
	}
	return b{C: createVarcharSlice(i)}
}

func createMix(i int32) mix_s {
	if i <= 0 {
		i = 1
	}
	return mix_s{A: createIntSlice(i), B: []b{createB(rand.Int31n(100))}}
}

func createMixList(i int32) []mix_s {
	if i <= 0 {
		i = 1
	}
	var l []mix_s
	var j int32
	for j = 0; j < i; j++ {
		l = append(l, createMix(rand.Int31n(100)))
	}
	return l
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
		s3            struct3
		structList    []struct1
		listStruct    list_struct
		listListList  [][][]int32
		mix           []mix_s
	}
	randRow := func(i int) dataRow {

		return dataRow{
			ID:            i,
			intList:       createIntSlice(rand.Int31n(3000)),
			charList:      createVarcharSlice(rand.Int31n(3000)),
			nestedIntList: createNestedIntSlice(rand.Int31n(100)),
			s1:            createStruct1(i),
			s2:            struct2{I: int32(i), INNER_STRUCT: createStruct1(i)},
			s3:            struct3{I: int32(i), INNER_STRUCT: struct2{I: int32(i), INNER_STRUCT: createStruct1(i)}},
			structList:    createStructSlice(rand.Intn(100)),
			listStruct:    list_struct{createIntSlice(rand.Int31n(3000))},
			listListList:  createNestedLLL(rand.Int31n(10)),
			mix:           createMixList(rand.Int31n(100)),
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
			row.s3,
			row.structList,
			row.listStruct,
			row.listListList,
			row.mix,
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
				s2,
				s3,
				structList,
				listStruct,
				listListList,
				mix
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
		var s3 interface{}
		var structList []interface{}
		var listStruct interface{}
		var listListList []interface{}
		var mix []interface{}
		err := res.Scan(
			&r.ID,
			&intSlice,
			&charSlice,
			&nestedIntSlice,
			&s1,
			&s2,
			&s3,
			&structList,
			&listStruct,
			&listListList,
			&mix,
		)
		require.NoError(t, err)
		r.intList = convertInterfaceToIntSlice(intSlice)
		r.charList = convertInterfaceToStringSlice(charSlice)
		r.nestedIntList = convertInterfaceToNestedIntSlice(nestedIntSlice)
		r.s1 = convertDuckDBStructToStruct1(s1)
		r.s2 = convertDuckDBStructToStruct2(s2)
		r.s3 = convertDuckDBStructToStruct3(s3)
		r.structList = convertDuckDBStructToStructSlice(structList)
		r.listStruct = convertDuckDBListStructToListStruct(listStruct)
		r.listListList = convertDuckDBListListListToListListList(listListList)
		r.mix = convertDuckDBMixListToMixList(mix)
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

func convertDuckDBStructToStruct3(i interface{}) struct3 {
	var s struct3
	innerStruct := i.(map[string]interface{})
	s.I = innerStruct["I"].(int32)
	s.INNER_STRUCT = convertDuckDBStructToStruct2(innerStruct["INNER_STRUCT"])
	return s
}

func convertDuckDBStructToStructSlice(i interface{}) []struct1 {
	var l []struct1
	for _, v := range i.([]interface{}) {
		l = append(l, convertDuckDBStructToStruct1(v))
	}
	return l
}

func convertDuckDBListListListToListListList(i interface{}) [][][]int32 {
	var l [][][]int32
	for _, v := range i.([]interface{}) {
		l = append(l, convertInterfaceToNestedIntSlice(v.([]interface{})))
	}
	return l
}

func convertDuckDBBStructToBStruct(i interface{}) b {
	var s b
	inner := i.([]interface{})
	innerStruct := inner[0].(map[string]interface{})
	s.C = convertInterfaceToStringSlice(innerStruct["C"].([]interface{}))
	return s
}

func convertDuckDBMixListToMixList(i []interface{}) []mix_s {
	var l []mix_s
	for _, v := range i {
		innerStruct := v.(map[string]interface{})
		l = append(l, mix_s{A: convertInterfaceToIntSlice(innerStruct["A"].([]interface{})), B: []b{convertDuckDBBStructToBStruct(innerStruct["B"])}})
	}
	return l
}
