package duckdb

import "strconv"

const standardListSize = 3000
const standardNestedListSize = 30

type ListString struct {
	L []string
}

func (l *ListString) Fill() ListString {
	for j := 0; j < standardListSize; j++ {
		l.L = append(l.L, strconv.Itoa(j)+" ducks are cool")
	}
	return *l
}

func (l *ListString) FillFromInterface(i []interface{}) ListString {
	for _, v := range i {
		l.L = append(l.L, v.(string))
	}
	return *l
}

type ListInt struct {
	L []int32
}

func (l *ListInt) Fill() ListInt {
	for j := 0; j < standardListSize; j++ {
		l.L = append(l.L, int32(j))
	}
	return *l
}

func (l *ListInt) FillFromInterface(i interface{}) ListInt {
	inner := i.(map[string]interface{})["L"]
	l.FillInnerFromInterface(inner.([]interface{}))
	return *l
}

func (l *ListInt) FillInnerFromInterface(i []interface{}) ListInt {
	for _, v := range i {
		l.L = append(l.L, v.(int32))
	}
	return *l
}

type NestedListInt struct {
	L [][]int32
}

func (l *NestedListInt) Fill() NestedListInt {
	for j := 0; j < standardNestedListSize; j++ {
		inner := ListInt{}
		l.L = append(l.L, inner.Fill().L)
	}
	return *l
}

func (l *NestedListInt) FillFromInterface(i []interface{}) NestedListInt {
	for _, v := range i {
		inner := ListInt{}
		l.L = append(l.L, inner.FillInnerFromInterface(v.([]interface{})).L)
	}
	return *l
}

type TripleNestedListInt struct {
	L [][][]int32
}

func (l *TripleNestedListInt) Fill() TripleNestedListInt {
	for j := 0; j < standardNestedListSize/3; j++ {
		inner := NestedListInt{}
		l.L = append(l.L, inner.Fill().L)
	}
	return *l
}

func (l *TripleNestedListInt) FillFromInterface(i []interface{}) TripleNestedListInt {
	for _, v := range i {
		inner := NestedListInt{}
		l.L = append(l.L, inner.FillFromInterface(v.([]interface{})).L)
	}
	return *l
}

type Base struct {
	I int32
	V string
}

func (b *Base) Fill(i int) Base {
	b.I = int32(i)
	b.V = strconv.Itoa(i) + " ducks are cool"
	return *b
}

func (b *Base) FillFromInterface(i interface{}) Base {
	inner := i.(map[string]interface{})
	b.I = inner["I"].(int32)
	b.V = inner["V"].(string)
	return *b
}

func (b *Base) ListFill(i int) []Base {
	var l []Base
	for j := 0; j < i; j++ {
		b := Base{}
		l = append(l, b.Fill(j))
	}
	return l
}

func (b *Base) ListFillFromInterface(i interface{}) []Base {
	var l []Base
	inner := i.([]interface{})
	for _, v := range inner {
		tmpB := Base{}
		l = append(l, tmpB.FillFromInterface(v))
	}
	return l
}

type Wrapper struct {
	Base
}

func (w *Wrapper) Fill(i int) Wrapper {
	w.Base.Fill(i)
	return *w
}

func (w *Wrapper) FillFromInterface(i interface{}) Wrapper {
	inner := i.(map[string]interface{})
	w.Base.FillFromInterface(inner["Base"])
	return *w
}

type TopWrapper struct {
	Wrapper
}

func (w *TopWrapper) Fill(i int) TopWrapper {
	w.Wrapper.Fill(i)
	return *w
}

func (w *TopWrapper) FillFromInterface(i interface{}) TopWrapper {
	inner := i.(map[string]interface{})
	w.Wrapper.FillFromInterface(inner["Wrapper"])
	return *w
}

type ListBase struct {
	L []Base
}

func (l *ListBase) Fill() ListBase {
	for j := 0; j < standardListSize; j++ {
		b := Base{}
		b.Fill(j)
		l.L = append(l.L, b)
	}
	return *l
}

func (l *ListBase) FillFromInterface(i interface{}) ListBase {
	inner := i.(map[string]interface{})
	for _, v := range inner["L"].([]interface{}) {
		b := Base{}
		l.L = append(l.L, b.FillFromInterface(v))
	}
	return *l
}

type Mix struct {
	A ListString
	B []ListInt
}

func (m *Mix) Fill() Mix {
	m.A.Fill()
	for j := 0; j < standardNestedListSize; j++ {
		l := ListInt{}
		m.B = append(m.B, l.Fill())
	}
	return *m
}

func (m *Mix) FillFromInterface(i interface{}) Mix {
	inner := i.(map[string]interface{})
	innerA := inner["A"].(map[string]interface{})
	m.A.FillFromInterface(innerA["L"].([]interface{}))
	for _, v := range inner["B"].([]interface{}) {
		l := ListInt{}
		m.B = append(m.B, l.FillFromInterface(v))
	}
	return *m
}

func (m *Mix) ListFill(i int) []Mix {
	var l []Mix
	var j int
	for j = 0; j < i; j++ {
		m := Mix{}
		l = append(l, m.Fill())
	}
	return l
}

func (m *Mix) ListFillFromInterface(i interface{}) []Mix {
	var l []Mix
	inner := i.([]interface{})
	for _, v := range inner {
		tmpM := Mix{}
		l = append(l, tmpM.FillFromInterface(v))
	}
	return l
}
