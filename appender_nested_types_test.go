package duckdb

import (
	"strconv"
)

const childLength = 3000
const parentLength = 30
const topLength = 3

type ListString struct {
	L []string
}

func (l *ListString) Fill() ListString {
	for j := 0; j < childLength; j++ {
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

type Int32List []int32
type ListInt struct {
	L Int32List
}

func (l *Int32List) Fill() {
	*l = make(Int32List, childLength)
	for j := int32(0); j < childLength; j++ {
		(*l)[j] = j
	}
}

func (l *Int32List) FillFromInterface(i interface{}) {
	inner := i.(map[string]interface{})["L"]
	l.FillInnerFromInterface(inner.([]interface{}))
}

func (l *Int32List) FillInnerFromInterface(i []interface{}) {
	*l = make(Int32List, len(i))
	for j, v := range i {
		(*l)[j] = v.(int32)
	}
}

type Int32ListList []Int32List

func (l *Int32ListList) Fill() {
	*l = make(Int32ListList, parentLength)
	for j := 0; j < parentLength; j++ {
		(*l)[j].Fill()
	}
}

func (l *Int32ListList) FillFromInterface(i []interface{}) {
	*l = make(Int32ListList, len(i))
	for j, v := range i {
		(*l)[j].FillInnerFromInterface(v.([]interface{}))
	}
}

type Int32ListListList []Int32ListList

func (l *Int32ListListList) Fill() {
	*l = make(Int32ListListList, topLength)
	for j := 0; j < topLength; j++ {
		(*l)[j].Fill()
	}
}

func (l *Int32ListListList) FillFromInterface(i []interface{}) {
	*l = make(Int32ListListList, len(i))
	for j, v := range i {
		(*l)[j].FillFromInterface(v.([]interface{}))
	}
}

type BaseList []Base

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

func (b *BaseList) ListFill() {
	*b = make(BaseList, childLength)
	for j := 0; j < childLength; j++ {
		(*b)[j].Fill(j)
	}
}

func (b *BaseList) ListFillFromInterface(i interface{}) {
	inner := i.([]interface{})
	*b = make(BaseList, len(inner))
	for j, v := range inner {
		(*b)[j].FillFromInterface(v)
	}
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

type MixList []Mix
type Mix struct {
	A ListString
	B []ListInt
}

func (m *Mix) Fill() Mix {
	m.A.Fill()
	m.B = make([]ListInt, parentLength)
	for j := 0; j < parentLength; j++ {
		m.B[j].L.Fill()
	}
	return *m
}

func (m *Mix) FillFromInterface(i interface{}) Mix {
	inner := i.(map[string]interface{})
	innerA := inner["A"].(map[string]interface{})
	m.A.FillFromInterface(innerA["L"].([]interface{}))
	innerB := inner["B"].([]interface{})
	m.B = make([]ListInt, len(innerB))
	for j, v := range innerB {
		m.B[j].L.FillFromInterface(v)
	}
	return *m
}

func (m *MixList) ListFill() {
	*m = make(MixList, topLength)
	for j := int32(0); j < topLength; j++ {
		(*m)[j].Fill()
	}
}

func (m *MixList) ListFillFromInterface(i interface{}) {
	inner := i.([]interface{})
	*m = make(MixList, len(inner))
	for j, v := range inner {
		(*m)[j].FillFromInterface(v)
	}
}
