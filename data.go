package main

type Table struct {
	Fields []Field
	Rows   []map[string]interface{}
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"` // TIMESTAMP, FLOAT, STRING, INTEGER
	Mode string `json:"mode"` // NULLABLE, REQUIRED
}

type Dataset struct {
	Tables map[string]Table
}

type Project struct {
	Datasets map[string]Dataset
}

type Result struct {
	Fields []Field
	Rows   []ResultRow
}

type ResultRow struct {
	Values []ResultValue `json:"f"`
}

type ResultValue struct {
	Value *string `json:"v"`
}

var projects = map[string]Project{}

var queryResultByJobId = map[string]Result{}
