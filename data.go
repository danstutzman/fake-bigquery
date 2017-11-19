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

type DatasetReference struct {
	DatasetId string `json:"datasetId"`
	ProjectId string `json:"projectId"`
}

type Configuration struct {
	Query1 Query1 `json:"query"`
}

type Query1 struct {
	Query2 string `json:"query"`
}

type JobReference struct {
	ProjectId string `json:"projectId"`
	JobId     string `json:"jobId"`
}

type TableReference struct {
	ProjectId string `json:"projectId"`
	DatasetId string `json:"datasetId"`
	TableId   string `json:"tableId"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}

var projects = map[string]Project{}

var queryResultByJobId = map[string]Result{}
