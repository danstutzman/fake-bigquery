package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

var discoveryJson []byte
var DATASETS_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/(.*?)/datasets$")
var TABLES_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/(.*?)/datasets/(.*?)/tables$")
var JOBS_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/(.*?)/jobs$")
var QUERY_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/(.*?)/queries/(.*?)$")
var INSERT_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/(.*?)/datasets/(.*?)/tables/(.*?)/insertAll")
var SELECT_COUNT_STAR_REGEXP = regexp.MustCompile(`(?i)^SELECT COUNT\(\*\) FROM ([^.]+).([^\s]+)$`)
var SELECT_STAR_REGEXP = regexp.MustCompile(`(?i)^SELECT \* FROM ([^.]+).([^\s]+)( LIMIT ([0-9])+)?$`)

type CreateDatasetRequest struct {
	DatasetReference DatasetReference `json:"datasetReference"`
}

type DatasetReference struct {
	DatasetId string `json:"datasetId"`
	ProjectId string `json:"projectId"`
}

type CreateJobRequest struct {
	Configuration Configuration `json:"configuration"`
	JobReference  JobReference  `json:"jobReference"`
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

type InsertRowsRequest struct {
	Rows []InsertRow `json:"rows"`
}

type InsertRow struct {
	InsertId string                 `json:"insertId"`
	Json     map[string]interface{} `json:"json"`
}

func serveDiscovery(w http.ResponseWriter, r *http.Request, discoveryJson []byte) {
	w.Write(discoveryJson)
}

func listDatasets(w http.ResponseWriter, r *http.Request, project string) {
	dataset := "belugacdn_logs"
	fmt.Fprintf(w, `{
		"kind": "bigquery#datasetList",
		"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/qwnfLrlOKTXd94DjXLYMd9AnLA8\"",
		"datasets": [
		 {
			"kind": "bigquery#dataset",
			"id": "%s:%s",
			"datasetReference": {
			 "datasetId": "%s",
			 "projectId": "%s"
			}
		 }
		]
	 }`, project, dataset, dataset, project)
}

func createDataset(w http.ResponseWriter, r *http.Request, projectName string) {
	decoder := json.NewDecoder(r.Body)
	var body CreateDatasetRequest
	err := decoder.Decode(&body)
	if err != nil {
		panic(err)
	}
	defer r.Body.Close()

	projectName2 := body.DatasetReference.ProjectId
	if projectName2 != projectName {
		log.Fatalf("Expected project name to match")
	}
	datasetName := body.DatasetReference.DatasetId

	project, projectOk := projects[projectName]
	if !projectOk {
		project = Project{Datasets: map[string]Dataset{}}
		projects[projectName] = project
	}

	project.Datasets[datasetName] = Dataset{Tables: map[string]Table{}}

	// Just serve the input as output
	outputJson, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Error from Marshal: %v", err)
	}
	w.Write(outputJson)
}

func listTables(w http.ResponseWriter, r *http.Request, project, dataset string) {
	table := "visits"
	fmt.Fprintf(w, `{
		"kind": "bigquery#tableList",
		"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/zZCSENSD7Bu0j7yv3iZTn_ilPBg\"",
		"tables": [
			{
				"kind": "bigquery#table",
				"id": "%s:%s.%s",
				"tableReference": {
					"projectId": "%s",
					"datasetId": "%s",
					"tableId": "%s"
				},
				"type": "TABLE",
				"creationTime": "1510171319097"
			}
		],
		"totalItems": 1
		}
	`, project, dataset, table, project, dataset, table)
}

type CreateTableRequest struct {
	TableReference TableReference `json:"tableReference"`
	Schema         Schema         `json:"schema"`
}

type TableReference struct {
	ProjectId string `json:"projectId"`
	DatasetId string `json:"datasetId"`
	TableId   string `json:"tableId"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}

func createTable(w http.ResponseWriter, r *http.Request, projectName, datasetName string) {
	decoder := json.NewDecoder(r.Body)
	var body CreateTableRequest
	err := decoder.Decode(&body)
	if err != nil {
		panic(err)
	}
	defer r.Body.Close()

	projectName2 := body.TableReference.ProjectId
	if projectName2 != projectName {
		log.Fatalf("Expected project name to match")
	}
	datasetName2 := body.TableReference.DatasetId
	if datasetName2 != datasetName {
		log.Fatalf("Expected dataset name to match")
	}
	tableName := body.TableReference.TableId

	project, projectOk := projects[projectName]
	if !projectOk {
		project = Project{Datasets: map[string]Dataset{}}
		projects[projectName] = project
	}

	dataset, datasetOk := project.Datasets[datasetName]
	if !datasetOk {
		log.Fatalf("Dataset doesn't exist: %s", datasetName)
	}
	dataset.Tables[tableName] = Table{
		Fields: body.Schema.Fields,
	}

	// Just serve the input as output
	outputJson, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Error from Marshal: %v", err)
	}
	w.Write(outputJson)

}

func createJob(w http.ResponseWriter, r *http.Request, projectName string) {
	decoder := json.NewDecoder(r.Body)
	var body CreateJobRequest
	err := decoder.Decode(&body)
	if err != nil {
		panic(err)
	}
	defer r.Body.Close()

	query := body.Configuration.Query1.Query2

	var result Result
	if match := SELECT_COUNT_STAR_REGEXP.FindStringSubmatch(query); match != nil {
		datasetName := match[1]
		tableName := match[2]

		project, projectOk := projects[projectName]
		if !projectOk {
			log.Fatalf("Unknown project: %s", projectName)
		}

		dataset, datasetOk := project.Datasets[datasetName]
		if !datasetOk {
			log.Fatalf("Unknown dataset: %s", dataset)
		}

		table, tableOk := dataset.Tables[tableName]
		if !tableOk {
			log.Fatalf("Unknown table: %s", tableName)
		}

		numRows := len(table.Rows)
		numRowsString := fmt.Sprintf("%d", numRows)
		result = Result{
			Fields: []Field{
				Field{
					Name: "f0_",
					Type: "INTEGER",
					Mode: "NULLABLE",
				},
			},
			Rows: []ResultRow{
				ResultRow{
					Values: []ResultValue{
						ResultValue{
							Value: &numRowsString,
						},
					},
				},
			},
		}

	} else if match := SELECT_STAR_REGEXP.FindStringSubmatch(query); match != nil {
		datasetName := match[1]
		tableName := match[2]
		limit := -1
		if match[3] != "" {
			limit, err = strconv.Atoi(match[4])
			if err != nil {
				log.Fatalf("Error from Atoi: %s", err)
			}
		}

		project, projectOk := projects[projectName]
		if !projectOk {
			log.Fatalf("Unknown project: %s", projectName)
		}

		dataset, datasetOk := project.Datasets[datasetName]
		if !datasetOk {
			log.Fatalf("Unknown dataset: %s", dataset)
		}

		table, tableOk := dataset.Tables[tableName]
		if !tableOk {
			log.Fatalf("Unknown table: %s", tableName)
		}

		rows := table.Rows
		if limit != -1 {
			rows = table.Rows[0:limit]
		}

		result.Fields = table.Fields
		for _, row := range rows {
			resultValues := []ResultValue{}
			for _, field := range table.Fields {
				value := row[field.Name]

				var resultValue ResultValue
				if value != nil {
					var valueString string
					if valueFloat64, ok := value.(float64); ok {
						valueString = strconv.FormatFloat(valueFloat64, 'f', -1, 64)
					} else if field.Type == "TIMESTAMP" {
						valueString = fmt.Sprintf("%d", value.(time.Time).Unix())
					} else {
						valueString = fmt.Sprintf("%s", value)
					}
					resultValue = ResultValue{Value: &valueString}
				}

				resultValues = append(resultValues, resultValue)
			}
			result.Rows = append(result.Rows, ResultRow{Values: resultValues})
		}

	} else {
		log.Fatalf("Can't parse query: %s", query)
	}

	jobId := body.JobReference.JobId
	queryResultByJobId[jobId] = result

	email := "a@b.com"
	fmt.Fprintf(w, `{
		"kind": "bigquery#job",
		"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/_oiKSu1NLem_L8Icwp_IYkfy3vg\"",
		"id": "%s:%s",
		"selfLink": "https://www.googleapis.com/bigquery/v2/projects/%s/jobs/%s",
		"jobReference": {
		 "projectId": "%s",
		 "jobId": "%s"
		},
		"configuration": {
		 "query": {
			"query": "%s",
			"destinationTable": {
			 "projectId": "%s",
			 "datasetId": "_2cf7cfaa9c05dd2381014b72df999b53fd45fe85",
			 "tableId": "anon5fb7e0264db7f54e07e3df0833fbfcfd11d63e03"
			},
			"createDisposition": "CREATE_IF_NEEDED",
			"writeDisposition": "WRITE_TRUNCATE"
		 }
		},
		"status": {
		 "state": "DONE"
		},
		"statistics": {
		 "creationTime": "1511049825816",
		 "startTime": "1511049826072"
		},
		"user_email": "%s"
	 }`, projectName, jobId,
		projectName, jobId,
		projectName, jobId,
		query,
		projectName,
		email)
}

func serveQuery(w http.ResponseWriter, r *http.Request, projectName, jobId string) {
	fields := queryResultByJobId[jobId].Fields
	fieldsJson, err := json.Marshal(fields)
	if err != nil {
		log.Fatalf("Error from Marshal: %s", err)
	}

	rows := queryResultByJobId[jobId].Rows
	rowsJson, err := json.Marshal(rows)
	if err != nil {
		log.Fatalf("Error from Marshal: %s", err)
	}

	fmt.Fprintf(w, `{
		"kind": "bigquery#getQueryResultsResponse",
		"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/wLFL5h11OCxiWY3yDLqREwltkXs\"",
		"schema": {
			"fields": %s
		},
		"jobReference": {
			"projectId": "%s",
			"jobId": "%s"
		},
		"totalRows": "1",
		"rows": %s,
		"totalBytesProcessed": "0",
		"jobComplete": true,
		"cacheHit": true
	}`, fieldsJson, projectName, jobId, rowsJson)
}

func insertRows(w http.ResponseWriter, r *http.Request, projectName, datasetName, tableName string) {
	decoder := json.NewDecoder(r.Body)
	var body InsertRowsRequest
	err := decoder.Decode(&body)
	if err != nil {
		panic(err)
	}
	defer r.Body.Close()

	project, projectOk := projects[projectName]
	if !projectOk {
		project = Project{Datasets: map[string]Dataset{}}
		projects[projectName] = project
	}

	dataset, datasetOk := project.Datasets[datasetName]
	if !datasetOk {
		log.Fatalf("Dataset doesn't exist: %s", datasetName)
	}

	table, tableOk := dataset.Tables[tableName]
	if !tableOk {
		log.Fatalf("Table doesn't exist: %s", tableName)
	}

	for _, row := range body.Rows {
		newRow := map[string]interface{}{}
		for _, field := range table.Fields {
			value := row.Json[field.Name]
			if field.Type == "TIMESTAMP" {
				parsedTime, err := time.Parse(time.RFC3339, value.(string))
				if err != nil {
					log.Fatalf("Can't parse time: %s", value)
				}
				newRow[field.Name] = parsedTime
			} else {
				newRow[field.Name] = row.Json[field.Name]
			}
		}

		table.Rows = append(table.Rows, newRow)
	}
	dataset.Tables[tableName] = table

	// No errors implies success
	fmt.Fprintf(w, `{
		"kind": "bigquery#tableDataInsertAllResponse"
	}`)
}

func serve(w http.ResponseWriter, r *http.Request, discoveryJson []byte) {
	path := r.URL.Path
	log.Printf("Incoming path: %s", path)

	if path == "/discovery/v1/apis/bigquery/v2/rest" {
		serveDiscovery(w, r, discoveryJson)
	} else if match := DATASETS_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		if r.Method == "GET" {
			listDatasets(w, r, project)
		} else if r.Method == "POST" {
			createDataset(w, r, project)
		} else {
			log.Fatalf("Unexpected method: %s", r.Method)
		}
	} else if match := TABLES_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		dataset := match[3]
		if r.Method == "GET" {
			listTables(w, r, project, dataset)
		} else if r.Method == "POST" {
			createTable(w, r, project, dataset)
		} else {
			log.Fatalf("Unexpected method: %s", r.Method)
		}
	} else if match := JOBS_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		if r.Method == "POST" {
			createJob(w, r, project)
		} else {
			log.Fatalf("Unexpected method: %s", r.Method)
		}
	} else if match := QUERY_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		jobId := match[3]
		serveQuery(w, r, project, jobId)
	} else if match := INSERT_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		dataset := match[3]
		table := match[4]
		insertRows(w, r, project, dataset, table)
	} else {
		log.Fatalf("Don't know how to serve path %s", r.URL.Path)
	}
}

func listenAndServe(discoveryJson []byte, portNum int) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serve(w, r, discoveryJson)
	})

	log.Printf("Listening on :%d...", portNum)
	err := http.ListenAndServe(fmt.Sprintf(":%d", portNum), nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
