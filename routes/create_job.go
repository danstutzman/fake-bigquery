package routes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/danielstutzman/fake-bigquery/data"
)

var SELECT_COUNT_STAR_REGEXP = regexp.MustCompile(`(?i)^SELECT COUNT\(\*\) FROM ([^.]+).([^\s]+)$`)
var SELECT_STAR_REGEXP = regexp.MustCompile(`(?i)^SELECT \* FROM ([^.]+).([^\s]+)( LIMIT ([0-9])+)?$`)

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

func (app *App) createJob(w http.ResponseWriter, r *http.Request, projectName string) {
	decoder := json.NewDecoder(r.Body)
	var body CreateJobRequest
	err := decoder.Decode(&body)
	if err != nil {
		panic(err)
	}
	defer r.Body.Close()

	query := body.Configuration.Query1.Query2

	var result data.Result
	if match := SELECT_COUNT_STAR_REGEXP.FindStringSubmatch(query); match != nil {
		datasetName := match[1]
		tableName := match[2]

		project, projectOk := app.projects[projectName]
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
		result = data.Result{
			Fields: []data.Field{
				data.Field{
					Name: "f0_",
					Type: "INTEGER",
					Mode: "NULLABLE",
				},
			},
			Rows: []data.ResultRow{
				data.ResultRow{
					Values: []data.ResultValue{
						data.ResultValue{
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

		project, projectOk := app.projects[projectName]
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

		fromRows := table.Rows
		if limit != -1 {
			fromRows = fromRows[0:limit]
		}

		result.Fields = make([]data.Field, len(table.Fields))
		result.Rows = make([]data.ResultRow, 0)

		copy(result.Fields, table.Fields)
		for _, fromRow := range fromRows {
			resultValues := []data.ResultValue{}
			for _, field := range table.Fields {
				value := fromRow[field.Name]

				var resultValue data.ResultValue
				if value != nil {
					var valueString string
					if valueFloat64, ok := value.(float64); ok {
						valueString = strconv.FormatFloat(valueFloat64, 'f', -1, 64)
					} else if field.Type == "TIMESTAMP" {
						valueString = fmt.Sprintf("%d", value.(time.Time).Unix())
					} else {
						valueString = fmt.Sprintf("%s", value)
					}
					resultValue = data.ResultValue{Value: &valueString}
				}

				resultValues = append(resultValues, resultValue)
			}
			result.Rows = append(result.Rows, data.ResultRow{
				Values: resultValues,
			})
		}

	} else {
		log.Fatalf("Can't parse query: %s", query)
	}

	jobId := body.JobReference.JobId
	app.queryResultByJobId[jobId] = result

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
