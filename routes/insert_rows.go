package routes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/danielstutzman/fake-bigquery/data"
)

type InsertRowsRequest struct {
	Rows []InsertRow `json:"rows"`
}

type InsertRow struct {
	InsertId string                 `json:"insertId"`
	Json     map[string]interface{} `json:"json"`
}

func insertRows(w http.ResponseWriter, r *http.Request, projectName, datasetName, tableName string) {
	decoder := json.NewDecoder(r.Body)
	var body InsertRowsRequest
	err := decoder.Decode(&body)
	if err != nil {
		panic(err)
	}
	defer r.Body.Close()

	project, projectOk := data.Projects[projectName]
	if !projectOk {
		project = data.Project{
			Datasets: map[string]data.Dataset{},
		}
		data.Projects[projectName] = project
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
