package routes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/danielstutzman/fake-bigquery/data"
)

func (app *App) listTables(w http.ResponseWriter, r *http.Request, projectName, datasetName string) {
	project, projectOk := app.projects[projectName]
	if !projectOk {
		project = data.Project{
			Datasets: map[string]data.Dataset{},
		}
		app.projects[projectName] = project
	}

	dataset, datasetOk := project.Datasets[datasetName]
	if !datasetOk {
		log.Fatalf("Unknown dataset %s", datasetName)
	}

	tableOutputs := []map[string]interface{}{}
	for table := range dataset.Tables {
		tableOutput := map[string]interface{}{
			"kind": "bigquery#table",
			"id":   fmt.Sprintf("%s:%s.%s", projectName, datasetName, table),
			"tableReference": map[string]string{
				"projectId": projectName,
				"datasetId": datasetName,
				"tableId":   table,
			},
			"type":         "TABLE",
			"creationTime": "1234567890123",
		}
		tableOutputs = append(tableOutputs, tableOutput)
	}

	tableOutputsJson, err := json.Marshal(tableOutputs)
	if err != nil {
		log.Fatalf("Error from Marshal: %v", err)
	}

	fmt.Fprintf(w, `{
		"kind": "bigquery#tableList",
		"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/zZCSENSD7Bu0j7yv3iZTn_ilPBg\"",
		"tables": %s,
		"totalItems": %d
	}`, tableOutputsJson, len(tableOutputs))
}
