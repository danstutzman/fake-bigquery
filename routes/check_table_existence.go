package routes

import (
	"fmt"
	"log"
	"net/http"

	"github.com/danielstutzman/fake-bigquery/data"
)

func (app *App) checkTableExistence(w http.ResponseWriter, r *http.Request, projectName, datasetName, tableName string) {
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

	_, tableExists := dataset.Tables[tableName]
	if tableExists {
		fmt.Fprintf(w, `{
			"kind": "bigquery#table",
			"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/MTUxMTEyMDI0ODcwMA\"",
			"id": "%s:%s.%s",
			"selfLink": "https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s",
			"tableReference": {
				"projectId": "%s",
				"datasetId": "%s",
				"tableId": "%s"
			},
			"numBytes": "0",
			"numLongTermBytes": "0",
			"numRows": "0",
			"creationTime": "1234567890123",
			"lastModifiedTime": "1234567890123",
			"type": "TABLE"
		}`, projectName, datasetName, tableName,
			projectName, datasetName, tableName,
			projectName, datasetName, tableName)
	} else {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{
			"error": {
			 "errors": [
          {
					 "domain": "global",
					 "reason": "notFound",
					 "message": "Not found: Table %s:%s.%s"
					}
			 ],
			 "code": 404,
			 "message": "Not found: Table %s:%s.%s"
			}
		 }`, projectName, datasetName, tableName,
			projectName, datasetName, tableName)
	}
}
