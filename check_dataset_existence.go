package main

import (
	"fmt"
	"net/http"
)

func checkDatasetExistence(w http.ResponseWriter, r *http.Request, projectName, datasetName string) {
	project, projectOk := projects[projectName]
	if !projectOk {
		project = Project{Datasets: map[string]Dataset{}}
		projects[projectName] = project
	}

	_, datasetExists := project.Datasets[datasetName]
	if datasetExists {
		fmt.Fprintf(w, `{
			"kind": "bigquery#dataset",
			"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/MTUxMTExNTg0MDU2Ng\"",
			"id": "%s:%s",
			"selfLink": "https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s",
			"datasetReference": {
				"projectId": "%s",
				"datasetId": "%s"
			}
		}`, projectName, datasetName, projectName, datasetName, projectName, datasetName)
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
						"message": "Not found: Dataset %s:%s"
					}
				],
				"code": 404,
				"message": "Not found: Dataset %s:%s"
			}
		}
`, projectName, datasetName, projectName, datasetName)
	}
}
