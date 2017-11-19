package routes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/danielstutzman/fake-bigquery/data"
)

func (app *App) listDatasets(w http.ResponseWriter, r *http.Request, projectName string) {
	project, projectOk := app.projects[projectName]
	if !projectOk {
		project = data.Project{
			Datasets: map[string]data.Dataset{},
		}
		app.projects[projectName] = project
	}

	datasetOutputs := []map[string]interface{}{}
	for datasetName := range project.Datasets {
		datasetOutput := map[string]interface{}{
			"kind": "bigquery#dataset",
			"id":   fmt.Sprintf("%s:%s", projectName, datasetName),
			"datasetReference": map[string]string{
				"projectId": projectName,
				"datasetId": datasetName,
			},
		}
		datasetOutputs = append(datasetOutputs, datasetOutput)
	}

	datasetOutputsJson, err := json.Marshal(datasetOutputs)
	if err != nil {
		log.Fatalf("Error from Marshal: %v", err)
	}

	fmt.Fprintf(w, `{
		"kind": "bigquery#datasetList",
		"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/qwnfLrlOKTXd94DjXLYMd9AnLA8\"",
		"datasets": %s
	 }`, datasetOutputsJson)
}
