package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func listDatasets(w http.ResponseWriter, r *http.Request, projectName string) {
	project, projectOk := projects[projectName]
	if !projectOk {
		project = Project{Datasets: map[string]Dataset{}}
		projects[projectName] = project
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
