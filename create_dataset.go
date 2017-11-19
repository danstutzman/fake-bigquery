package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type CreateDatasetRequest struct {
	DatasetReference DatasetReference `json:"datasetReference"`
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

	project.Datasets[datasetName] = Dataset{
		Tables: map[string]Table{},
	}

	// Just serve the input as output
	outputJson, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Error from Marshal: %v", err)
	}
	w.Write(outputJson)
}

func listTables(w http.ResponseWriter, r *http.Request, projectName, datasetName string) {
	project, projectOk := projects[projectName]
	if !projectOk {
		project = Project{Datasets: map[string]Dataset{}}
		projects[projectName] = project
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
