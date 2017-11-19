package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type CreateTableRequest struct {
	TableReference TableReference `json:"tableReference"`
	Schema         Schema         `json:"schema"`
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

	fieldsCopy := make([]Field, len(body.Schema.Fields))
	copy(fieldsCopy, body.Schema.Fields)
	dataset.Tables[tableName] = Table{
		Fields: fieldsCopy,
		Rows:   []map[string]interface{}{},
	}

	// Just serve the input as output
	outputJson, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Error from Marshal: %v", err)
	}
	w.Write(outputJson)
}
