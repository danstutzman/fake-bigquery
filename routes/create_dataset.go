package routes

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/danielstutzman/fake-bigquery/data"
)

type CreateDatasetRequest struct {
	DatasetReference DatasetReference `json:"datasetReference"`
}

type DatasetReference struct {
	DatasetId string `json:"datasetId"`
	ProjectId string `json:"projectId"`
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

	project, projectOk := data.Projects[projectName]
	if !projectOk {
		project = data.Project{
			Datasets: map[string]data.Dataset{},
		}
		data.Projects[projectName] = project
	}

	project.Datasets[datasetName] = data.Dataset{
		Tables: map[string]data.Table{},
	}

	// Just serve the input as output
	outputJson, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Error from Marshal: %v", err)
	}
	w.Write(outputJson)
}
