package routes

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/danielstutzman/fake-bigquery/queries"
)

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
	result := queries.ExecuteQuery(query, app.projects, projectName)

	jobId := body.JobReference.JobId
	app.queryResultByJobId[jobId] = *result

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
