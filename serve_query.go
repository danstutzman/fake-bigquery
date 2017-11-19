package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func serveQuery(w http.ResponseWriter, r *http.Request, projectName, jobId string) {
	fields := queryResultByJobId[jobId].Fields
	fieldsJson, err := json.Marshal(fields)
	if err != nil {
		log.Fatalf("Error from Marshal: %s", err)
	}

	rows := queryResultByJobId[jobId].Rows
	rowsJson, err := json.Marshal(rows)
	if err != nil {
		log.Fatalf("Error from Marshal: %s", err)
	}

	fmt.Fprintf(w, `{
		"kind": "bigquery#getQueryResultsResponse",
		"etag": "\"cX5UmbB_R-S07ii743IKGH9YCYM/wLFL5h11OCxiWY3yDLqREwltkXs\"",
		"schema": {
			"fields": %s
		},
		"jobReference": {
			"projectId": "%s",
			"jobId": "%s"
		},
		"totalRows": "1",
		"rows": %s,
		"totalBytesProcessed": "0",
		"jobComplete": true,
		"cacheHit": true
	}`, fieldsJson, projectName, jobId, rowsJson)
}
