package routes

import (
	"log"
	"net/http"
	"regexp"
)

var DATASETS_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/([^/]*)/datasets$")
var DATASET_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/([^/]*)/datasets/([^/]*)$")
var TABLES_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/([^/]*)/datasets/([^/]*)/tables$")
var TABLE_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/([^/]*)/datasets/([^/]*)/tables/([^/]*)$")
var JOBS_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/([^/]*)/jobs$")
var QUERY_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/([^/]*)/queries/([^/]*)$")
var INSERT_REGEXP = regexp.MustCompile("^(/bigquery/v2)?/projects/([^/]*)/datasets/([^/]*)/tables/([^/]*)/insertAll")

func Route(w http.ResponseWriter, r *http.Request, discoveryJson []byte) {
	path := r.URL.Path
	log.Printf("Incoming path: %s", path)

	if path == "/discovery/v1/apis/bigquery/v2/rest" {
		w.Write(discoveryJson)
	} else if match := DATASET_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		dataset := match[3]
		if r.Method == "GET" {
			checkDatasetExistence(w, r, project, dataset)
		} else {
			log.Fatalf("Unexpected method: %s", r.Method)
		}
	} else if match := DATASETS_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		if r.Method == "GET" {
			listDatasets(w, r, project)
		} else if r.Method == "POST" {
			createDataset(w, r, project)
		} else {
			log.Fatalf("Unexpected method: %s", r.Method)
		}
	} else if match := TABLES_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		dataset := match[3]
		if r.Method == "GET" {
			listTables(w, r, project, dataset)
		} else if r.Method == "POST" {
			createTable(w, r, project, dataset)
		} else {
			log.Fatalf("Unexpected method: %s", r.Method)
		}
	} else if match := TABLE_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		dataset := match[3]
		table := match[4]
		if r.Method == "GET" {
			checkTableExistence(w, r, project, dataset, table)
		} else {
			log.Fatalf("Unexpected method: %s", r.Method)
		}
	} else if match := JOBS_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		if r.Method == "POST" {
			createJob(w, r, project)
		} else {
			log.Fatalf("Unexpected method: %s", r.Method)
		}
	} else if match := QUERY_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		jobId := match[3]
		serveQuery(w, r, project, jobId)
	} else if match := INSERT_REGEXP.FindStringSubmatch(path); match != nil {
		project := match[2]
		dataset := match[3]
		table := match[4]
		insertRows(w, r, project, dataset, table)
	} else {
		log.Fatalf("Don't know how to serve path %s", r.URL.Path)
	}
}
