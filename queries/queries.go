package queries

import (
	"log"
	"regexp"
	"strconv"

	"github.com/danielstutzman/fake-bigquery/data"
)

var SELECT_COUNT_STAR_REGEXP = regexp.MustCompile(
	`(?i)^SELECT COUNT\(\*\) FROM ([^.]+).([^\s]+)$`)
var SELECT_STAR_REGEXP = regexp.MustCompile(
	`(?i)^SELECT \* FROM ([^.]+).([^\s]+)( LIMIT ([0-9])+)?$`)

func ExecuteQuery(query string, projects map[string]data.Project,
	projectName string) *data.Result {

	if match := SELECT_COUNT_STAR_REGEXP.FindStringSubmatch(query); match != nil {
		datasetName := match[1]
		tableName := match[2]
		return executeSelectCountStar(projectName, datasetName, tableName, projects)

	} else if match := SELECT_STAR_REGEXP.FindStringSubmatch(query); match != nil {
		datasetName := match[1]
		tableName := match[2]
		limit := -1
		if match[3] != "" {
			var err error
			limit, err = strconv.Atoi(match[4])
			if err != nil {
				log.Fatalf("Error from Atoi: %s", err)
			}
		}
		return executeSelectStar(projectName, datasetName, tableName, limit, projects)

	} else {
		log.Fatalf("Can't parse query: %s", query)
		return nil
	}
}
