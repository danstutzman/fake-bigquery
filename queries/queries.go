package queries

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/danielstutzman/fake-bigquery/data"
)

var SELECT_COUNT_STAR_REGEXP = regexp.MustCompile(`(?i)^SELECT COUNT\(\*\) FROM ([^.]+).([^\s]+)$`)
var SELECT_STAR_REGEXP = regexp.MustCompile(`(?i)^SELECT \* FROM ([^.]+).([^\s]+)( LIMIT ([0-9])+)?$`)

func ExecuteQuery(query string, projects map[string]data.Project,
	projectName string) *data.Result {

	if match := SELECT_COUNT_STAR_REGEXP.FindStringSubmatch(query); match != nil {
		datasetName := match[1]
		tableName := match[2]

		project, projectOk := projects[projectName]
		if !projectOk {
			log.Fatalf("Unknown project: %s", projectName)
		}

		dataset, datasetOk := project.Datasets[datasetName]
		if !datasetOk {
			log.Fatalf("Unknown dataset: %s", dataset)
		}

		table, tableOk := dataset.Tables[tableName]
		if !tableOk {
			log.Fatalf("Unknown table: %s", tableName)
		}

		numRows := len(table.Rows)
		numRowsString := fmt.Sprintf("%d", numRows)
		return &data.Result{
			Fields: []data.Field{
				data.Field{
					Name: "f0_",
					Type: "INTEGER",
					Mode: "NULLABLE",
				},
			},
			Rows: []data.ResultRow{
				data.ResultRow{
					Values: []data.ResultValue{
						data.ResultValue{
							Value: &numRowsString,
						},
					},
				},
			},
		}

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

		project, projectOk := projects[projectName]
		if !projectOk {
			log.Fatalf("Unknown project: %s", projectName)
		}

		dataset, datasetOk := project.Datasets[datasetName]
		if !datasetOk {
			log.Fatalf("Unknown dataset: %s", dataset)
		}

		table, tableOk := dataset.Tables[tableName]
		if !tableOk {
			log.Fatalf("Unknown table: %s", tableName)
		}

		fromRows := table.Rows
		if limit != -1 {
			fromRows = fromRows[0:limit]
		}

		var result data.Result
		result.Fields = make([]data.Field, len(table.Fields))
		result.Rows = make([]data.ResultRow, 0)

		copy(result.Fields, table.Fields)
		for _, fromRow := range fromRows {
			resultValues := []data.ResultValue{}
			for _, field := range table.Fields {
				value := fromRow[field.Name]

				var resultValue data.ResultValue
				if value != nil {
					var valueString string
					if valueFloat64, ok := value.(float64); ok {
						valueString = strconv.FormatFloat(valueFloat64, 'f', -1, 64)
					} else if field.Type == "TIMESTAMP" {
						valueString = fmt.Sprintf("%d", value.(time.Time).Unix())
					} else {
						valueString = fmt.Sprintf("%s", value)
					}
					resultValue = data.ResultValue{Value: &valueString}
				}

				resultValues = append(resultValues, resultValue)
			}
			result.Rows = append(result.Rows, data.ResultRow{
				Values: resultValues,
			})
		}
		return &result

	} else {
		log.Fatalf("Can't parse query: %s", query)
		return nil
	}

}
