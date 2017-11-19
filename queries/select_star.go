package queries

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/danielstutzman/fake-bigquery/data"
)

func executeSelectStar(projectName, datasetName, tableName string, limit int,
	projects map[string]data.Project) *data.Result {

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
}
