## Supported features

* `SELECT COUNT(*) FROM dataset.tablename`
* `SELECT * FROM dataset.tablename LIMIT n`

## Example usage

* `curl https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest > discovery.json`
* `go install .`
* `$GOPATH/bin/fake-bigquery -discovery-json-path discovery.json -port 9090`
* `bq --api http://localhost:9090 mk mydataset`
* `bq --api http://localhost:9090 ls`
* `bq --api http://localhost:9090 mk mydataset.mytable`
* `bq --api http://localhost:9090 ls mydataset`
* `bq --api http://localhost:9090 query 'select count(*) from mydataset.mytable'`
