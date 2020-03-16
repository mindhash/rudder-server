package helpers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/db"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/sjson"
)

// Ginkgo tests use the following credentials
// CONFIG_BACKEND_URL=https://api.dev.rudderlabs.com
// CONFIG_BACKEND_TOKEN=1TEeQIJJqpviy5uAbWuxjk1XttY
// USERNAME=srikanth+ginkgo@rudderlabs.com
// PASSWORD=secret123

const (
	serverIP = "http://localhost:8080"
)

// EventOptsT is the type specifying override options over sample event.json
type EventOptsT struct {
	Integrations map[string]bool
	WriteKey     string
	ID           string
	MessageID    string
	GaVal        int
	ValString    string
}

type DatabasesT struct {
	Datname string
}

//RemoveKeyFromJSON returns the json with keys removed from the input json
func RemoveKeyFromJSON(json string, keys ...string) string {
	for _, key := range keys {
		var err error
		json, err = sjson.Delete(json, key)
		if err != nil {
			panic(err)
		}
	}
	return json
}

// SendEventRequest sends sample event.json with EventOptionsT overrides to gateway server
func SendEventRequest(options EventOptsT) int {
	if options.Integrations == nil {
		options.Integrations = map[string]bool{
			"All": true,
		}
	}

	//Source with WriteKey: 1Yc6YbOGg6U2E8rlj97ZdOawPyr has one S3 and one GA as destinations. Using this WriteKey as default.
	if options.WriteKey == "" {
		options.WriteKey = "1Yc6YbOGg6U2E8rlj97ZdOawPyr"
	}
	if options.ID == "" {
		options.ID = ksuid.New().String()
	}
	if options.MessageID == "" {
		options.MessageID = uuid.NewV4().String()
	}

	jsonPayload, _ := sjson.Set(BatchPayload, "batch.0.sentAt", time.Now())
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.integrations", options.Integrations)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.anonymousId", options.ID)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.messageId", options.MessageID)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.properties.value", options.GaVal)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.properties.strvalue", options.ValString)

	return SendBatchRequest(options.WriteKey, jsonPayload)
}

// SendBatchRequest sends request to /v1/batch
func SendBatchRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/batch", userNameForBasicAuth, jsonPayload)
}

// SendIdentifyRequest sends request to /v1/identify
func SendIdentifyRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/identify", userNameForBasicAuth, jsonPayload)
}

// SendTrackRequest sends request to /v1/track
func SendTrackRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/track", userNameForBasicAuth, jsonPayload)
}

// SendPageRequest sends request to /v1/page
func SendPageRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/page", userNameForBasicAuth, jsonPayload)
}

// SendScreenRequest sends request to /v1/screen
func SendScreenRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/screen", userNameForBasicAuth, jsonPayload)
}

// SendAliasRequest sends request to /v1/alias
func SendAliasRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/alias", userNameForBasicAuth, jsonPayload)
}

// SendGroupRequest sends request to /v1/group
func SendGroupRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/group", userNameForBasicAuth, jsonPayload)
}

// SendRequest sends jsonPayload to gateway server with userNameForBasicAuth in basic auth
func SendRequest(endPoint, userNameForBasicAuth, jsonPayload string) int {
	req, err := http.NewRequest("POST", serverIP+endPoint, bytes.NewBuffer([]byte(jsonPayload)))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(userNameForBasicAuth, "")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// SendHealthRequest sends health request
func SendHealthRequest() []byte {
	resp, err := http.Get(serverIP + "/health")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return body
}

// SendVersionRequest sends version request
func SendVersionRequest() []byte {
	resp, err := http.Get(serverIP + "/version")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return body
}

// SameStringSlice checks if two slices have same strings in any order
func SameStringSlice(x, y []string) bool {
	// If x is nil, then y must also be nil.
	if (x == nil) != (y == nil) {
		return false
	}

	if len(x) != len(y) {
		return false
	}

	sort.Strings(x)
	sort.Strings(y)

	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}

	return true
}

// GetTableNamesWithPrefix returns all table names with specified prefix
func GetTableNamesWithPrefix(dbHandle *sql.DB, prefix string) []string {
	//Read the table names from PG
	stmt, err := dbHandle.Prepare(`SELECT tablename
                                        FROM pg_catalog.pg_tables
                                        WHERE schemaname != 'pg_catalog' AND
                                        schemaname != 'information_schema'`)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		if err != nil {
			panic(err)
		}
		if strings.HasPrefix(tbName, prefix) {
			tableNames = append(tableNames, tbName)
		}
	}

	return tableNames
}

// GetJobsCount returns count of jobs across all tables with specified prefix
func GetJobsCount(dbHandle *sql.DB, prefix string) int {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_jobs_")
	count := 0
	for _, tableName := range tableNames {
		var jobsCount int
		dbHandle.QueryRow(fmt.Sprintf(`select count(*) as count from %s;`, tableName)).Scan(&jobsCount)
		count += jobsCount
	}
	return count
}

// GetJobsCountForSourceAndDestination returns count of jobs across all tables with specified prefix
func GetJobsCountForSourceAndDestination(dbHandle *sql.DB, prefix string, sourceID string, destinationID string) int {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_jobs_")
	count := 0
	for _, tableName := range tableNames {
		var jobsCount int
		dbHandle.QueryRow(fmt.Sprintf(`select count(*) as count from %s where ("parameters"::TEXT = '{"source_id": "%s", "destination_id": "%s"}');`, tableName, sourceID, destinationID)).Scan(&jobsCount)
		count += jobsCount
	}
	return count
}

// GetJobStatusCount returns count of job status across all tables with specified prefix
func GetJobStatusCount(dbHandle *sql.DB, jobState string, prefix string) int {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_job_status_")
	count := 0
	for _, tableName := range tableNames {
		var jobsCount int
		dbHandle.QueryRow(fmt.Sprintf(`select count(*) as count from %s where job_state='%s';`, tableName, jobState)).Scan(&jobsCount)
		count += jobsCount
	}
	return count
}

// GetLatestJobs returns jobs (with a limit) across all tables with specified prefix
func GetLatestJobs(dbHandle *sql.DB, prefix string, limit int) []*jobsdb.JobT {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_jobs_")
	var jobList []*jobsdb.JobT
	for _, tableName := range tableNames {
		rows, err := dbHandle.Query(fmt.Sprintf(`select %[1]s.job_id, %[1]s.uuid, %[1]s.parameters, %[1]s.custom_val,
		%[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at from %[1]s order by %[1]s.created_at desc, %[1]s.job_id desc limit %v;`, tableName, limit-len(jobList)))
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var job jobsdb.JobT
			rows.Scan(&job.JobID, &job.UUID, &job.Parameters, &job.CustomVal,
				&job.EventPayload, &job.CreatedAt, &job.ExpireAt)
			if len(jobList) < limit {
				jobList = append(jobList, &job)
			}
		}
		if len(jobList) >= limit {
			break
		}
	}
	return jobList
}

// GetJobStatus returns job statuses (with a limit) across all tables with specified prefix
func GetJobStatus(dbHandle *sql.DB, prefix string, limit int, jobState string) []*jobsdb.JobStatusT {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_job_status_")
	var jobStatusList []*jobsdb.JobStatusT
	for _, tableName := range tableNames {
		rows, err := dbHandle.Query(fmt.Sprintf(`select * from %[1]s where job_state='%s' order by %[1]s.created_at desc limit %v;`, tableName, jobState, limit-len(jobStatusList)))
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var jobStatus jobsdb.JobStatusT
			rows.Scan(&jobStatus)
			if len(jobStatusList) < limit {
				jobStatusList = append(jobStatusList, &jobStatus)
			}
		}
		if len(jobStatusList) >= limit {
			break
		}
	}
	return jobStatusList
}

// GetTableSize returns the size of table in MB
func GetTableSize(dbHandle *sql.DB, jobTable string) int64 {
	var tableSize int64
	sqlStatement := fmt.Sprintf(`SELECT PG_TOTAL_RELATION_SIZE('%s')`, jobTable)
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&tableSize)
	if err != nil {
		panic(err)
	}
	return tableSize
}

// GetListOfMaintenanceModeOriginalDBs returns the list of databases in the format of original_jobsdb*
func GetListOfMaintenanceModeOriginalDBs(dbHandle *sql.DB, jobsdb string) []string {
	var dbNames []string
	sqlStatement := "SELECT datname FROM pg_database where datname like 'original_" + jobsdb + "_%'"
	rows, err := dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var dbName string
		err = rows.Scan(&dbName)
		if err != nil {
			panic(err)
		}
		dbNames = append(dbNames, dbName)
	}
	return dbNames
}

// GetRecoveryData gets the recovery data from json file
func GetRecoveryData(storagePath string) db.RecoveryDataT {
	data, err := ioutil.ReadFile(storagePath)
	if os.IsNotExist(err) {
		defaultRecoveryJSON := "{\"mode\":\"" + "normal" + "\"}"
		data = []byte(defaultRecoveryJSON)
	} else {
		if err != nil {
			panic(err)
		}
	}

	var recoveryData db.RecoveryDataT
	err = json.Unmarshal(data, &recoveryData)
	if err != nil {
		panic(err)
	}

	return recoveryData
}
