/*
 *
Table: event_types

| id  | uuid   | write_key | ev_type  | event_identifier | created_at        |
| --- | ------ | --------- | -------- | ---------------- | ----------------- |
| 1   | uuid-1 | ksuid-1   | track    | logged_in        | 01, Jan 12: 00 PM |
| 2   | uuid-2 | ksuid-1   | track    | signed_up        | 01, Jan 12: 00 PM |
| 3   | uuid-3 | ksuid-1   | page     | Home Page        | 01, Jan 12: 00 PM |
| 4   | uuid-4 | ksuid-2   | identify |                  | 01, Jan 12: 00 PM |


Table: schema_versions

| id  | uuid   | event_id | schema_hash | schema                          | metadata | first_seen        | last_seen          |
| --- | ------ | -------- | ----------- | ------------------------------- | -------- | ----------------- | ------------------ |
| 1   | uuid-9 | uuid-1   | hash-1      | {"a": "string", "b": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 2   | uuid-8 | uuid-2   | hash-2      | {"a": "string", "b": "string"}  | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 3   | uuid-7 | uuid-3   | hash-3      | {"a": "string", "c": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 4   | uuid-6 | uuid-2   | hash-1      | {"a": "string", "b": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |

*/

package protocols

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jeremywohl/flatten"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

// EventTypeT is a struct that represents EVENT_TYPES_TABLE
type EventTypeT struct {
	ID              int
	UUID            string `json:"eventID"`
	WriteKey        string `json:"writeKey"`
	EvType          string `json:"eventType"`
	EventIdentifier string `json:"eventIdentifier"`
	CreatedAt       time.Time
}

// SchemaVersionT is a struct that represents SCHEMA_VERSIONS_TABLE
type SchemaVersionT struct {
	ID         int64
	UUID       string `json:"versionID"`
	SchemaHash string `json:"-"`
	EventID    string
	Schema     json.RawMessage
	Metadata   json.RawMessage
	FirstSeen  time.Time
	LastSeen   time.Time
	EventType  *EventTypeT `json:"-"`
}

//EventTypeIDMapT :	event_type's uuid to EventType Mapping
type EventTypeIDMapT map[string]*EventTypeT

//EventTypeMapT : <writeKey, evType, eventIdentifier> to EventType Mapping
type EventTypeMapT map[string]map[string]map[string]*EventTypeT

//SchemaVersionMapT : <event_id, schema_hash> to SchemaVersion Mapping
type SchemaVersionMapT map[string]map[string]*SchemaVersionT

// ProtocolManagerT handles all protocols related features
type ProtocolManagerT struct {
	dbHandle          *sql.DB
	eventTypeIDMap    EventTypeIDMapT
	eventTypeMap      EventTypeMapT
	schemaVersionMap  SchemaVersionMapT
	eventTypeLock     sync.RWMutex
	schemaVersionLock sync.RWMutex
}

var enableProtocols bool
var flushInterval time.Duration
var adminUser string
var adminPassword string

const EVENT_TYPES_TABLE = "event_types"
const SCHEMA_VERSIONS_TABLE = "schema_versions"

var eventSchemaChannel chan *GatewayEventBatchT

var newEventTypes map[string]*EventTypeT
var newSchemaVersions map[string]*SchemaVersionT
var dirtySchemaVersions map[string]*SchemaVersionT

type GatewayEventBatchT struct {
	writeKey   string
	eventBatch string
}

//EventT : Generic type for singular event
type EventT map[string]interface{}

//EventPayloadT : Generic type for gateway event payload
type EventPayloadT struct {
	WriteKey   string
	ReceivedAt string
	Batch      []EventT
}

func loadConfig() {
	enableProtocols = config.GetBool("Protocols.enabled", true)
	flushInterval = config.GetDuration("Protocols.syncIntervalInS", 5) * time.Second
	adminUser = config.GetEnv("RUDDER_ADMIN_USER", "rudder")
	adminPassword = config.GetEnv("RUDDER_ADMIN_PASSWORD", "rudderstack")

	if adminPassword == "rudderstack" {
		fmt.Println("[Protocols] You are using default password. Please change it by setting env variable RUDDER_ADMIN_PASSWORD")
	}
}

func init() {
	loadConfig()
}

//RecordEventSchema : Records event schema for every event in the batch
func (manager *ProtocolManagerT) RecordEventSchema(writeKey string, eventBatch string) bool {
	if !enableProtocols {
		return false
	}

	eventSchemaChannel <- &GatewayEventBatchT{writeKey, eventBatch}
	return true
}

func (manager *ProtocolManagerT) getEventType(writeKey string, evType string, eventIdentifier string) *EventTypeT {
	_, ok := manager.eventTypeMap[writeKey]
	if !ok {
		return nil
	}
	_, ok = manager.eventTypeMap[writeKey][evType]
	if !ok {
		return nil
	}
	return manager.eventTypeMap[writeKey][evType][eventIdentifier]
}

/*
 *
| Event Type | ev_type  | event_identfier |
| ---------- | -------- | --------------- |
| track      | track    | event["event"]  |
| page       | page     | event["name"]   |
| screen     | screen   | event["name"]   |
| identify   | identify | ""              |
| alias      | alias    | ""              |
| group      | group    | ""              |
*
* All event types and schema versions are generated by grouping according to the table above.
* Eg:
*    <track, login> will always be of same event_type. Different payloads will result in different schema_versions
*    <track, login> will always be of same event_type. Different payloads will result in different schema_versions
*    <page, home-page> will always be of same event_type. Different payloads will result in different schema_versions
*    <identify> There will be only identify event_type per source. Schema versions can change with different traits.
*
* This function is goroutine-safe. We can scale multiple go-routines calling this function,
* but since this method does mostly in-memory operations and has locks, there might not be much perfomance improvement.
*/
func (manager *ProtocolManagerT) handleEvent(writeKey string, event EventT) {
	evType := event["type"].(string)
	eventIdentifier := ""
	if evType == "track" {
		eventIdentifier = event["event"].(string)
	} else if evType == "page" {
		eventIdentifier = event["name"].(string)
	} else if evType == "screen" {
		eventIdentifier = event["name"].(string)
	}

	//TODO: Review the concurrency by scaling goroutines
	manager.eventTypeLock.RLock()
	eventType := manager.getEventType(writeKey, evType, eventIdentifier)
	manager.eventTypeLock.RUnlock()
	if eventType == nil {
		eventID := uuid.NewV4().String()
		eventType = &EventTypeT{
			UUID:            eventID,
			WriteKey:        writeKey,
			EvType:          evType,
			EventIdentifier: eventIdentifier,
		}

		manager.eventTypeLock.Lock()
		newEventTypes[eventID] = eventType
		_, ok := manager.eventTypeMap[writeKey]
		if !ok {
			manager.eventTypeMap[writeKey] = make(map[string]map[string]*EventTypeT)
		}
		_, ok = manager.eventTypeMap[writeKey][evType]
		if !ok {
			manager.eventTypeMap[writeKey][evType] = make(map[string]*EventTypeT)
		}
		manager.eventTypeMap[writeKey][evType][eventIdentifier] = eventType
		manager.eventTypeIDMap[eventID] = eventType
		manager.eventTypeLock.Unlock()
	}

	schemaHash, schema := computeVersion(event)
	manager.schemaVersionLock.Lock()
	_, ok := manager.schemaVersionMap[eventType.UUID]
	if !ok {
		manager.schemaVersionMap[eventType.UUID] = make(map[string]*SchemaVersionT)
	}
	schemaVersion, ok := manager.schemaVersionMap[eventType.UUID][schemaHash]
	manager.schemaVersionLock.Unlock()

	if !ok {
		schemaJSON, err := json.Marshal(schema)
		assertError(err)
		versionID := uuid.NewV4().String()
		schemaVersion = &SchemaVersionT{
			UUID:       versionID,
			SchemaHash: schemaHash,
			EventID:    eventType.UUID,
			Schema:     schemaJSON,
			EventType:  eventType,
		}
		manager.schemaVersionLock.Lock()
		newSchemaVersions[versionID] = schemaVersion
		manager.schemaVersionMap[eventType.UUID][schemaHash] = schemaVersion
		manager.schemaVersionLock.Unlock()
	} else {
		manager.schemaVersionLock.Lock()
		_, ok := newSchemaVersions[schemaVersion.UUID]
		// If not present in newSchemaVersions, add it to dirty
		if !ok {
			dirtySchemaVersions[schemaVersion.UUID] = schemaVersion
		}
		manager.schemaVersionLock.Unlock()
	}
}

func (manager *ProtocolManagerT) recordEvents() {
	for gatewayEventBatch := range eventSchemaChannel {
		var eventPayload EventPayloadT
		err := json.Unmarshal([]byte(gatewayEventBatch.eventBatch), &eventPayload)
		assertError(err)
		for _, event := range eventPayload.Batch {
			manager.handleEvent(eventPayload.WriteKey, event)
		}
	}
}

func (manager *ProtocolManagerT) flushEventSchemas() {
	// This will run forever. If you want to quit in between, change it to ticker and call stop()
	// Otherwise the ticker won't be GC'ed
	ticker := time.Tick(flushInterval)
	for range ticker {

		// If needed, copy the maps and release the lock immediately
		manager.eventTypeLock.Lock()
		manager.schemaVersionLock.Lock()

		if len(newEventTypes) == 0 && len(newSchemaVersions) == 0 && len(dirtySchemaVersions) == 0 {
			manager.eventTypeLock.Unlock()
			manager.schemaVersionLock.Unlock()
			continue
		}

		txn, err := manager.dbHandle.Begin()
		assertError(err)

		if len(newEventTypes) > 0 {
			stmt, err := txn.Prepare(pq.CopyIn(EVENT_TYPES_TABLE, "uuid", "write_key", "ev_type", "event_identifier"))
			assertTxnError(err, txn)
			defer stmt.Close()
			for eventID, eventType := range newEventTypes {
				_, err = stmt.Exec(eventID, eventType.WriteKey, eventType.EvType, eventType.EventIdentifier)
				assertTxnError(err, txn)
			}
			_, err = stmt.Exec()
			assertTxnError(err, txn)
			logger.Debugf("[Protocols][Flush] %d new event types", len(newEventTypes))
		}

		if len(newSchemaVersions) > 0 {
			stmt, err := txn.Prepare(pq.CopyIn(SCHEMA_VERSIONS_TABLE, "uuid", "event_id", "schema_hash", "schema", "last_seen"))
			assertTxnError(err, txn)
			defer stmt.Close()
			for versionID, schemaVersion := range newSchemaVersions {
				_, err = stmt.Exec(versionID, schemaVersion.EventID, schemaVersion.SchemaHash, string(schemaVersion.Schema), "now()")
				assertTxnError(err, txn)
			}
			_, err = stmt.Exec()
			assertTxnError(err, txn)
			logger.Debugf("[Protocols][Flush] %d new schema versions", len(newSchemaVersions))
		}

		// To improve efficiency, making 1 query for all last_seen timestamps
		// Since the flush interval is short (i.e., 5 sec), this should not be a problem
		if len(dirtySchemaVersions) > 0 {
			versionIDs := make([]string, 0, len(dirtySchemaVersions))
			for versionID, _ := range dirtySchemaVersions {
				versionIDs = append(versionIDs, versionID)
			}
			updateLastSeenSQL := fmt.Sprintf(`UPDATE %s SET last_seen = now() WHERE uuid IN ('%s')`, SCHEMA_VERSIONS_TABLE, strings.Join(versionIDs, "', '"))
			_, err := txn.Exec(updateLastSeenSQL)
			assertTxnError(err, txn)
			logger.Debugf("[Protocols][Flush] %d last_seen updates", len(dirtySchemaVersions))
		}

		err = txn.Commit()
		assertError(err)

		newEventTypes = make(map[string]*EventTypeT)
		newSchemaVersions = make(map[string]*SchemaVersionT)
		dirtySchemaVersions = make(map[string]*SchemaVersionT)

		manager.schemaVersionLock.Unlock()
		manager.eventTypeLock.Unlock()
	}
}

// TODO: Move this into some DB manager
func createDBConnection() *sql.DB {
	psqlInfo := jobsdb.GetConnectionString()
	var err error
	dbHandle, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	err = dbHandle.Ping()
	if err != nil {
		panic(err)
	}
	return dbHandle
}

func assertError(err error) {
	if err != nil {
		panic(err)
	}
}

func assertTxnError(err error, txn *sql.Tx) {
	if err != nil {
		txn.Rollback()
		panic(err)
	}
}

func (manager *ProtocolManagerT) fetchEventTypesByWriteKey(writeKey string) []*EventTypeT {
	var eventTypesSelectSQL string
	if writeKey == "" {
		eventTypesSelectSQL = fmt.Sprintf(`SELECT * FROM %s`, EVENT_TYPES_TABLE)
	} else {
		eventTypesSelectSQL = fmt.Sprintf(`SELECT * FROM %s WHERE write_key = '%s'`, EVENT_TYPES_TABLE, writeKey)
	}

	rows, err := manager.dbHandle.Query(eventTypesSelectSQL)
	assertError(err)
	defer rows.Close()

	eventTypes := make([]*EventTypeT, 0)

	for rows.Next() {
		var eventType EventTypeT
		err := rows.Scan(&eventType.ID, &eventType.UUID, &eventType.WriteKey, &eventType.EvType,
			&eventType.EventIdentifier, &eventType.CreatedAt)
		assertError(err)

		eventTypes = append(eventTypes, &eventType)
	}

	return eventTypes
}

func (manager *ProtocolManagerT) populateEventTypes() (EventTypeIDMapT, EventTypeMapT) {
	eventTypeIDMap := make(EventTypeIDMapT)
	eventTypeMap := make(EventTypeMapT)

	eventTypesSelectSQL := fmt.Sprintf(`SELECT * FROM %s`, EVENT_TYPES_TABLE)

	rows, err := manager.dbHandle.Query(eventTypesSelectSQL)
	assertError(err)
	defer rows.Close()

	for rows.Next() {
		var eventType EventTypeT
		err := rows.Scan(&eventType.ID, &eventType.UUID, &eventType.WriteKey, &eventType.EvType,
			&eventType.EventIdentifier, &eventType.CreatedAt)

		assertError(err)
		eventTypeIDMap[eventType.UUID] = &eventType
		_, ok := eventTypeMap[eventType.WriteKey]
		if !ok {
			eventTypeMap[eventType.WriteKey] = make(map[string]map[string]*EventTypeT)
		}
		_, ok = eventTypeMap[eventType.WriteKey][eventType.EvType]
		if !ok {
			eventTypeMap[eventType.WriteKey][eventType.EvType] = make(map[string]*EventTypeT)
		}
		eventTypeMap[eventType.WriteKey][eventType.EvType][eventType.EventIdentifier] = &eventType
	}

	return eventTypeIDMap, eventTypeMap
}

func (manager *ProtocolManagerT) fetchSchemaVersionsByEventID(eventID string) []*SchemaVersionT {
	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT * FROM %s WHERE event_id = '%s'`, SCHEMA_VERSIONS_TABLE, eventID)

	rows, err := manager.dbHandle.Query(schemaVersionsSelectSQL)
	assertError(err)
	defer rows.Close()

	schemaVersions := make([]*SchemaVersionT, 0)

	for rows.Next() {
		var schemaVersion SchemaVersionT
		err := rows.Scan(&schemaVersion.ID, &schemaVersion.UUID, &schemaVersion.EventID, &schemaVersion.SchemaHash,
			&schemaVersion.Schema, &schemaVersion.Metadata, &schemaVersion.FirstSeen, &schemaVersion.LastSeen)
		assertError(err)

		schemaVersions = append(schemaVersions, &schemaVersion)
	}

	return schemaVersions
}

func (manager *ProtocolManagerT) populateSchemaVersions(eventTypeIDMap EventTypeIDMapT) SchemaVersionMapT {

	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT * FROM %s`, SCHEMA_VERSIONS_TABLE)

	rows, err := manager.dbHandle.Query(schemaVersionsSelectSQL)
	assertError(err)
	defer rows.Close()

	schemaVersionMap := make(map[string]map[string]*SchemaVersionT)

	for rows.Next() {
		var schemaVersion SchemaVersionT
		err := rows.Scan(&schemaVersion.ID, &schemaVersion.UUID, &schemaVersion.EventID, &schemaVersion.SchemaHash,
			&schemaVersion.Schema, &schemaVersion.Metadata, &schemaVersion.FirstSeen, &schemaVersion.LastSeen)
		assertError(err)

		schemaVersion.EventType = eventTypeIDMap[schemaVersion.EventID]
		_, ok := schemaVersionMap[schemaVersion.EventID]
		if !ok {
			schemaVersionMap[schemaVersion.EventID] = make(map[string]*SchemaVersionT)
		}
		schemaVersionMap[schemaVersion.EventID][schemaVersion.SchemaHash] = &schemaVersion
	}

	return schemaVersionMap
}

// This should be called during the Setup() to populate existing event Schemas
func (manager *ProtocolManagerT) populateEventSchemas() {
	manager.eventTypeIDMap, manager.eventTypeMap = manager.populateEventTypes()
	manager.schemaVersionMap = manager.populateSchemaVersions(manager.eventTypeIDMap)
}

//TODO: Support for prefix based
func computeVersion(event EventT) (schemaHash string, schema map[string]string) {

	eventMap := map[string]interface{}(event)

	flattenedEvent, err := flatten.Flatten((eventMap), "", flatten.DotStyle)

	if err != nil {
		fmt.Println(err)
		panic("Failed to flatten the event")
	} else {
		fmt.Println(flattenedEvent)
		finalSchema := make(map[string]string)
		keys := make([]string, 0, len(finalSchema))
		for k, v := range flattenedEvent {
			keys = append(keys, k)
			reflectType := reflect.TypeOf(v)
			finalSchema[k] = reflectType.String()
		}
		fmt.Println(finalSchema)
		sort.Strings(keys)

		var sb strings.Builder
		for _, k := range keys {
			sb.WriteString(k)
			sb.WriteString(":")
			sb.WriteString(finalSchema[k])
			sb.WriteString(",")
		}
		return misc.GetMD5Hash(sb.String()), finalSchema
	}
}

//TODO: Use Migrations library
func (manager *ProtocolManagerT) setupTables() {
	createEventTypesSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		uuid VARCHAR(36) NOT NULL,
		write_key VARCHAR(32) NOT NULL,
		ev_type TEXT NOT NULL,
		event_identifier TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMP NOT NULL DEFAULT NOW()
	)
	`, EVENT_TYPES_TABLE)

	_, err := manager.dbHandle.Exec(createEventTypesSQL)
	assertError(err)

	createWriteKeyIndexSQL := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS write_key_index ON %s (write_key)`, EVENT_TYPES_TABLE)
	_, err = manager.dbHandle.Exec(createWriteKeyIndexSQL)
	assertError(err)

	createSchemaVersionsSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGSERIAL PRIMARY KEY,
		uuid VARCHAR(36) NOT NULL, 
		event_id VARCHAR(36) NOT NULL,
		schema_hash VARCHAR(32) NOT NULL,
		schema JSONB NOT NULL,
		metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
		first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
		last_seen TIMESTAMP NOT NULL DEFAULT NOW()
	)
	`, SCHEMA_VERSIONS_TABLE)

	_, err = manager.dbHandle.Exec(createSchemaVersionsSQL)
	assertError(err)

	createEventIDIndexSQL := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS event_id_index ON %s (event_id)`, SCHEMA_VERSIONS_TABLE)
	_, err = manager.dbHandle.Exec(createEventIDIndexSQL)
	assertError(err)

	createUniqueIndexSQL := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS event_id_schema_hash_index ON %s (event_id, schema_hash)`, SCHEMA_VERSIONS_TABLE)
	_, err = manager.dbHandle.Exec(createUniqueIndexSQL)
	assertError(err)
}

func (manager *ProtocolManagerT) Setup() {

	if !enableProtocols {
		logger.Info("[Protocols] Feature is disabled.")
		return
	}

	logger.Info("[Protocols] Setting up protocols...")
	// Clean this up
	manager.dbHandle = createDBConnection()

	// Following data structures store events and schemas since last flush
	newEventTypes = make(map[string]*EventTypeT)
	newSchemaVersions = make(map[string]*SchemaVersionT)
	dirtySchemaVersions = make(map[string]*SchemaVersionT)

	manager.setupTables()
	manager.populateEventSchemas()
	eventSchemaChannel = make(chan *GatewayEventBatchT, 1000)

	rruntime.Go(func() {
		manager.recordEvents()
	})

	rruntime.Go(func() {
		manager.flushEventSchemas()
	})

	logger.Info("[Protocols] Set up protocols successful.")
}

/*
 * Handling HTTP requests to expose the schemas
 *
 */

func handleBasicAuth(r *http.Request) error {
	username, password, ok := r.BasicAuth()
	if !ok {
		return fmt.Errorf("Basic auth credentials missing")
	}
	if username != adminUser || password != adminPassword {
		return fmt.Errorf("Invalid admin credentials")
	}
	return nil
}

func (manager *ProtocolManagerT) GetEventTypes(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	writeKeys, ok := r.URL.Query()["writeKey"]
	writeKey := ""
	if ok && writeKeys[0] != "" {
		writeKey = writeKeys[0]
	}

	eventTypes := manager.fetchEventTypesByWriteKey(writeKey)

	eventTypesJSON, err := json.Marshal(eventTypes)
	if err != nil {
		http.Error(w, "Internal Error: Failed to Marshal event types", 500)
		return
	}

	w.Write(eventTypesJSON)
}

func (manager *ProtocolManagerT) GetEventVersions(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	eventIDs, ok := r.URL.Query()["eventID"]
	if !ok {
		http.Error(w, "Mandatory field: eventID missing", 400)
		return
	}
	eventID := eventIDs[0]

	schemaVersions := manager.fetchSchemaVersionsByEventID(eventID)
	schemaVersionsJSON, err := json.Marshal(schemaVersions)
	if err != nil {
		http.Error(w, "Internal Error: Failed to Marshal event types", 500)
		return
	}

	w.Write(schemaVersionsJSON)
}
