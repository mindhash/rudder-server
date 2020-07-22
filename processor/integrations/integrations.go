package integrations

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/tidwall/gjson"
)

var (
	destTransformURL, userTransformURL string
	customDestination                  []string
	whSchemaVersion                    string
)

func init() {
	loadConfig()
}

func loadConfig() {
	destTransformURL = config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
}

const (
	//PostDataKV means post data is sent as KV
	PostDataKV = iota + 1
	//PostDataJSON means post data is sent as JSON
	PostDataJSON
	//PostDataXML means post data is sent as XML
	PostDataXML
)

// PostParameterT emulates parameters needed tp make a request
type PostParameterT struct {
	Type          string      `json:"type"`
	URL           string      `json:"endpoint"`
	RequestMethod string      `json:"method"`
	UserID        string      `json:"userId"`
	Headers       interface{} `json:"headers"`
	QueryParams   interface{} `json:"params"`
	Body          interface{} `json:"body"`
	Files         interface{} `json:"files"`
}

// GetPostInfo parses the transformer response
func GetPostInfo(transformRaw json.RawMessage) (PostParameterT, error) {
	var postInfo PostParameterT
	err := json.Unmarshal(transformRaw, &postInfo)
	if err != nil {
		panic(err) //TODO: Handle error
	}
	return postInfo, nil
}

// GetUserIDFromTransformerResponse parses the payload to get userId
func GetUserIDFromTransformerResponse(transformRaw json.RawMessage) (userID string, found bool) {
	parsedJSON := gjson.ParseBytes(transformRaw)
	userIDVal := parsedJSON.Get("userId").Value()

	if userIDVal != nil {
		return fmt.Sprintf("%v", userIDVal), true
	}
	return
}

//FilterClientIntegrations parses the destination names from the
//input JSON, matches them with enabled destinations from controle plane and returns the IDSs
func FilterClientIntegrations(clientEvent types.SingularEventT, destNameIDMap map[string]backendconfig.DestinationDefinitionT) (retVal []string) {
	clientIntgs, ok := misc.GetRudderEventVal("integrations", clientEvent)
	if !ok {
		clientIntgs = make(map[string]interface{})
	}
	clientIntgsList, ok := clientIntgs.(map[string]interface{})
	if !ok {
		return
	}
	var outVal []string
	for dest := range destNameIDMap {
		if clientIntgsList[dest] == false {
			continue
		}
		if (clientIntgsList["All"] != false) || clientIntgsList[dest] == true {
			outVal = append(outVal, destNameIDMap[dest].Name)
		}
	}
	retVal = outVal
	return
}

//GetDestinationURL returns node URL
func GetDestinationURL(destType string) string {
	destinationEndPoint := fmt.Sprintf("%s/v0/%s", destTransformURL, strings.ToLower(destType))
	if misc.Contains(warehouse.WarehouseDestinations, destType) {
		whSchemaVersionQueryParam := fmt.Sprintf("whSchemaVersion=%s", config.GetWHSchemaVersion())
		if destType == "RS" {
			rsAlterStringToTextQueryParam := fmt.Sprintf("rsAlterStringToText=%s", fmt.Sprintf("%v", config.GetVarCharMaxForRS()))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + rsAlterStringToTextQueryParam
		}
		return destinationEndPoint + "?" + whSchemaVersionQueryParam
	}
	return destinationEndPoint

}

//GetUserTransformURL returns the port of running user transform
func GetUserTransformURL(processSessions bool) string {
	if processSessions {
		return destTransformURL + "/customTransform?processSessions=true"
	}
	return destTransformURL + "/customTransform"
}
