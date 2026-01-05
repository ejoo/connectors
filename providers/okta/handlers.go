package okta

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/common/urlbuilder"
	"github.com/amp-labs/connectors/internal/datautils"
	"github.com/amp-labs/connectors/internal/httpkit"
	"github.com/amp-labs/connectors/internal/jsonquery"
	"github.com/amp-labs/connectors/providers/okta/metadata"
	"github.com/spyzhov/ajson"
)

const (
	limitKey   = "limit"
	limitValue = "200" // Okta maximum per page
	filterKey  = "filter"
	sinceKey   = "since"
)

//nolint:gochecknoglobals
var objectsWithLastUpdatedFilter = datautils.NewStringSet(
	"users",
	"groups",
	"apps",
)

func responseField(objectName string) string {
	if objectName == "domains" {
		return "domains"
	}

	return ""
}

func (c *Connector) buildReadRequest(ctx context.Context, params common.ReadParams) (*http.Request, error) {
	if params.NextPage != "" {
		return http.NewRequestWithContext(ctx, http.MethodGet, params.NextPage.String(), nil)
	}

	path, err := metadata.Schemas.LookupURLPath(c.ProviderContext.Module(), params.ObjectName)
	if err != nil {
		return nil, err
	}

	url, err := urlbuilder.New(c.ProviderInfo().BaseURL, path)
	if err != nil {
		return nil, err
	}

	url.WithQueryParam(limitKey, limitValue)

	if !params.Since.IsZero() {
		if params.ObjectName == "logs" {
			url.WithQueryParam(sinceKey, datautils.Time.FormatRFC3339inUTC(params.Since))
		} else if objectsWithLastUpdatedFilter.Has(params.ObjectName) {
			filterValue := "lastUpdated gt \"" + datautils.Time.FormatRFC3339inUTC(params.Since) + "\""
			url.WithQueryParam(filterKey, filterValue)
		}
	}

	return http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
}

func (c *Connector) parseReadResponse(
	ctx context.Context,
	params common.ReadParams,
	request *http.Request,
	response *common.JSONHTTPResponse,
) (*common.ReadResult, error) {
	return common.ParseResult(
		response,
		common.ExtractRecordsFromPath(responseField(params.ObjectName)),
		makeNextRecordsURL(response.Headers),
		common.GetMarshaledData,
		params.Fields,
	)
}

func makeNextRecordsURL(responseHeaders http.Header) common.NextPageFunc {
	return func(node *ajson.Node) (string, error) {
		nextURL := httpkit.HeaderLink(&common.JSONHTTPResponse{Headers: responseHeaders}, "next")
		if nextURL == "" {
			return "", nil
		}

		return nextURL, nil
	}
}

func (c *Connector) buildWriteRequest(ctx context.Context, params common.WriteParams) (*http.Request, error) {
	path, err := metadata.Schemas.LookupURLPath(c.ProviderContext.Module(), params.ObjectName)
	if err != nil {
		return nil, err
	}

	url, err := urlbuilder.New(c.ProviderInfo().BaseURL, path)
	if err != nil {
		return nil, err
	}

	method := http.MethodPost

	if params.RecordId != "" {
		url.AddPath(params.RecordId)

		// Users use POST for partial updates, other objects use PUT for full replacement.
		if params.ObjectName != "users" {
			method = http.MethodPut
		}
	}

	jsonData, err := json.Marshal(params.RecordData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), bytes.NewReader(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	return req, nil
}

func (c *Connector) parseWriteResponse(
	ctx context.Context,
	params common.WriteParams,
	request *http.Request,
	response *common.JSONHTTPResponse,
) (*common.WriteResult, error) {
	body, ok := response.Body()
	if !ok {
		return &common.WriteResult{
			Success: true,
		}, nil
	}

	recordID, err := jsonquery.New(body).TextWithDefault("id", params.RecordId)
	if err != nil {
		return nil, err
	}

	data, err := jsonquery.Convertor.ObjectToMap(body)
	if err != nil {
		return nil, err
	}

	return &common.WriteResult{
		Success:  true,
		RecordId: recordID,
		Data:     data,
	}, nil
}

func (c *Connector) buildDeleteRequest(ctx context.Context, params common.DeleteParams) (*http.Request, error) {
	path, err := metadata.Schemas.LookupURLPath(c.ProviderContext.Module(), params.ObjectName)
	if err != nil {
		return nil, err
	}

	url, err := urlbuilder.New(c.ProviderInfo().BaseURL, path, params.RecordId)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")

	return req, nil
}

func (c *Connector) parseDeleteResponse(
	ctx context.Context,
	params common.DeleteParams,
	request *http.Request,
	response *common.JSONHTTPResponse,
) (*common.DeleteResult, error) {
	return &common.DeleteResult{
		Success: true,
	}, nil
}
