package okta

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/common/readhelper"
	"github.com/amp-labs/connectors/common/urlbuilder"
	"github.com/amp-labs/connectors/internal/datautils"
	"github.com/amp-labs/connectors/internal/httpkit"
	"github.com/amp-labs/connectors/providers/okta/metadata"
	"github.com/spyzhov/ajson"
)

const (
	limitKey  = "limit"
	pageLimit = 200 // Okta maximum per page
	filterKey = "filter"
	sinceKey  = "since"
)

// Objects supporting incremental sync via lastUpdated filter.
// Reference: https://developer.okta.com/docs/reference/api/users/#list-users
// Reference: https://developer.okta.com/docs/reference/api/groups/#list-groups
//
//nolint:gochecknoglobals
var objectsWithProviderSideFilter = datautils.NewStringSet(
	"users",
	"groups",
	"apps",
)

// Objects that support connector-side filtering via lastUpdated field.
// These objects don't support provider-side filtering but have lastUpdated timestamp.
//
//nolint:gochecknoglobals
var objectsWithConnectorSideFilter = datautils.NewStringSet(
	"devices",
	"idps",
	"authorizationServers",
	"trustedOrigins",
	"zones",
	"authenticators",
	"policies",
	"eventHooks",
)

// responseField returns the JSON path for extracting records.
// Most Okta endpoints return arrays at root level, except domains.
func responseField(objectName string) string {
	// Domains endpoint wraps the array in a "domains" key
	if objectName == "domains" {
		return "domains"
	}

	// Empty string means the response is an array at root level
	return ""
}

// buildReadRequest constructs the HTTP request for read operations.
// Reference: https://developer.okta.com/docs/api/
func (c *Connector) buildReadRequest(ctx context.Context, params common.ReadParams) (*http.Request, error) {
	// Use NextPage directly (from Link header) if provided
	if params.NextPage != "" {
		return http.NewRequestWithContext(ctx, http.MethodGet, params.NextPage.String(), nil)
	}

	// Build URL from metadata
	path, err := metadata.Schemas.LookupURLPath(c.ProviderContext.Module(), params.ObjectName)
	if err != nil {
		return nil, err
	}

	url, err := urlbuilder.New(c.ProviderInfo().BaseURL, path)
	if err != nil {
		return nil, err
	}

	// Add pagination limit - use PageSize if provided, otherwise use default
	pageSize := pageLimit
	if params.PageSize > 0 {
		if params.PageSize > pageLimit {
			pageSize = pageLimit
		} else {
			pageSize = params.PageSize
		}
	}

	url.WithQueryParam(limitKey, strconv.Itoa(pageSize))

	// Add incremental sync filter based on object type
	if !params.Since.IsZero() {
		if params.ObjectName == "logs" {
			// Logs API uses 'since' query param instead of filter expression
			// Reference: https://developer.okta.com/docs/reference/api/system-log/#request-parameters
			url.WithQueryParam(sinceKey, datautils.Time.FormatRFC3339inUTC(params.Since))
		} else if objectsWithProviderSideFilter.Has(params.ObjectName) {
			// Other objects use lastUpdated filter expression
			// Reference: https://developer.okta.com/docs/reference/api/users/#list-users-with-a-filter
			filterValue := "lastUpdated gt \"" + datautils.Time.FormatRFC3339inUTC(params.Since) + "\""
			url.WithQueryParam(filterKey, filterValue)
		}
	}

	return http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
}

// parseReadResponse parses the HTTP response from read operations.
// Okta uses Link headers for pagination (cursor-based).
// Reference: https://developer.okta.com/docs/api/#pagination
func (c *Connector) parseReadResponse(
	ctx context.Context,
	params common.ReadParams,
	request *http.Request,
	response *common.JSONHTTPResponse,
) (*common.ReadResult, error) {
	return common.ParseResultFiltered(
		params,
		response,
		common.MakeRecordsFunc(responseField(params.ObjectName)),
		makeFilterFunc(params, response.Headers),
		common.MakeMarshaledDataFunc(nil),
		params.Fields,
	)
}

// makeFilterFunc returns the appropriate filter function based on object type.
// Objects with provider-side filtering (users, groups, apps, logs) don't need connector-side filtering.
// Objects with lastUpdated field but no provider-side support use connector-side filtering.
func makeFilterFunc(params common.ReadParams, headers http.Header) common.RecordsFilterFunc {
	nextPageFunc := makeNextRecordsURL(headers)

	// Objects with provider-side filtering don't need connector-side filtering
	if objectsWithProviderSideFilter.Has(params.ObjectName) || params.ObjectName == "logs" {
		return readhelper.MakeIdentityFilterFunc(nextPageFunc)
	}

	// Objects without any timestamp field - no filtering possible
	if !objectsWithConnectorSideFilter.Has(params.ObjectName) {
		return readhelper.MakeIdentityFilterFunc(nextPageFunc)
	}

	// Apply connector-side filtering using lastUpdated field
	return readhelper.MakeTimeFilterFunc(
		readhelper.ChronologicalOrder,
		readhelper.NewTimeBoundary(),
		"lastUpdated",
		time.RFC3339,
		nextPageFunc,
	)
}

// makeNextRecordsURL extracts the next page URL from Link header.
// Okta uses Link headers with rel="next" for pagination.
// Reference: https://developer.okta.com/docs/api/#link-header
func makeNextRecordsURL(responseHeaders http.Header) common.NextPageFunc {
	return func(node *ajson.Node) (string, error) {
		return httpkit.HeaderLink(&common.JSONHTTPResponse{Headers: responseHeaders}, "next"), nil
	}
}
