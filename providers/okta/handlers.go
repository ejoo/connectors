package okta

import (
	"context"
	"net/http"

	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/common/urlbuilder"
	"github.com/amp-labs/connectors/internal/datautils"
	"github.com/amp-labs/connectors/internal/httpkit"
	"github.com/amp-labs/connectors/providers/okta/metadata"
	"github.com/spyzhov/ajson"
)

const (
	limitKey   = "limit"
	limitValue = "200" // Okta maximum per page
	filterKey  = "filter"
	sinceKey   = "since"
)

// Objects supporting incremental sync via lastUpdated filter.
// Reference: https://developer.okta.com/docs/reference/api/users/#list-users
// Reference: https://developer.okta.com/docs/reference/api/groups/#list-groups
//
//nolint:gochecknoglobals
var objectsWithLastUpdatedFilter = datautils.NewStringSet(
	"users",
	"groups",
	"apps",
)

// responseField returns the JSON path for extracting records.
// Most Okta endpoints return arrays at root level, except domains.
func responseField(objectName string) string {
	if objectName == "domains" {
		return "domains"
	}

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

	// Add pagination limit
	url.WithQueryParam(limitKey, limitValue)

	// Add incremental sync filter based on object type
	if !params.Since.IsZero() {
		if params.ObjectName == "logs" {
			// Logs API uses 'since' query param instead of filter expression
			// Reference: https://developer.okta.com/docs/reference/api/system-log/#request-parameters
			url.WithQueryParam(sinceKey, datautils.Time.FormatRFC3339inUTC(params.Since))
		} else if objectsWithLastUpdatedFilter.Has(params.ObjectName) {
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
	return common.ParseResult(
		response,
		common.ExtractRecordsFromPath(responseField(params.ObjectName)),
		makeNextRecordsURL(response.Headers),
		common.GetMarshaledData,
		params.Fields,
	)
}

// makeNextRecordsURL extracts the next page URL from Link header.
// Okta uses Link headers with rel="next" for pagination.
// Reference: https://developer.okta.com/docs/api/#link-header
func makeNextRecordsURL(responseHeaders http.Header) common.NextPageFunc {
	return func(node *ajson.Node) (string, error) {
		nextURL := httpkit.HeaderLink(&common.JSONHTTPResponse{Headers: responseHeaders}, "next")
		if nextURL == "" {
			return "", nil
		}

		return nextURL, nil
	}
}
