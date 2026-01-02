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
)

// Objects supporting incremental sync via lastUpdated filter.
// Reference: https://developer.okta.com/docs/api/openapi/okta-management/management/tag/User/#tag/User/operation/listUsers
//
//nolint:gochecknoglobals
var supportIncrementalSync = datautils.NewStringSet(
	"users",
	"groups",
	"apps",
)

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

	// Add incremental sync filter using lastUpdated (ISO 8601 format)
	// Reference: https://developer.okta.com/docs/api/#filter
	if supportIncrementalSync.Has(params.ObjectName) && !params.Since.IsZero() {
		filterValue := "lastUpdated gt \"" +
			datautils.Time.FormatRFC3339inUTC(params.Since) + "\""
		url.WithQueryParam(filterKey, filterValue)
	}

	return http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
}

// parseReadResponse parses the HTTP response from read operations.
// Okta uses Link headers for pagination (cursor-based).
// Okta returns an array directly at the root for most endpoints.
// Reference: https://developer.okta.com/docs/api/#pagination
func (c *Connector) parseReadResponse(
	ctx context.Context,
	params common.ReadParams,
	request *http.Request,
	response *common.JSONHTTPResponse,
) (*common.ReadResult, error) {
	// Okta returns array at root level - use empty path ""
	return common.ParseResult(
		response,
		common.ExtractRecordsFromPath(""),
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
