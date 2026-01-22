package supersend

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/common/readhelper"
	"github.com/amp-labs/connectors/common/urlbuilder"
	"github.com/amp-labs/connectors/internal/jsonquery"
	"github.com/amp-labs/connectors/providers/supersend/metadata"
	"github.com/spyzhov/ajson"
)

// Pagination constants for SuperSend API.
// SuperSend uses offset-based pagination with limit/offset query parameters.
// The API returns pagination.has_more to indicate if more records exist.
const (
	defaultPageSize = "100" // Default page size for SuperSend API (max is 100)
	limitParam      = "limit"
	offsetParam     = "offset"

	// updatedAtField is the timestamp field used for connector-side filtering.
	// SuperSend API doesn't support native time-based filtering, so we filter
	// records client-side using the updatedAt field for incremental sync.
	// Format: ISO 8601 / RFC3339 (e.g., "2024-01-15T10:00:00.000Z").
	updatedAtField = "updatedAt"
)

// buildReadRequest constructs the HTTP request for read operations.
// Handles pagination via offset parameter and respects PageSize up to max limit.
func (c *Connector) buildReadRequest(ctx context.Context, params common.ReadParams) (*http.Request, error) {
	if params.NextPage != "" {
		// Use NextPage URL directly for pagination
		nextPageURL, err := urlbuilder.New(params.NextPage.String())
		if err != nil {
			return nil, err
		}

		return http.NewRequestWithContext(ctx, http.MethodGet, nextPageURL.String(), nil)
	}

	// Build initial URL from metadata
	apiURL, err := c.buildURL(params.ObjectName)
	if err != nil {
		return nil, err
	}

	// Add pagination limit - use PageSize if provided, otherwise use default.
	// SuperSend API enforces max of 100 on its side.
	apiURL.WithQueryParam(limitParam, readhelper.PageSizeWithDefaultStr(params, defaultPageSize))

	return http.NewRequestWithContext(ctx, http.MethodGet, apiURL.String(), nil)
}

func (c *Connector) buildURL(objectName string) (*urlbuilder.URL, error) {
	path, err := metadata.Schemas.LookupURLPath(common.ModuleRoot, objectName)
	if err != nil {
		return nil, err
	}

	return urlbuilder.New(c.ProviderInfo().BaseURL, path)
}

func (c *Connector) parseReadResponse(
	ctx context.Context,
	params common.ReadParams,
	request *http.Request,
	response *common.JSONHTTPResponse,
) (*common.ReadResult, error) {
	// LookupArrayFieldName returns the responseKey from the schema
	responseKey := metadata.Schemas.LookupArrayFieldName(common.ModuleRoot, params.ObjectName)
	nextPageFunc := makeNextRecordsURL(c.ProviderInfo().BaseURL, request.URL)

	return common.ParseResultFiltered(
		params,
		response,
		getRecords(responseKey),
		makeFilterFunc(params, nextPageFunc),
		common.MakeMarshaledDataFunc(nil),
		params.Fields,
	)
}

// makeFilterFunc returns a filter function for connector-side time-based filtering.
// SuperSend API doesn't support native time filtering, so we filter records
// client-side using the updatedAt field when Since/Until params are provided.
func makeFilterFunc(params common.ReadParams, nextPageFunc common.NextPageFunc) common.RecordsFilterFunc {
	// If no time filtering is requested, use identity filter (no filtering)
	if params.Since.IsZero() && params.Until.IsZero() {
		return readhelper.MakeIdentityFilterFunc(nextPageFunc)
	}

	// Apply time-based filtering using updatedAt field.
	// Using Unordered since SuperSend doesn't guarantee record ordering.
	return readhelper.MakeTimeFilterFunc(
		readhelper.Unordered,
		readhelper.NewTimeBoundary(),
		updatedAtField,
		time.RFC3339,
		nextPageFunc,
	)
}

// getRecords returns a function that extracts records from the response.
// Handles three cases:
// 1. Nested responseKey (dot-notation like "data.conversations") - traverses nested structure
// 2. Empty responseKey - response is a single object, wrap it in an array
// 3. Standard responseKey - extract array from the specified key.
func getRecords(responseKey string) common.NodeRecordsFunc {
	return func(node *ajson.Node) ([]*ajson.Node, error) {
		// Handle empty responseKey (single object response like /org endpoint)
		if responseKey == "" {
			return getSingleObjectAsArray(node)
		}

		// Handle nested responseKey (dot-notation like "data.conversations")
		if strings.Contains(responseKey, ".") {
			return getNestedRecords(node, responseKey)
		}

		// Standard responseKey - extract array from the specified key
		return jsonquery.New(node).ArrayOptional(responseKey)
	}
}

// getSingleObjectAsArray wraps a single object response in an array.
func getSingleObjectAsArray(node *ajson.Node) ([]*ajson.Node, error) {
	if node.IsObject() {
		return []*ajson.Node{node}, nil
	}

	return nil, jsonquery.ErrNotArray
}

// getNestedRecords traverses a nested responseKey (dot-notation) to extract records.
func getNestedRecords(node *ajson.Node, responseKey string) ([]*ajson.Node, error) {
	parts := strings.Split(responseKey, ".")
	currentNode := node

	for i, part := range parts {
		isLastPart := i == len(parts)-1

		// Try as array for the last part
		if isLastPart {
			return jsonquery.New(currentNode).ArrayOptional(part)
		}

		childNode, err := jsonquery.New(currentNode).ObjectOptional(part)
		if err != nil {
			return nil, err
		}

		if childNode == nil {
			return nil, jsonquery.ErrKeyNotFound
		}

		currentNode = childNode
	}

	return nil, jsonquery.ErrNotArray
}

// makeNextRecordsURL returns a function that builds the next page URL if more records exist.
// SuperSend uses pagination.has_more to indicate if there are more records.
func makeNextRecordsURL(baseURL string, requestURL *url.URL) common.NextPageFunc {
	return func(node *ajson.Node) (string, error) {
		if !hasMoreRecords(node) {
			return "", nil
		}

		// Calculate next offset based on current request
		nextOffset := calculateNextOffset(requestURL)

		return buildNextPageURL(baseURL, requestURL, nextOffset)
	}
}

// hasMoreRecords checks the pagination.has_more field to determine if more records exist.
func hasMoreRecords(node *ajson.Node) bool {
	paginationNode, err := jsonquery.New(node).ObjectOptional("pagination")
	if err != nil || paginationNode == nil {
		return false
	}

	hasMore, err := jsonquery.New(paginationNode).BoolOptional("has_more")
	if err != nil || hasMore == nil {
		return false
	}

	return *hasMore
}

// calculateNextOffset extracts current offset from URL and adds the limit to get next offset.
func calculateNextOffset(requestURL *url.URL) int {
	query := requestURL.Query()

	currentOffset := 0

	if offsetStr := query.Get(offsetParam); offsetStr != "" {
		if parsed, err := strconv.Atoi(offsetStr); err == nil {
			currentOffset = parsed
		}
	}

	// Get limit from URL, default to 100 if not present or invalid
	limit := 100

	if limitStr := query.Get(limitParam); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limit = parsed
		}
	}

	return currentOffset + limit
}

// buildNextPageURL constructs the URL for the next page of results.
func buildNextPageURL(baseURL string, requestURL *url.URL, nextOffset int) (string, error) {
	// Preserve existing query params but update offset
	query := requestURL.Query()
	query.Set(offsetParam, strconv.Itoa(nextOffset))

	// Ensure limit is set
	if query.Get(limitParam) == "" {
		query.Set(limitParam, defaultPageSize)
	}

	nextURL, err := urlbuilder.New(baseURL, requestURL.Path)
	if err != nil {
		return "", err
	}

	// Apply all query params
	for key, values := range query {
		for _, value := range values {
			nextURL.WithQueryParam(key, value)
		}
	}

	return nextURL.String(), nil
}
