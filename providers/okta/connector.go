// Package okta provides a connector for the Okta Management API.
// API Documentation: https://developer.okta.com/docs/api/
// Users API: https://developer.okta.com/docs/api/openapi/okta-management/management/tag/User/
// Groups API: https://developer.okta.com/docs/api/openapi/okta-management/management/tag/Group/
// Apps API: https://developer.okta.com/docs/api/openapi/okta-management/management/tag/Application/
package okta

import (
	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/internal/components"
	"github.com/amp-labs/connectors/internal/components/operations"
	"github.com/amp-labs/connectors/internal/components/reader"
	"github.com/amp-labs/connectors/internal/components/schema"
	"github.com/amp-labs/connectors/providers"
	"github.com/amp-labs/connectors/providers/okta/metadata"
)

// Connector is the Okta connector.
type Connector struct {
	// Basic connector
	*components.Connector

	// Require authenticated client
	common.RequireAuthenticatedClient

	// Supported operations
	components.SchemaProvider
	components.Reader
}

// NewConnector creates a new Okta connector.
func NewConnector(params common.ConnectorParams) (*Connector, error) {
	return components.Initialize(providers.Okta, params, constructor)
}

func constructor(base *components.Connector) (*Connector, error) {
	connector := &Connector{Connector: base}

	// Set the metadata provider for the connector
	connector.SchemaProvider = schema.NewOpenAPISchemaProvider(
		connector.ProviderContext.Module(),
		metadata.Schemas,
	)

	// Add Reader for read operations
	connector.Reader = reader.NewHTTPReader(
		connector.HTTPClient().Client,
		components.NewEmptyEndpointRegistry(),
		connector.ProviderContext.Module(),
		operations.ReadHandlers{
			BuildRequest:  connector.buildReadRequest,
			ParseResponse: connector.parseReadResponse,
			ErrorHandler:  common.InterpretError,
		},
	)

	return connector, nil
}
