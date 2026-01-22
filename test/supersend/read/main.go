package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/amp-labs/connectors"
	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/test/supersend"
)

func main() {
	ctx := context.Background()

	conn := supersend.GetSuperSendConnector(ctx)

	// Test reading teams (standard array response)
	result, err := conn.Read(ctx, common.ReadParams{
		ObjectName: "teams",
		Fields:     connectors.Fields("id", "name", "domain", "isDefault"),
	})
	if err != nil {
		slog.Error("error reading teams", "error", err)
	} else {
		slog.Info("teams", "rows", result.Rows, "done", result.Done)
	}

	// Test reading senders (standard array response)
	result, err = conn.Read(ctx, common.ReadParams{
		ObjectName: "senders",
		Fields:     connectors.Fields("id", "email", "warm", "max_per_day"),
	})
	if err != nil {
		slog.Error("error reading senders", "error", err)
	} else {
		slog.Info("senders", "rows", result.Rows, "done", result.Done)
	}

	// Test reading org (single object response - empty responseKey)
	result, err = conn.Read(ctx, common.ReadParams{
		ObjectName: "org",
		Fields:     connectors.Fields("id", "name", "current_plan"),
	})
	if err != nil {
		slog.Error("error reading org", "error", err)
	} else {
		slog.Info("org", "rows", result.Rows, "done", result.Done)
	}

	// Note: Many SuperSend endpoints require TeamId query parameter:
	// - labels, sender-profiles, campaigns/overview, contact/all, etc.
	// These would need custom query parameter support to work.

	os.Exit(0)
}
