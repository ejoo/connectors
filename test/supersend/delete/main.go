package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/providers/supersend"
	testsupersend "github.com/amp-labs/connectors/test/supersend"
	"github.com/amp-labs/connectors/test/utils"
)

func main() {
	os.Exit(mainFn())
}

func mainFn() int {
	ctx := context.Background()
	conn := testsupersend.GetSuperSendConnector(ctx)

	// Step 1: Create a team first (needed for label creation)
	teamID, err := createTeam(ctx, conn)
	if err != nil {
		slog.Error("create team failed", "error", err)
		return 1
	}

	// Step 2: Create a label using the team ID
	labelID, err := createLabel(ctx, conn, teamID)
	if err != nil {
		slog.Error("create label failed", "error", err)
		return 1
	}

	// Step 3: Delete the label
	err = testDeleteLabel(ctx, conn, labelID)
	if err != nil {
		slog.Error("delete label failed", "error", err)
		return 1
	}

	slog.Info("All delete tests completed successfully!")

	return 0
}

func createTeam(ctx context.Context, conn *supersend.Connector) (string, error) {
	slog.Info("Creating team for delete test...")

	params := common.WriteParams{
		ObjectName: "teams",
		RecordData: map[string]any{
			"name":   fmt.Sprintf("Delete Test Team %d", os.Getpid()),
			"domain": "deleteteam.example.com",
			"about":  "Created for delete test",
		},
	}

	res, err := conn.Write(ctx, params)
	if err != nil {
		return "", err
	}

	slog.Info("team created", "teamId", res.RecordId)

	return res.RecordId, nil
}

func createLabel(ctx context.Context, conn *supersend.Connector, teamID string) (string, error) {
	slog.Info("Creating label for delete test...", "teamId", teamID)

	params := common.WriteParams{
		ObjectName: "labels",
		RecordData: map[string]any{
			"name":   "Label To Delete",
			"color":  "#FF0000",
			"TeamId": teamID,
		},
	}

	res, err := conn.Write(ctx, params)
	if err != nil {
		return "", err
	}

	slog.Info("label created", "labelId", res.RecordId)

	return res.RecordId, nil
}

func testDeleteLabel(ctx context.Context, conn *supersend.Connector, labelID string) error {
	slog.Info("Testing delete label...", "labelId", labelID)

	params := common.DeleteParams{
		ObjectName: "labels",
		RecordId:   labelID,
	}

	res, err := conn.Delete(ctx, params)
	if err != nil {
		slog.Error("error deleting label", "error", err)
		return err
	}

	slog.Info("Delete label response:")
	utils.DumpJSON(res, os.Stdout)

	slog.Info("delete label completed", "success", res.Success)

	return nil
}
