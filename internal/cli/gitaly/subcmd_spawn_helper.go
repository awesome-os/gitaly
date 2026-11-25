package gitaly

import (
	"context"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v19/internal/command/spawnhelper"
)

func newSpawnHelperCommand() *cli.Command {
	return &cli.Command{
		Name:   spawnhelper.Subcommand,
		Usage:  "spawn processes on behalf of a running Gitaly",
		Hidden: true,
		Action: func(context.Context, *cli.Command) error {
			return spawnhelper.Serve()
		},
	}
}
