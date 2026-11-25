package featureflag

// RebaseWithReplay enables the use of git-replay(1) in the
// UserRebaseConfirmable RPC.
var RebaseWithReplay = NewFeatureFlag(
	"rebase_with_replay",
	"v19.6",
	"https://gitlab.com/gitlab-org/gitaly/-/work_items/7145",
	false,
)
