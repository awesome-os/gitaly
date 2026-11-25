package featureflag

// MVCCMigration enables the opportunistic migration of existing repositories to
// the MVCC reference backend.
var MVCCMigration = NewFeatureFlag(
	"mvcc_migration",
	"v19.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/work_items/7347",
	false,
)
