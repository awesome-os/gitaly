package gitcmd_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

func TestMVCCFromContext(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc       string
		prepareCtx func(context.Context) context.Context
		expected   bool
	}{
		{
			desc: "unset",
		},
		{
			desc:       "set",
			prepareCtx: gitcmd.ContextWithMVCC,
			expected:   true,
		},
		{
			desc: "cleared",
			prepareCtx: func(ctx context.Context) context.Context {
				return gitcmd.ContextWithoutMVCC(gitcmd.ContextWithMVCC(ctx))
			},
		},
		{
			desc: "set again after clearing",
			prepareCtx: func(ctx context.Context) context.Context {
				return gitcmd.ContextWithMVCC(gitcmd.ContextWithoutMVCC(gitcmd.ContextWithMVCC(ctx)))
			},
			expected: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			if tc.prepareCtx != nil {
				ctx = tc.prepareCtx(ctx)
			}

			require.Equal(t, tc.expected, gitcmd.MVCCFromContext(ctx))
		})
	}
}

func TestContextWithoutMVCC_unmarked(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	require.Same(t, ctx, gitcmd.ContextWithoutMVCC(ctx))
}
