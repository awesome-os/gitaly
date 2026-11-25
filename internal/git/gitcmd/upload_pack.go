package gitcmd

import "regexp"

const blobNoneFilter = "blob:none"

// uploadPackMissingObject matches a single "not our ref" fatal line for a SHA-1 or SHA-256 object
// ID, and nothing else.
var uploadPackMissingObject = regexp.MustCompile(`\Afatal: git upload-pack: not our ref ([0-9a-f]{40}|[0-9a-f]{64})\n\z`)

// IsUploadPackMissingObject reports whether git-upload-pack(1) failed because a partial clone's lazy
// fetch wanted an object that doesn't exist in the repository. Lazy fetches always request the
// "blob:none" filter. Gitaly runs upload-pack with uploadpack.allowAnySHA1InWant, so
// "fatal: git upload-pack: not our ref <oid>" means the wanted object doesn't exist, not that it's
// unreachable. With allowAnySHA1InWant disabled, unreachable objects are reported as
// "error: git upload-pack: not our ref <oid>" instead and aren't matched.
//
// The fatal line must be the only stderr output. Server-side faults that hide an object, like a
// corrupt object or a broken alternates link, print "error:" lines before git dies, and those must
// not be treated as the client's fault.
//
// The filter is only known if the caller parsed it before upload-pack died. Protocol v2 sends it
// ahead of the wants, but protocol v0 sends it after them, so a v0 request with enough wants may
// fail before the filter is read. Such requests keep their original error code.
func IsUploadPackMissingObject(filter, stderr string) bool {
	return filter == blobNoneFilter && uploadPackMissingObject.MatchString(stderr)
}
