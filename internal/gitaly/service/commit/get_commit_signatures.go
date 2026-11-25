package commit

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v19/internal/signature"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v19/streamio"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxVerificationKeys caps how many candidate public keys a request may carry.
// The signature of a commit is made by a single key, so a caller only needs the
// keys for the distinct signers in the batch. The cap keeps the request small
// and bounds the per-commit verification work, which is linear in the number of
// keys. gRPC's own message-size limit guards total request bytes separately.
const maxVerificationKeys = 256

func (s *server) GetCommitSignatures(request *gitalypb.GetCommitSignaturesRequest, stream gitalypb.CommitService_GetCommitSignaturesServer) error {
	ctx := stream.Context()

	if err := s.locator.ValidateRepository(stream.Context(), request.GetRepository()); err != nil {
		return err
	}

	repo := s.localRepoFactory.Build(request.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	if err := validateGetCommitSignaturesRequest(objectHash, request); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	objectReader, cancel, err := s.catfileCache.ObjectReaderWithoutMailmap(ctx, repo)
	if err != nil {
		return structerr.NewInternal("%w", err)
	}
	defer cancel()

	var signingKeys *signature.SigningKeys
	if s.cfg.Git.SigningKey != "" {
		signingKeys, err = signature.ParseSigningKeys(s.cfg.Git.SigningKey, s.cfg.Git.RotatedSigningKeys...)
		if err != nil {
			return fmt.Errorf("failed to parse signing key: %w", err)
		}
	}

	// Parse the caller-supplied verification keys once up front so they are not
	// re-parsed for every commit.
	var verificationKeyring *signature.Keyring
	if len(request.GetVerificationKeys()) > 0 {
		verificationKeyring = signature.ParseVerificationKeys(request.GetVerificationKeys())
	}

	parser := catfile.NewParser()
	for _, commitID := range request.GetCommitIds() {
		commitObj, err := objectReader.Object(ctx, git.Revision(commitID)+"^{commit}")
		if err != nil {
			if errors.As(err, &catfile.NotFoundError{}) {
				continue
			}
			return structerr.NewInternal("%w", err)
		}

		commit, err := parser.ParseCommit(commitObj)
		if err != nil {
			return structerr.NewInternal("%w", err)
		}

		sig := []byte{}
		if len(commit.SignatureData.Signatures) > 0 {
			// While there could be potentially multiple signatures in a Git
			// commit, like Git, we only consider the first.
			sig = commit.SignatureData.Signatures[0]
		}

		signer := gitalypb.GetCommitSignaturesResponse_SIGNER_USER
		if signingKeys != nil {
			if signingKeys.Verify(sig, commit.SignatureData.Payload) == nil {
				signer = gitalypb.GetCommitSignaturesResponse_SIGNER_SYSTEM
			}
		}

		result := verificationResult{signer: signer}
		if commit.SignatureType == gitalypb.SignatureType_PGP && len(sig) > 0 {
			if info, err := signature.ParsePgpSignatureInfo(sig); err == nil {
				result.fingerprint = info.Fingerprint
				if !info.CreatedAt.IsZero() {
					result.signatureTime = timestamppb.New(info.CreatedAt)
				}
			}

			if verificationKeyring != nil {
				result.verification = gitalypb.GetCommitSignaturesResponse_VERIFICATION_UNVERIFIED

				status, fp, err := verificationKeyring.VerifyPgpSignature(sig, commit.SignatureData.Payload)
				if err == nil {
					switch status {
					case signature.PgpVerified:
						result.verification = gitalypb.GetCommitSignaturesResponse_VERIFICATION_VERIFIED
						result.verifiedFingerprint = fp
					case signature.PgpRevokedKey:
						result.verification = gitalypb.GetCommitSignaturesResponse_VERIFICATION_REVOKED_KEY
					case signature.PgpExpiredKey:
						result.verification = gitalypb.GetCommitSignaturesResponse_VERIFICATION_EXPIRED_KEY
					}
				}
			}
		}

		if err = sendResponse(sig, commit, result, stream); err != nil {
			return structerr.NewInternal("%w", err)
		}
	}

	return nil
}

// verificationResult carries the signature facts Gitaly derives for a single
// commit, populated on the first response message for that commit.
type verificationResult struct {
	signer              gitalypb.GetCommitSignaturesResponse_Signer
	fingerprint         string
	signatureTime       *timestamppb.Timestamp
	verification        gitalypb.GetCommitSignaturesResponse_Verification
	verifiedFingerprint string
}

func sendResponse(
	sig []byte,
	commit *catfile.Commit,
	result verificationResult,
	stream gitalypb.CommitService_GetCommitSignaturesServer,
) error {
	if len(sig) <= 0 {
		return nil
	}

	err := stream.Send(&gitalypb.GetCommitSignaturesResponse{
		CommitId:               commit.Id,
		Signature:              sig,
		Signer:                 result.signer,
		Author:                 commit.Author,
		Committer:              commit.Committer,
		SigningKeyFingerprint:  result.fingerprint,
		SignatureType:          commit.SignatureType,
		Verification:           result.verification,
		VerifiedKeyFingerprint: result.verifiedFingerprint,
		SignatureTime:          result.signatureTime,
	})
	if err != nil {
		return err
	}

	streamWriter := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetCommitSignaturesResponse{SignedText: p})
	})

	msgReader := bytes.NewReader(commit.SignatureData.Payload)

	_, err = io.Copy(streamWriter, msgReader)
	if err != nil {
		return fmt.Errorf("failed to send response: %w", err)
	}

	return nil
}

func validateGetCommitSignaturesRequest(objectHash git.ObjectHash, request *gitalypb.GetCommitSignaturesRequest) error {
	if len(request.GetCommitIds()) == 0 {
		return errors.New("empty CommitIds")
	}

	// Do not support shorthand or invalid commit SHAs
	for _, commitID := range request.GetCommitIds() {
		if err := objectHash.ValidateHex(commitID); err != nil {
			return err
		}
	}

	if n := len(request.GetVerificationKeys()); n > maxVerificationKeys {
		return fmt.Errorf("too many verification keys: %d exceeds limit of %d", n, maxVerificationKeys)
	}

	return nil
}
