package signature

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/armor"
	pgperrors "github.com/ProtonMail/go-crypto/openpgp/errors"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
)

// PgpVerificationStatus is the cryptographic outcome of checking a PGP
// signature against a set of candidate public keys.
type PgpVerificationStatus int

const (
	// PgpUnverified indicates that no supplied key validated the signature. It
	// also covers an otherwise valid signature whose own validity period has
	// expired, matching how GPGME-based verification treats that case.
	PgpUnverified PgpVerificationStatus = iota
	// PgpVerified indicates that a supplied key validated the signature.
	PgpVerified
	// PgpRevokedKey indicates that the signing key has been revoked.
	PgpRevokedKey
	// PgpExpiredKey indicates that the signing key has expired.
	PgpExpiredKey
)

// PgpSignatureInfo holds facts parsed from a PGP signature packet. It is
// extracted without verifying the signature and without any public key, so
// every value reflects the signature's own unverified claims.
type PgpSignatureInfo struct {
	// Fingerprint is the uppercase hex issuer fingerprint the signature claims.
	// When the signature carries no fingerprint subpacket it falls back to the
	// 16-character key ID. It is empty when neither is present. It is a claim,
	// not a verified fact: use it to resolve a candidate key, then verify.
	Fingerprint string
	// CreatedAt is the signature creation time.
	CreatedAt time.Time
}

// ParsePgpSignatureInfo parses an armored detached PGP signature and extracts
// the claimed issuer fingerprint and creation time. It does not verify the
// signature, so it needs no public key.
func ParsePgpSignatureInfo(signature []byte) (PgpSignatureInfo, error) {
	sig, err := parseSignaturePacket(signature)
	if err != nil {
		return PgpSignatureInfo{}, err
	}

	var fingerprint string
	switch {
	case len(sig.IssuerFingerprint) > 0:
		fingerprint = fingerprintHex(sig.IssuerFingerprint)
	case sig.IssuerKeyId != nil:
		fingerprint = fmt.Sprintf("%016X", *sig.IssuerKeyId)
	}

	return PgpSignatureInfo{
		Fingerprint: fingerprint,
		CreatedAt:   sig.CreationTime,
	}, nil
}

// Keyring is a set of candidate public keys parsed once so the same keys can be
// reused to verify many signatures in a request without re-parsing them.
type Keyring struct {
	entities openpgp.EntityList
}

// ParseVerificationKeys parses armored PGP public keys into a Keyring. A
// malformed key is skipped rather than failing the whole set.
func ParseVerificationKeys(publicKeys [][]byte) *Keyring {
	keyring := &Keyring{}
	for _, key := range publicKeys {
		entities, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(key))
		if err != nil {
			continue
		}
		keyring.entities = append(keyring.entities, entities...)
	}

	return keyring
}

// VerifyPgpSignature checks an armored detached PGP signature over signedText
// against the keyring. On success it returns PgpVerified and the uppercase hex
// fingerprint of the key that actually validated the signature, primary or
// signing subkey, read from the key material rather than the signature's own
// claim. A revoked or expired signing key returns PgpRevokedKey or
// PgpExpiredKey with an empty fingerprint. Everything else, including an
// expired signature or an unknown issuer, returns PgpUnverified.
func (k *Keyring) VerifyPgpSignature(signature, signedText []byte) (PgpVerificationStatus, string, error) {
	if len(k.entities) == 0 {
		return PgpUnverified, "", nil
	}

	signer, err := openpgp.CheckArmoredDetachedSignature(
		k.entities,
		bytes.NewReader(signedText),
		bytes.NewReader(signature),
		&packet.Config{},
	)
	switch {
	case err == nil:
		fingerprint, ferr := signingKeyFingerprint(signer, signature)
		if ferr != nil {
			return PgpUnverified, "", ferr
		}
		return PgpVerified, fingerprint, nil
	case errors.Is(err, pgperrors.ErrKeyRevoked):
		return PgpRevokedKey, "", nil
	case errors.Is(err, pgperrors.ErrKeyExpired):
		return PgpExpiredKey, "", nil
	default:
		return PgpUnverified, "", nil
	}
}

// signingKeyFingerprint returns the fingerprint of the key within the verified
// signer that produced the signature. go-crypto matches and verifies against
// the key whose ID equals the signature's issuer key ID, so reading that key's
// fingerprint from the entity reflects the key that actually signed rather than
// any claim the signature carries. It falls back to the primary key.
func signingKeyFingerprint(signer *openpgp.Entity, signature []byte) (string, error) {
	sig, err := parseSignaturePacket(signature)
	if err != nil {
		return "", err
	}

	if sig.IssuerKeyId != nil {
		if signer.PrimaryKey != nil && signer.PrimaryKey.KeyId == *sig.IssuerKeyId {
			return fingerprintHex(signer.PrimaryKey.Fingerprint), nil
		}
		for _, subkey := range signer.Subkeys {
			if subkey.PublicKey != nil && subkey.PublicKey.KeyId == *sig.IssuerKeyId {
				return fingerprintHex(subkey.PublicKey.Fingerprint), nil
			}
		}
	}

	if signer.PrimaryKey != nil {
		return fingerprintHex(signer.PrimaryKey.Fingerprint), nil
	}

	return "", nil
}

func parseSignaturePacket(signature []byte) (*packet.Signature, error) {
	block, err := armor.Decode(bytes.NewReader(signature))
	if err != nil {
		return nil, fmt.Errorf("decode armor: %w", err)
	}

	pkt, err := packet.NewReader(block.Body).Next()
	if err != nil {
		return nil, fmt.Errorf("read packet: %w", err)
	}

	sig, ok := pkt.(*packet.Signature)
	if !ok {
		return nil, fmt.Errorf("unexpected packet type %T", pkt)
	}

	return sig, nil
}

func fingerprintHex(fingerprint []byte) string {
	return strings.ToUpper(hex.EncodeToString(fingerprint))
}
