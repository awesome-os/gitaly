package signature

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/armor"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
	"github.com/stretchr/testify/require"
)

func TestParsePgpSignatureInfo(t *testing.T) {
	t.Parallel()

	entity, armoredSig := newSignature(t, []byte("hello world"))

	t.Run("extracts fingerprint and creation time", func(t *testing.T) {
		t.Parallel()

		info, err := ParsePgpSignatureInfo(armoredSig)
		require.NoError(t, err)
		require.NotEmpty(t, info.Fingerprint)
		require.Equal(t, strings.ToUpper(info.Fingerprint), info.Fingerprint)
		require.False(t, info.CreatedAt.IsZero())

		// The issuer fingerprint must identify the signing key.
		require.Equal(t, fingerprintHex(entity.PrimaryKey.Fingerprint), info.Fingerprint)
	})

	t.Run("rejects non-signature input", func(t *testing.T) {
		t.Parallel()

		_, err := ParsePgpSignatureInfo([]byte("not a signature"))
		require.Error(t, err)
	})
}

func TestVerifyPgpSignature(t *testing.T) {
	t.Parallel()

	signedText := []byte("the signed payload")
	entity, armoredSig := newSignature(t, signedText)
	pubKey := armoredPublicKey(t, entity)

	otherEntity, _ := newSignature(t, signedText)
	otherPubKey := armoredPublicKey(t, otherEntity)

	for _, tc := range []struct {
		desc           string
		signedText     []byte
		publicKeys     [][]byte
		expectedStatus PgpVerificationStatus
	}{
		{
			desc:           "matching key validates",
			signedText:     signedText,
			publicKeys:     [][]byte{pubKey},
			expectedStatus: PgpVerified,
		},
		{
			desc:           "matching key among several validates",
			signedText:     signedText,
			publicKeys:     [][]byte{otherPubKey, pubKey},
			expectedStatus: PgpVerified,
		},
		{
			desc:           "wrong key does not validate",
			signedText:     signedText,
			publicKeys:     [][]byte{otherPubKey},
			expectedStatus: PgpUnverified,
		},
		{
			desc:           "no keys does not validate",
			signedText:     signedText,
			publicKeys:     nil,
			expectedStatus: PgpUnverified,
		},
		{
			desc:           "tampered payload does not validate",
			signedText:     []byte("the signed payload tampered"),
			publicKeys:     [][]byte{pubKey},
			expectedStatus: PgpUnverified,
		},
		{
			desc:           "malformed candidate key is skipped",
			signedText:     signedText,
			publicKeys:     [][]byte{[]byte("garbage"), pubKey},
			expectedStatus: PgpVerified,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			status, fingerprint, err := ParseVerificationKeys(tc.publicKeys).VerifyPgpSignature(armoredSig, tc.signedText)
			require.NoError(t, err)
			require.Equal(t, tc.expectedStatus, status)

			if status == PgpVerified {
				require.Equal(t, fingerprintHex(entity.PrimaryKey.Fingerprint), fingerprint)
			} else {
				require.Empty(t, fingerprint)
			}
		})
	}
}

func TestVerifyPgpSignature_revokedKey(t *testing.T) {
	t.Parallel()

	signedText := []byte("payload signed by a revoked key")

	entity, err := openpgp.NewEntity("Revoked User", "", "revoked@example.com", nil)
	require.NoError(t, err)

	var sig bytes.Buffer
	require.NoError(t, openpgp.ArmoredDetachSign(&sig, entity, bytes.NewReader(signedText), &packet.Config{}))

	require.NoError(t, entity.RevokeKey(packet.KeyCompromised, "key compromised", &packet.Config{}))

	status, fingerprint, err := ParseVerificationKeys([][]byte{armoredPublicKey(t, entity)}).
		VerifyPgpSignature(sig.Bytes(), signedText)
	require.NoError(t, err)
	require.Equal(t, PgpRevokedKey, status)
	require.Empty(t, fingerprint)
}

func TestVerifyPgpSignature_expiredKey(t *testing.T) {
	t.Parallel()

	signedText := []byte("payload signed by a key that later expired")

	// Create a key that was born two days ago and lived for only one hour, so
	// it is expired by the time VerifyPgpSignature checks it against time.Now.
	pastConfig := &packet.Config{
		Time:            func() time.Time { return time.Now().Add(-48 * time.Hour) },
		KeyLifetimeSecs: 3600,
	}

	entity, err := openpgp.NewEntity("Expired User", "", "expired@example.com", pastConfig)
	require.NoError(t, err)

	var sig bytes.Buffer
	require.NoError(t, openpgp.ArmoredDetachSign(&sig, entity, bytes.NewReader(signedText), pastConfig))

	status, fingerprint, err := ParseVerificationKeys([][]byte{armoredPublicKey(t, entity)}).
		VerifyPgpSignature(sig.Bytes(), signedText)
	require.NoError(t, err)
	require.Equal(t, PgpExpiredKey, status)
	require.Empty(t, fingerprint)
}

func TestVerifyPgpSignature_signingSubkey(t *testing.T) {
	t.Parallel()

	signedText := []byte("payload signed by a subkey")

	entity, err := openpgp.NewEntity("Subkey User", "", "subkey@example.com", nil)
	require.NoError(t, err)
	require.NoError(t, entity.AddSigningSubkey(&packet.Config{}))
	// NewEntity already added an encryption subkey, so the signing one is last.
	signingSubkey := entity.Subkeys[len(entity.Subkeys)-1]
	require.True(t, signingSubkey.Sig.FlagSign)

	// go-crypto signs with the newest signing subkey over the primary key.
	var sig bytes.Buffer
	require.NoError(t, openpgp.ArmoredDetachSign(&sig, entity, bytes.NewReader(signedText), &packet.Config{}))

	status, fingerprint, err := ParseVerificationKeys([][]byte{armoredPublicKey(t, entity)}).
		VerifyPgpSignature(sig.Bytes(), signedText)
	require.NoError(t, err)
	require.Equal(t, PgpVerified, status)
	require.Equal(t, fingerprintHex(signingSubkey.PublicKey.Fingerprint), fingerprint)
	require.NotEqual(t, fingerprintHex(entity.PrimaryKey.Fingerprint), fingerprint)
}

func TestVerifyPgpSignature_spoofedIssuerFingerprint(t *testing.T) {
	t.Parallel()

	signedText := []byte("payload with a spoofed issuer fingerprint")
	signer, armoredSig := newSignature(t, signedText)
	victim, _ := newSignature(t, signedText)

	// The unhashed area is not covered by the signature, so an attacker can
	// claim the victim's fingerprint there while keeping the signer's key ID
	// last, which is the ID go-crypto uses to look up the verifying key.
	spoofed := replaceUnhashedSubpackets(t, armoredSig,
		append([]byte{22, 33, 4}, victim.PrimaryKey.Fingerprint...),
		binary.BigEndian.AppendUint64([]byte{9, 16}, signer.PrimaryKey.KeyId),
	)

	info, err := ParsePgpSignatureInfo(spoofed)
	require.NoError(t, err)
	require.Equal(t, fingerprintHex(victim.PrimaryKey.Fingerprint), info.Fingerprint)

	keyring := ParseVerificationKeys([][]byte{armoredPublicKey(t, victim), armoredPublicKey(t, signer)})
	status, fingerprint, err := keyring.VerifyPgpSignature(spoofed, signedText)
	require.NoError(t, err)
	require.Equal(t, PgpVerified, status)
	require.Equal(t, fingerprintHex(signer.PrimaryKey.Fingerprint), fingerprint)
}

// replaceUnhashedSubpackets rewrites the unhashed subpacket area of an armored
// v4 signature packet with the given encoded subpackets.
func replaceUnhashedSubpackets(t *testing.T, armoredSig []byte, subpackets ...[]byte) []byte {
	t.Helper()

	block, err := armor.Decode(bytes.NewReader(armoredSig))
	require.NoError(t, err)
	raw, err := io.ReadAll(block.Body)
	require.NoError(t, err)

	// Strip the new-format packet header written by go-crypto.
	require.Equal(t, byte(0xc2), raw[0])
	var body []byte
	switch l := raw[1]; {
	case l < 192:
		body = raw[2:]
	case l < 224:
		body = raw[3:]
	default:
		body = raw[6:]
	}
	require.Equal(t, byte(4), body[0])

	hashedEnd := 6 + int(binary.BigEndian.Uint16(body[4:6]))
	unhashedEnd := hashedEnd + 2 + int(binary.BigEndian.Uint16(body[hashedEnd:hashedEnd+2]))

	unhashed := bytes.Join(subpackets, nil)
	newBody := append([]byte{}, body[:hashedEnd]...)
	newBody = binary.BigEndian.AppendUint16(newBody, uint16(len(unhashed)))
	newBody = append(newBody, unhashed...)
	newBody = append(newBody, body[unhashedEnd:]...)

	var buf bytes.Buffer
	w, err := armor.Encode(&buf, openpgp.SignatureType, nil)
	require.NoError(t, err)
	_, err = w.Write(binary.BigEndian.AppendUint32([]byte{0xc2, 0xff}, uint32(len(newBody))))
	require.NoError(t, err)
	_, err = w.Write(newBody)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func newSignature(t *testing.T, message []byte) (*openpgp.Entity, []byte) {
	t.Helper()

	entity, err := openpgp.NewEntity("Test User", "", "test@example.com", nil)
	require.NoError(t, err)

	var sig bytes.Buffer
	require.NoError(t, openpgp.ArmoredDetachSign(&sig, entity, bytes.NewReader(message), &packet.Config{}))

	return entity, sig.Bytes()
}

func armoredPublicKey(t *testing.T, entity *openpgp.Entity) []byte {
	t.Helper()

	var buf bytes.Buffer
	w, err := armor.Encode(&buf, openpgp.PublicKeyType, nil)
	require.NoError(t, err)
	require.NoError(t, entity.Serialize(w))
	require.NoError(t, w.Close())

	return buf.Bytes()
}
