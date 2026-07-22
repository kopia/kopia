package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image/png"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"github.com/pquerna/otp"
	"github.com/pquerna/otp/totp"

	"github.com/kopia/kopia/internal/serverapi"
)

const totpQRSize = 200

func validateTOTPCode(secret, code string) bool {
	return totp.Validate(strings.TrimSpace(code), secret)
}

func encodeTOTPQR(key *otp.Key) (string, error) {
	img, err := key.Image(totpQRSize, totpQRSize)
	if err != nil {
		return "", errors.Wrap(err, "unable to render TOTP QR code")
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return "", errors.Wrap(err, "unable to encode TOTP QR code")
	}

	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func handleTOTPSetupBegin(_ context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	username, apiErr := requireUISessionUser(rc)
	if apiErr != nil {
		return nil, apiErr
	}

	key, err := totp.Generate(totp.GenerateOpts{
		Issuer:      "Kopia",
		AccountName: username,
	})
	if err != nil {
		return nil, internalServerError(err)
	}

	sessionID, err := sessionIDFromRequest(rc.req)
	if err != nil {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, authFailureMessage}
	}

	if !s.loginSessions.storePendingTOTPSecret(sessionID, key.Secret()) {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, authFailureMessage}
	}

	qrURL, err := encodeTOTPQR(key)
	if err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.TOTPSetupBeginResponse{
		Secret:        key.Secret(),
		QRCodeDataURL: qrURL,
	}, nil
}

func handleTOTPSetupConfirm(_ context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	username, apiErr := requireUISessionUser(rc)
	if apiErr != nil {
		return nil, apiErr
	}

	var req serverapi.TOTPVerifyRequest
	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	sessionID, err := sessionIDFromRequest(rc.req)
	if err != nil {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, authFailureMessage}
	}

	secret := s.loginSessions.takePendingTOTPSecret(sessionID)
	if secret == "" {
		return nil, requestError(serverapi.ErrorMalformedRequest, "TOTP setup not started")
	}

	if !validateTOTPCode(secret, req.Code) {
		_ = s.loginSessions.storePendingTOTPSecret(sessionID, secret)

		return nil, &apiError{http.StatusBadRequest, serverapi.ErrorTOTPInvalid, "invalid TOTP code"}
	}

	enc, err := s.mfaStore.encryptSecret(secret)
	if err != nil {
		return nil, internalServerError(err)
	}

	if err := s.mfaStore.update(username, func(u *mfaUserCredentials) error {
		u.TOTPSecretEnc = enc
		u.TOTPEnabled = true

		return nil
	}); err != nil {
		return nil, internalServerError(err)
	}

	return serverapi.Empty{}, nil
}

func handleTOTPDisable(ctx context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	username, apiErr := requireUISessionUser(rc)
	if apiErr != nil {
		return nil, apiErr
	}

	var req serverapi.MFAPasswordRequest
	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	if apiErr := s.requirePasswordStepUp(ctx, rc, username, req.Password); apiErr != nil {
		return nil, apiErr
	}

	if err := s.mfaStore.update(username, func(u *mfaUserCredentials) error {
		u.TOTPSecretEnc = ""
		u.TOTPEnabled = false

		return nil
	}); err != nil {
		return nil, internalServerError(err)
	}

	return serverapi.Empty{}, nil
}
