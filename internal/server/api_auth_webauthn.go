package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
)

func handleWebAuthnLoginBegin(_ context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	if err := s.requireLoginRateLimit(rc.req); err != nil {
		return nil, err
	}

	wa, err := s.webAuthnForRequest(rc.req)
	if err != nil {
		return nil, internalServerError(err)
	}

	assertion, sessionData, err := wa.BeginDiscoverableLogin()
	if err != nil {
		return nil, internalServerError(errors.Wrap(err, "begin passkey login"))
	}

	sessionID := ensureSessionCookie(rc.w, rc.req)
	s.loginSessions.storeWebAuthnCeremony(sessionID, sessionData)

	return assertion, nil
}

func handleWebAuthnLoginFinish(ctx context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	if err := s.requireLoginRateLimit(rc.req); err != nil {
		return nil, err
	}

	sessionID, err := sessionIDFromRequest(rc.req)
	if err != nil {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, authFailureMessage}
	}

	sessionData := s.loginSessions.takeWebAuthnCeremony(sessionID)
	if sessionData == nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "passkey login not started")
	}

	wa, err := s.webAuthnForRequest(rc.req)
	if err != nil {
		return nil, internalServerError(err)
	}

	req := rc.req.Clone(ctx)
	req.Body = io.NopCloser(bytes.NewReader(rc.body))

	user, credential, err := wa.FinishPasskeyLogin(func(_, userHandle []byte) (webauthn.User, error) {
		username, creds := s.mfaStore.findByWebAuthnUserID(userHandle)
		if username == "" {
			return nil, errors.New("unknown passkey user")
		}

		if s.options.UIUser != "" && username != s.options.UIUser {
			return nil, errors.New("passkey user not allowed for UI")
		}

		return &webAuthnUser{username: username, creds: creds}, nil
	}, *sessionData, req)
	if err != nil {
		s.loginLimiter.failure(clientRateLimitKey(rc.req))
		userLog(ctx).Warnf("failed passkey login by client %s", rc.req.RemoteAddr)

		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, "passkey verification failed"}
	}

	waUser, ok := user.(*webAuthnUser)
	if !ok {
		return nil, internalServerError(errors.New("unexpected WebAuthn user type"))
	}

	if err := s.mfaStore.update(waUser.username, func(u *mfaUserCredentials) error {
		for i := range u.Passkeys {
			if bytes.Equal(u.Passkeys[i].ID, credential.ID) {
				u.Passkeys[i].Authenticator.UpdateCounter(credential.Authenticator.SignCount)
				return nil
			}
		}

		return nil
	}); err != nil {
		return nil, internalServerError(err)
	}

	return s.completeUILogin(ctx, rc, clientRateLimitKey(rc.req), sessionID, waUser.username), nil
}

func handleWebAuthnRegisterBegin(ctx context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	username, apiErr := requireUISessionUser(rc)
	if apiErr != nil {
		return nil, apiErr
	}

	var req serverapi.MFAPasswordRequest
	if len(rc.body) > 0 && string(rc.body) != "{}" {
		if err := json.Unmarshal(rc.body, &req); err != nil {
			return nil, unableToDecodeRequest(err)
		}
	}

	if apiErr := s.requirePasswordStepUp(ctx, rc, username, req.Password); apiErr != nil {
		return nil, apiErr
	}

	creds, err := s.mfaStore.getOrCreate(username)
	if err != nil {
		return nil, internalServerError(err)
	}

	user := &webAuthnUser{username: username, creds: creds}

	wa, err := s.webAuthnForRequest(rc.req)
	if err != nil {
		return nil, internalServerError(err)
	}

	creation, sessionData, err := wa.BeginRegistration(user,
		webauthn.WithResidentKeyRequirement(protocol.ResidentKeyRequirementRequired),
		webauthn.WithExclusions(webauthn.Credentials(user.WebAuthnCredentials()).CredentialDescriptors()),
	)
	if err != nil {
		return nil, internalServerError(errors.Wrap(err, "begin passkey registration"))
	}

	sessionID, err := sessionIDFromRequest(rc.req)
	if err != nil {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, authFailureMessage}
	}

	s.loginSessions.storeWebAuthnCeremony(sessionID, sessionData)

	return creation, nil
}

func handleWebAuthnRegisterFinish(ctx context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	username, apiErr := requireUISessionUser(rc)
	if apiErr != nil {
		return nil, apiErr
	}

	sessionID, err := sessionIDFromRequest(rc.req)
	if err != nil {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, authFailureMessage}
	}

	sessionData := s.loginSessions.takeWebAuthnCeremony(sessionID)
	if sessionData == nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "passkey registration not started")
	}

	creds, err := s.mfaStore.getOrCreate(username)
	if err != nil {
		return nil, internalServerError(err)
	}

	user := &webAuthnUser{username: username, creds: creds}

	wa, err := s.webAuthnForRequest(rc.req)
	if err != nil {
		return nil, internalServerError(err)
	}

	req := rc.req.Clone(ctx)
	req.Body = io.NopCloser(bytes.NewReader(rc.body))

	credential, err := wa.FinishRegistration(user, *sessionData, req)
	if err != nil {
		userLog(ctx).Warnf("passkey registration failed for %s: %v", username, err)

		return nil, requestError(serverapi.ErrorMalformedRequest, "passkey registration failed")
	}

	if err := s.mfaStore.update(username, func(u *mfaUserCredentials) error {
		u.Passkeys = append(u.Passkeys, *credential)
		return nil
	}); err != nil {
		return nil, internalServerError(err)
	}

	return serverapi.Empty{}, nil
}

func handleWebAuthnDelete(ctx context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	username, apiErr := requireUISessionUser(rc)
	if apiErr != nil {
		return nil, apiErr
	}

	var req serverapi.PasskeyDeleteRequest
	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	if apiErr := s.requirePasswordStepUp(ctx, rc, username, req.Password); apiErr != nil {
		return nil, apiErr
	}

	credID, err := base64.RawURLEncoding.DecodeString(req.CredentialID)
	if err != nil || len(credID) == 0 {
		return nil, requestError(serverapi.ErrorMalformedRequest, "invalid credential id")
	}

	removed := false

	if err := s.mfaStore.update(username, func(u *mfaUserCredentials) error {
		kept := u.Passkeys[:0]
		for _, pk := range u.Passkeys {
			if bytes.Equal(pk.ID, credID) {
				removed = true
				continue
			}

			kept = append(kept, pk)
		}

		u.Passkeys = kept

		return nil
	}); err != nil {
		return nil, internalServerError(err)
	}

	if !removed {
		return nil, requestError(serverapi.ErrorMalformedRequest, "passkey not found")
	}

	return serverapi.Empty{}, nil
}
