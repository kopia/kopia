package serverapi

type AuthStatusResponse struct {
	Authenticated    bool   `json:"authenticated"`
	Username         string `json:"username,omitempty"`
	TOTPRequired     bool   `json:"totpRequired,omitempty"`
	TOTPEnabled      bool   `json:"totpEnabled,omitempty"`
	PasskeyAvailable bool   `json:"passkeyAvailable,omitempty"`
	CSRFToken        string `json:"csrfToken,omitempty"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Status    string `json:"status"`
	Username  string `json:"username,omitempty"`
	CSRFToken string `json:"csrfToken,omitempty"`
}

type TOTPVerifyRequest struct {
	Code string `json:"code"`
}

type TOTPSetupBeginResponse struct {
	Secret        string `json:"secret"`
	QRCodeDataURL string `json:"qrCodeDataUrl"`
}

// MFAPasswordRequest is a step-up confirmation for sensitive MFA changes.
type MFAPasswordRequest struct {
	Password string `json:"password"`
}

// PasskeyDeleteRequest removes a registered passkey after password confirmation.
type PasskeyDeleteRequest struct {
	CredentialID string `json:"credentialId"`
	Password     string `json:"password"`
}

const (
	ErrorAuthFailed  APIErrorCode = "AUTH_FAILED"
	ErrorTOTPInvalid APIErrorCode = "TOTP_INVALID"
	ErrorMFAPending  APIErrorCode = "MFA_PENDING"
	ErrorRateLimited APIErrorCode = "RATE_LIMITED"
)
