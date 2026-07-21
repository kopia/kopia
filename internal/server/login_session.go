package server

import (
	"sync"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/google/uuid"

	"github.com/kopia/kopia/internal/clock"
)

const (
	loginSessionTTL      = 12 * time.Hour
	pendingMFASessionTTL = 5 * time.Minute
	webAuthnCeremonyTTL  = 5 * time.Minute
	sessionPurgeInterval = 5 * time.Minute
)

type loginSessionState int

const (
	loginSessionAnonymous loginSessionState = iota
	loginSessionPendingMFA
	loginSessionAuthenticated
)

type loginSession struct {
	ID                string
	Username          string
	State             loginSessionState
	CreatedAt         time.Time
	ExpiresAt         time.Time
	WebAuthnSession   *webauthn.SessionData
	PendingTOTPSecret string
}

type loginSessionManager struct {
	mu        sync.Mutex
	sessions  map[string]*loginSession
	lastPurge time.Time
}

func newLoginSessionManager() *loginSessionManager {
	return &loginSessionManager{
		sessions:  map[string]*loginSession{},
		lastPurge: clock.Now(),
	}
}

func (m *loginSessionManager) get(id string) *loginSession {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.maybePurgeLocked(clock.Now())

	s, ok := m.sessions[id]
	if !ok {
		return nil
	}

	if clock.Now().After(s.ExpiresAt) {
		delete(m.sessions, id)
		return nil
	}

	cp := *s
	if s.WebAuthnSession != nil {
		wa := *s.WebAuthnSession
		cp.WebAuthnSession = &wa
	}

	return &cp
}

func (m *loginSessionManager) put(s *loginSession) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := *s
	if s.WebAuthnSession != nil {
		wa := *s.WebAuthnSession
		cp.WebAuthnSession = &wa
	}

	m.sessions[s.ID] = &cp
}

func (m *loginSessionManager) delete(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.sessions, id)
}

func (m *loginSessionManager) markAuthenticated(oldSessionID, username string) *loginSession {
	now := clock.Now()
	s := &loginSession{
		ID:        newSessionID(),
		Username:  username,
		State:     loginSessionAuthenticated,
		CreatedAt: now,
		ExpiresAt: now.Add(loginSessionTTL),
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if oldSessionID != "" {
		delete(m.sessions, oldSessionID)
	}

	cp := *s
	m.sessions[s.ID] = &cp

	return s
}

func (m *loginSessionManager) markPendingMFA(oldSessionID, username string) *loginSession {
	now := clock.Now()
	s := &loginSession{
		ID:        newSessionID(),
		Username:  username,
		State:     loginSessionPendingMFA,
		CreatedAt: now,
		ExpiresAt: now.Add(pendingMFASessionTTL),
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if oldSessionID != "" {
		delete(m.sessions, oldSessionID)
	}

	cp := *s
	m.sessions[s.ID] = &cp

	return s
}

func (m *loginSessionManager) storeWebAuthnCeremony(sessionID string, data *webauthn.SessionData) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := clock.Now()
	s, ok := m.sessions[sessionID]
	if !ok {
		s = &loginSession{
			ID:        sessionID,
			State:     loginSessionAnonymous,
			CreatedAt: now,
			ExpiresAt: now.Add(webAuthnCeremonyTTL),
		}
		m.sessions[sessionID] = s
	}

	s.WebAuthnSession = data

	if s.State == loginSessionAnonymous || s.State == loginSessionPendingMFA {
		s.ExpiresAt = now.Add(webAuthnCeremonyTTL)
	}
}

func (m *loginSessionManager) takeWebAuthnCeremony(sessionID string) *webauthn.SessionData {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.sessions[sessionID]
	if !ok || s.WebAuthnSession == nil {
		return nil
	}

	if clock.Now().After(s.ExpiresAt) {
		delete(m.sessions, sessionID)
		return nil
	}

	data := s.WebAuthnSession
	s.WebAuthnSession = nil

	return data
}

func (m *loginSessionManager) storePendingTOTPSecret(sessionID, secret string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.sessions[sessionID]
	if !ok || s.State != loginSessionAuthenticated {
		return false
	}

	if clock.Now().After(s.ExpiresAt) {
		delete(m.sessions, sessionID)
		return false
	}

	s.PendingTOTPSecret = secret

	return true
}

func (m *loginSessionManager) takePendingTOTPSecret(sessionID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.sessions[sessionID]
	if !ok || s.State != loginSessionAuthenticated {
		return ""
	}

	secret := s.PendingTOTPSecret
	s.PendingTOTPSecret = ""

	return secret
}

func (m *loginSessionManager) maybePurgeLocked(now time.Time) {
	if now.Sub(m.lastPurge) < sessionPurgeInterval {
		return
	}

	m.lastPurge = now

	for id, s := range m.sessions {
		if now.After(s.ExpiresAt) {
			delete(m.sessions, id)
		}
	}
}

func newSessionID() string {
	return uuid.NewString()
}
