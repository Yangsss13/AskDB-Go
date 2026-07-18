// Package user handles registration, login, and user identity.
package user

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"
)

// TokenManager signs JWTs for authenticated users.
// Declared on the consuming side; implemented by auth.JWTManager.
type TokenManager interface {
	Sign(userID uint64) (token string, expiresAt time.Time, err error)
}

const (
	minPasswordBytes = 8
	maxPasswordBytes = 72
)

// dummyHash is a pre-computed bcrypt hash used when the email is not found
// during login, so the response time does not reveal account existence.
var dummyHash = func() []byte {
	h, _ := bcrypt.GenerateFromPassword([]byte("_askdb_dummy_login_2026"), bcrypt.DefaultCost)
	return h
}()

// AuthService handles user registration and login.
type AuthService struct {
	repo Repository
	tm   TokenManager
	now  func() time.Time
}

// NewAuthService wires the service dependencies.
func NewAuthService(repo Repository, tm TokenManager) *AuthService {
	return &AuthService{repo: repo, tm: tm, now: time.Now}
}

// Register creates a new user. email is trimmed and lowercased before storage.
// Password is NOT trimmed; its byte length must be 8–72.
func (s *AuthService) Register(ctx context.Context, email, password string) (*User, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if !isValidEmail(email) {
		return nil, &ServiceError{Code: ErrCodeInvalidEmail, Message: "invalid email address"}
	}
	if n := len(password); n < minPasswordBytes || n > maxPasswordBytes {
		return nil, &ServiceError{Code: ErrCodeInvalidPassword, Message: "password must be 8-72 bytes"}
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, &ServiceError{Code: ErrCodeInternal, Message: "internal error"}
	}

	now := s.now()
	u := &User{
		Email:        email,
		PasswordHash: string(hash),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.repo.Create(ctx, u); err != nil {
		if errors.Is(err, ErrDuplicateEmail) {
			return nil, &ServiceError{Code: ErrCodeEmailAlreadyRegistered, Message: "email already registered"}
		}
		return nil, &ServiceError{Code: ErrCodeInternal, Message: "internal error"}
	}
	return u, nil
}

// Login verifies credentials and returns a signed token.
// On any failure it returns ErrCodeInvalidCredentials to prevent enumeration.
func (s *AuthService) Login(ctx context.Context, email, password string) (token string, expiresAt time.Time, err error) {
	email = strings.ToLower(strings.TrimSpace(email))

	u, findErr := s.repo.FindByEmail(ctx, email)
	if findErr != nil {
		// Dummy compare to neutralise timing-based account enumeration.
		_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(password))
		return "", time.Time{}, &ServiceError{Code: ErrCodeInvalidCredentials, Message: "invalid email or password"}
	}

	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
		return "", time.Time{}, &ServiceError{Code: ErrCodeInvalidCredentials, Message: "invalid email or password"}
	}

	token, expiresAt, err = s.tm.Sign(u.ID)
	if err != nil {
		return "", time.Time{}, &ServiceError{Code: ErrCodeInternal, Message: "internal error"}
	}
	return token, expiresAt, nil
}

// isValidEmail is a minimal format check: non-empty local part, one @, non-empty domain.
func isValidEmail(email string) bool {
	if !utf8.ValidString(email) {
		return false
	}
	at := strings.Index(email, "@")
	if at <= 0 {
		return false
	}
	domain := email[at+1:]
	return len(domain) > 0 && strings.Contains(domain, ".")
}
