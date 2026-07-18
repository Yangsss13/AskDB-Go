// Package auth provides JWT signing and verification for the API server.
// The Worker does not import this package.
package auth

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ErrInvalidToken is returned by Verify for any token that cannot be trusted.
var ErrInvalidToken = errors.New("auth: invalid token")

// claims holds the JWT payload. Identity lives in the standard sub field only.
type claims struct {
	jwt.RegisteredClaims
}

// JWTManager signs and verifies HS256 JWTs.
// It is safe for concurrent use.
type JWTManager struct {
	secret    []byte
	issuer    string
	accessTTL time.Duration
	now       func() time.Time
}

// NewJWTManager returns a JWTManager. secret must be at least 32 bytes
// (enforced by config.Load before this is called).
func NewJWTManager(secret []byte, issuer string, accessTTL time.Duration) *JWTManager {
	return &JWTManager{
		secret:    secret,
		issuer:    issuer,
		accessTTL: accessTTL,
		now:       time.Now,
	}
}

// Sign issues a signed HS256 token for userID.
// Returns the token string and its expiry time.
func (m *JWTManager) Sign(userID uint64) (string, time.Time, error) {
	now := m.now()
	expiresAt := now.Add(m.accessTTL)

	c := claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   strconv.FormatUint(userID, 10),
			Issuer:    m.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	signed, err := tok.SignedString(m.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("auth: sign: %w", err)
	}
	return signed, expiresAt, nil
}

// Verify parses and validates tokenStr. It accepts only HS256 tokens with
// a valid exp, iat, and matching issuer. Returns the subject user ID.
func (m *JWTManager) Verify(tokenStr string) (uint64, error) {
	tok, err := jwt.ParseWithClaims(tokenStr, &claims{},
		func(t *jwt.Token) (any, error) { return m.secret, nil },
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithIssuer(m.issuer),
	)
	if err != nil || !tok.Valid {
		return 0, ErrInvalidToken
	}
	c, ok := tok.Claims.(*claims)
	if !ok || c.Subject == "" {
		return 0, ErrInvalidToken
	}
	// Require iat to be present (WithIssuedAt only validates it when set).
	if c.IssuedAt == nil {
		return 0, ErrInvalidToken
	}
	uid, err := strconv.ParseUint(c.Subject, 10, 64)
	if err != nil || uid == 0 {
		return 0, ErrInvalidToken
	}
	return uid, nil
}
