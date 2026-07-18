package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func newTestManager() *JWTManager {
	return NewJWTManager([]byte("test-secret-at-least-32-bytes-ok"), "askdb-api", time.Hour)
}

func TestJWTManager_SignVerify_Success(t *testing.T) {
	m := newTestManager()
	tok, exp, err := m.Sign(42)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if tok == "" {
		t.Fatal("expected non-empty token")
	}
	if exp.Before(time.Now()) {
		t.Error("expiry must be in the future")
	}

	uid, err := m.Verify(tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if uid != 42 {
		t.Errorf("userID: got %d, want 42", uid)
	}
}

func TestJWTManager_Verify_Expired(t *testing.T) {
	m := NewJWTManager([]byte("test-secret-at-least-32-bytes-ok"), "askdb-api", -time.Second)
	tok, _, err := m.Sign(1)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	_, err = m.Verify(tok)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestJWTManager_Verify_WrongIssuer(t *testing.T) {
	signer := NewJWTManager([]byte("test-secret-at-least-32-bytes-ok"), "other-issuer", time.Hour)
	verifier := NewJWTManager([]byte("test-secret-at-least-32-bytes-ok"), "askdb-api", time.Hour)

	tok, _, err := signer.Sign(1)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	_, err = verifier.Verify(tok)
	if err == nil {
		t.Fatal("expected error for wrong issuer")
	}
}

func TestJWTManager_Verify_WrongAlgorithm(t *testing.T) {
	// A none-alg token (header.payload.empty-sig) must be rejected.
	// Base64url: {"alg":"none","typ":"JWT"} . {"sub":"1","iss":"askdb-api","exp":9999999999}
	noneAlgToken := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0" +
		".eyJzdWIiOiIxIiwiaXNzIjoiYXNrZGItYXBpIiwiZXhwIjo5OTk5OTk5OTk5fQ."
	m := newTestManager()
	_, err := m.Verify(noneAlgToken)
	if err == nil {
		t.Fatal("expected error for alg:none token")
	}
}

func TestJWTManager_Verify_Tampered(t *testing.T) {
	m := newTestManager()
	tok, _, err := m.Sign(1)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	// Flip the last character of the signature.
	tampered := tok[:len(tok)-1] + "X"
	_, err = m.Verify(tampered)
	if err == nil {
		t.Fatal("expected error for tampered signature")
	}
}

func TestJWTManager_Verify_Empty(t *testing.T) {
	m := newTestManager()
	_, err := m.Verify("")
	if err == nil {
		t.Fatal("expected error for empty token")
	}
}

func TestJWTManager_Verify_MissingIssuedAt(t *testing.T) {
	// A token with valid alg/exp/iss/sub but no iat must be rejected, since
	// iat is a strictly-required claim.
	secret := []byte("test-secret-at-least-32-bytes-ok")
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   "1",
		Issuer:    "askdb-api",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		// IssuedAt deliberately omitted.
	})
	signed, err := tok.SignedString(secret)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	m := newTestManager()
	if _, err := m.Verify(signed); err == nil {
		t.Fatal("expected error for token missing iat")
	}
}

func TestJWTManager_Verify_MissingSubject(t *testing.T) {
	// A token with no sub must be rejected.
	secret := []byte("test-secret-at-least-32-bytes-ok")
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Issuer:    "askdb-api",
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		// Subject deliberately omitted.
	})
	signed, err := tok.SignedString(secret)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	m := newTestManager()
	if _, err := m.Verify(signed); err == nil {
		t.Fatal("expected error for token missing sub")
	}
}
