package user

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// --- hand-written fakes ---

type fakeRepo struct {
	created   *User
	createErr error
	findUser  *User
	findErr   error
}

func (f *fakeRepo) Create(_ context.Context, u *User) error {
	if f.createErr != nil {
		return f.createErr
	}
	u.ID = 1
	u.CreatedAt = time.Now()
	u.UpdatedAt = u.CreatedAt
	f.created = u
	return nil
}

func (f *fakeRepo) FindByEmail(_ context.Context, _ string) (*User, error) {
	return f.findUser, f.findErr
}

type fakeTokenManager struct {
	token     string
	expiresAt time.Time
	err       error
}

func (f *fakeTokenManager) Sign(_ uint64) (string, time.Time, error) {
	return f.token, f.expiresAt, f.err
}

func newSvcWithTM(repo Repository, tm TokenManager) *AuthService {
	return NewAuthService(repo, tm)
}

func newSvc(repo Repository) *AuthService {
	return newSvcWithTM(repo, &fakeTokenManager{
		token:     "signed.token",
		expiresAt: time.Now().Add(time.Hour),
	})
}

// makeHash returns a bcrypt hash of the password at DefaultCost.
func makeHash(pw string) string {
	h, _ := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(h)
}

// --- Register tests ---

func TestAuthService_Register_Success(t *testing.T) {
	repo := &fakeRepo{}
	u, err := newSvc(repo).Register(context.Background(), "  User@Example.COM  ", "password123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.Email != "user@example.com" {
		t.Errorf("email not normalised: %q", u.Email)
	}
	if repo.created == nil {
		t.Fatal("expected Create to be called")
	}
}

func TestAuthService_Register_DuplicateEmail(t *testing.T) {
	repo := &fakeRepo{createErr: ErrDuplicateEmail}
	_, err := newSvc(repo).Register(context.Background(), "a@b.com", "password123")
	var svcErr *ServiceError
	if !errors.As(err, &svcErr) || svcErr.Code != ErrCodeEmailAlreadyRegistered {
		t.Errorf("expected EMAIL_ALREADY_REGISTERED, got %v", err)
	}
}

func TestAuthService_Register_InvalidEmail(t *testing.T) {
	for _, bad := range []string{"", "notanemail", "@nodomain", "no-at"} {
		_, err := newSvc(&fakeRepo{}).Register(context.Background(), bad, "password123")
		var svcErr *ServiceError
		if !errors.As(err, &svcErr) || svcErr.Code != ErrCodeInvalidEmail {
			t.Errorf("email %q: expected INVALID_EMAIL, got %v", bad, err)
		}
	}
}

func TestAuthService_Register_PasswordTooShort(t *testing.T) {
	_, err := newSvc(&fakeRepo{}).Register(context.Background(), "a@b.com", "short")
	var svcErr *ServiceError
	if !errors.As(err, &svcErr) || svcErr.Code != ErrCodeInvalidPassword {
		t.Errorf("expected INVALID_PASSWORD for short password, got %v", err)
	}
}

func TestAuthService_Register_PasswordMaxBytes(t *testing.T) {
	// 72 bytes is the bcrypt limit; 73 must be rejected.
	pw73 := strings.Repeat("a", 73)
	_, err := newSvc(&fakeRepo{}).Register(context.Background(), "a@b.com", pw73)
	var svcErr *ServiceError
	if !errors.As(err, &svcErr) || svcErr.Code != ErrCodeInvalidPassword {
		t.Errorf("expected INVALID_PASSWORD for 73-byte password, got %v", err)
	}
}

// --- Login tests ---

func TestAuthService_Login_Success(t *testing.T) {
	hash := makeHash("correct_pass")
	repo := &fakeRepo{findUser: &User{ID: 5, Email: "a@b.com", PasswordHash: hash}}
	svc := newSvc(repo)

	tok, exp, err := svc.Login(context.Background(), "A@B.COM", "correct_pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok != "signed.token" {
		t.Errorf("token: got %q", tok)
	}
	if exp.IsZero() {
		t.Error("expiresAt must not be zero")
	}
}

func TestAuthService_Login_WrongPassword(t *testing.T) {
	hash := makeHash("correct_pass")
	repo := &fakeRepo{findUser: &User{ID: 1, Email: "a@b.com", PasswordHash: hash}}
	_, _, err := newSvc(repo).Login(context.Background(), "a@b.com", "wrong_pass")
	assertInvalidCredentials(t, err)
}

func TestAuthService_Login_UnknownEmail(t *testing.T) {
	repo := &fakeRepo{findErr: ErrNotFound}
	_, _, err := newSvc(repo).Login(context.Background(), "nobody@b.com", "password123")
	assertInvalidCredentials(t, err)
}

func TestAuthService_Login_SameErrorCodeForBothFailures(t *testing.T) {
	// Both "email not found" and "wrong password" must return the same code.
	hash := makeHash("correct")
	repoWrongPw := &fakeRepo{findUser: &User{ID: 1, Email: "a@b.com", PasswordHash: hash}}
	repoMissing := &fakeRepo{findErr: ErrNotFound}

	codeFor := func(svc *AuthService, email, pw string) string {
		_, _, err := svc.Login(context.Background(), email, pw)
		var e *ServiceError
		if errors.As(err, &e) {
			return e.Code
		}
		return ""
	}

	c1 := codeFor(newSvc(repoWrongPw), "a@b.com", "wrong")
	c2 := codeFor(newSvc(repoMissing), "nobody@b.com", "password123")
	if c1 != c2 || c1 != ErrCodeInvalidCredentials {
		t.Errorf("error codes differ: %q vs %q (both must be %s)", c1, c2, ErrCodeInvalidCredentials)
	}
}

func assertInvalidCredentials(t *testing.T, err error) {
	t.Helper()
	var svcErr *ServiceError
	if !errors.As(err, &svcErr) || svcErr.Code != ErrCodeInvalidCredentials {
		t.Errorf("expected INVALID_CREDENTIALS, got %v", err)
	}
}
