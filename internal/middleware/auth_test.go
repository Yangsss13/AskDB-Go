package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// fakeVerifier is a hand-written fake for Verifier.
type fakeVerifier struct {
	uid uint64
	err error
}

func (f *fakeVerifier) Verify(_ string) (uint64, error) {
	return f.uid, f.err
}

var errFakeInvalid = errors.New("fake: invalid token")

func setupRouter(v Verifier) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	protected := r.Group("/protected")
	protected.Use(Bearer(v))
	protected.GET("/resource", func(c *gin.Context) {
		uid := UserID(c)
		c.JSON(http.StatusOK, gin.H{"user_id": uid})
	})
	return r
}

func TestBearer_MissingHeader_401(t *testing.T) {
	r := setupRouter(&fakeVerifier{uid: 1})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected/resource", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("got %d, want 401", w.Code)
	}
}

func TestBearer_WrongScheme_401(t *testing.T) {
	r := setupRouter(&fakeVerifier{uid: 1})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected/resource", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("got %d, want 401", w.Code)
	}
}

func TestBearer_InvalidToken_401(t *testing.T) {
	r := setupRouter(&fakeVerifier{err: errFakeInvalid})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected/resource", nil)
	req.Header.Set("Authorization", "Bearer bad.token.here")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("got %d, want 401", w.Code)
	}
}

func TestBearer_ValidToken_200(t *testing.T) {
	r := setupRouter(&fakeVerifier{uid: 7})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected/resource", nil)
	req.Header.Set("Authorization", "Bearer valid.token.here")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("got %d, want 200", w.Code)
	}
}
