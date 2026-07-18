// Package middleware provides reusable Gin middleware for the API server.
package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// Verifier validates a JWT token string and returns the subject user ID.
// Implemented by auth.JWTManager; declared here on the consuming side.
type Verifier interface {
	Verify(tokenStr string) (userID uint64, err error)
}

const userIDKey = "userID"

// Bearer returns a Gin middleware that validates HS256 Bearer tokens.
// On success it stores the authenticated user ID in the context under
// the key accessible via UserID(c). On any failure it aborts with 401.
func Bearer(v Verifier) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			abort401(c)
			return
		}
		tokenStr := strings.TrimPrefix(header, "Bearer ")
		uid, err := v.Verify(tokenStr)
		if err != nil {
			abort401(c)
			return
		}
		c.Set(userIDKey, uid)
		c.Next()
	}
}

// UserID extracts the authenticated user ID stored by Bearer.
// Panics when called outside a Bearer-protected route (programming error).
func UserID(c *gin.Context) uint64 {
	uid, _ := c.Get(userIDKey)
	return uid.(uint64)
}

func abort401(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
}
