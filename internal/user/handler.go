package user

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// authService is the narrow dependency the handler requires.
type authService interface {
	Register(ctx context.Context, email, password string) (*User, error)
	Login(ctx context.Context, email, password string) (token string, expiresAt time.Time, err error)
}

// registerRequest is the POST body for /api/v1/auth/register.
type registerRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// registerResponse is the 201 response for successful registration.
type registerResponse struct {
	UserID    uint64    `json:"user_id"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

// loginRequest is the POST body for /api/v1/auth/login.
type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// loginResponse is the 200 response for successful login.
type loginResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// authErrorResponse is the error envelope for auth endpoints.
type authErrorResponse struct {
	Error string `json:"error"`
}

// AuthHandler handles the auth HTTP endpoints.
type AuthHandler struct {
	svc authService
}

// NewAuthHandler returns a handler backed by the given service.
func NewAuthHandler(svc authService) *AuthHandler {
	return &AuthHandler{svc: svc}
}

// Register handles POST /api/v1/auth/register.
func (h *AuthHandler) Register(c *gin.Context) {
	var req registerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Error: "invalid_request"})
		return
	}

	u, err := h.svc.Register(c.Request.Context(), req.Email, req.Password)
	if err != nil {
		var svcErr *ServiceError
		if errors.As(err, &svcErr) {
			c.JSON(authErrStatus(svcErr.Code), authErrorResponse{Error: svcErr.Code})
			return
		}
		c.JSON(http.StatusInternalServerError, authErrorResponse{Error: ErrCodeInternal})
		return
	}

	c.JSON(http.StatusCreated, registerResponse{
		UserID:    u.ID,
		Email:     u.Email,
		CreatedAt: u.CreatedAt,
	})
}

// Login handles POST /api/v1/auth/login.
func (h *AuthHandler) Login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Error: "invalid_request"})
		return
	}

	token, expiresAt, err := h.svc.Login(c.Request.Context(), req.Email, req.Password)
	if err != nil {
		var svcErr *ServiceError
		if errors.As(err, &svcErr) {
			c.JSON(authErrStatus(svcErr.Code), authErrorResponse{Error: svcErr.Code})
			return
		}
		c.JSON(http.StatusInternalServerError, authErrorResponse{Error: ErrCodeInternal})
		return
	}

	c.JSON(http.StatusOK, loginResponse{Token: token, ExpiresAt: expiresAt})
}

// authErrStatus maps stable service error codes to HTTP statuses.
func authErrStatus(code string) int {
	switch code {
	case ErrCodeInvalidEmail, ErrCodeInvalidPassword:
		return http.StatusBadRequest
	case ErrCodeEmailAlreadyRegistered:
		return http.StatusConflict
	case ErrCodeInvalidCredentials:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}
