package user

// Stable error codes for the user/auth domain.
const (
	ErrCodeInvalidEmail           = "INVALID_EMAIL"
	ErrCodeInvalidPassword        = "INVALID_PASSWORD"
	ErrCodeEmailAlreadyRegistered = "EMAIL_ALREADY_REGISTERED"
	ErrCodeInvalidCredentials     = "INVALID_CREDENTIALS"
	ErrCodeInternal               = "INTERNAL_ERROR"
)

// ServiceError carries a stable code and a safe client-facing message.
type ServiceError struct {
	Code    string
	Message string
}

func (e *ServiceError) Error() string { return e.Code + ": " + e.Message }
