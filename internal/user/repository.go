package user

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// ErrNotFound is returned when no user matches the query.
var ErrNotFound = errors.New("user: not found")

// ErrDuplicateEmail is returned when a user with the same email already exists.
var ErrDuplicateEmail = errors.New("user: email already registered")

// Repository is the data access interface for users.
// Declared on the consuming side (service.go uses it).
type Repository interface {
	Create(ctx context.Context, u *User) error
	FindByEmail(ctx context.Context, email string) (*User, error)
}

// GORMRepository persists users in askdb_app via GORM.
type GORMRepository struct {
	db *gorm.DB
}

// NewGORMRepository returns a repository backed by the given GORM handle.
func NewGORMRepository(db *gorm.DB) *GORMRepository {
	return &GORMRepository{db: db}
}

// Create inserts a new user and populates its generated ID.
func (r *GORMRepository) Create(ctx context.Context, u *User) error {
	if err := r.db.WithContext(ctx).Create(u).Error; err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return ErrDuplicateEmail
		}
		return fmt.Errorf("user: create: %w", err)
	}
	return nil
}

// FindByEmail loads a user by email, returning ErrNotFound when absent.
func (r *GORMRepository) FindByEmail(ctx context.Context, email string) (*User, error) {
	var u User
	err := r.db.WithContext(ctx).Where("email = ?", email).First(&u).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("user: find by email: %w", err)
	}
	return &u, nil
}
