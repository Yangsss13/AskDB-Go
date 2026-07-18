package user

import "time"

// User is the GORM persistence model for the users table in askdb_app.
// PasswordHash must never appear in HTTP responses or logs.
type User struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Email        string    `gorm:"column:email"`
	PasswordHash string    `gorm:"column:password_hash"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}

// TableName pins the table name so GORM does not pluralize unexpectedly.
func (User) TableName() string { return "users" }
