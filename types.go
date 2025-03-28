package migrate

import (
	"context"
	"database/sql"
	"fmt"
)

// StepFunc is a migration step operation. It is required to check the version prior to any
// operation by calling db.Version(), do its operations and then call db.SetVersion() if no
// error occurred. These operations should be wrapped in a transaction to ensure database
// integrity that should be rolled back in case of error of if dryRun is true.
type StepFunc func(ctx context.Context, db Database, info StepInfo, dryRun bool, log Logger) error

// Database is the interface to a database.
type Database interface {
	// InitVersion initialize the version information. Returns ErrAlreadyInitialized
	// if the database is already initialized. The ID of the given version MUST be 0.
	// Leaves the database unchanged if dryRun is true.
	InitVersion(ctx context.Context, v Version, dryRun bool) error

	// Version returns the current database version. Returns ErrNotInitialized if
	// the database is not initialized.
	Version(context.Context) (Version, error)

	// The DefaultStepFunc is called when the step function is nil. It changes the version
	// from info.From() to info.To() unless an error occurs or dryRun is true.
	DefaultStepFunc(ctx context.Context, info StepInfo, dryRun bool, log Logger) error
}

// StepInfo is a step information.
type StepInfo interface {
	fmt.Stringer

	// Name returns the step name.
	Name() string

	// From is the version of the database before the migrate step.
	From() Version

	// To is the version of the database after the migration step if there
	// is no error or DryRun is false.
	To() Version
}

// A Stepper manages migration steps.
type Stepper interface {
	// Len returns the number of steps.
	Len() int

	// Version returns the version of step ID. ID 0 is the initial state
	// of the database after it is initialized for migration. It is also
	// the state to which the database is returned with the migration
	// AllDown. It is the state where the database should be empty.
	Version(ID int) (Version, error)

	// Name returns the migration step name. ID 0 is the initial state
	// of the database after it is initialized for migration. It is also
	// to state to which the database is returned with the migration
	// AllDown. It is the state where the database should be empty.
	Name(ID int) (string, error)

	// Check returns an error if the given version is invalid. It may be
	// one of ErrBadVersionID when the ID is out of range, or ErrBadChecksum
	// if the checksum doesn't match the one in the stepper.
	Check(Version) error

	// Up returns the StepInfo and function for one step up migration
	// from the given version to the next version.
	// It returns a ErrBadVersion if the version is invalid or
	// ErrEndOfSteps if there are no more steps.
	Up(Version) (StepInfo, StepFunc, error)

	// Down returns the StepInfo and function for one step down migration
	// from the given version to the next version.
	// It returns a ErrBadVersion if the version is invalid or
	// ErrEndOfSteps if there are no more steps.
	Down(Version) (StepInfo, StepFunc, error)
}

// Migrater manages database migration steps.
type Migrater interface {
	// Init initializes the database version to v0 after verifying that it is not initialized.
	Init() error

	// InitDryRun simulates the database initialization to version v0 after verifying that
	// it is not initialized.
	InitDryRun() error

	// Version returns the current version of the database.
	Version() (Version, error)

	// OneUp executes one up step.
	OneUp(ctx context.Context) error

	// OneDown executes one down step.
	OneDown(ctx context.Context) error

	// OneUpDryRun simulates one up step.
	OneUpDryRun(ctx context.Context) error

	// OneDownDryRun simulates one down step.
	OneDownDryRun(ctx context.Context) error

	// AllUp executes all up steps.
	AllUp(ctx context.Context) error

	// AllDown executes all down steps.
	AllDown(ctx context.Context) error
}

// queries are the sqlite queries.
type Queries struct {
	// CreateTableQuery is the query to create a table holding its version.
	// The version is an integer ID and a 32 character string. The table
	// contains at most one row.
	CreateTableQuery string

	// InitTableQuery is the query to insert the database version.
	// The first parameter is the version ID which is an integer and the second
	// parameter is the checksum which is a 32 character string.
	InitTableQuery string

	// VersionQuery is the row query to get the database version. The
	// first value is the integer ID and the second is the 32 character
	// checksum value.
	VersionQuery string

	// SetVersionQuery is the update query to set the database version.
	// The first parameter is the version ID which is an integer and the second
	// parameter is the checksum which is a 32 character string.
	SetVersionQuery string // DB specific set version query.
}

// SQLTx is an sql database transaction handle.
type SQLTx interface {
	// Tx returns the sql transaction.
	Tx() *sql.Tx

	// FinalizeTransaction finalizes a transaction and should be called as a deferred
	// after starting the transaction. It commits the transaction when err is nil and dryRun
	// is false, otherwise it rolls back the transaction.
	FinalizeTransaction(err *error, dryRun bool)
}

// SQLDB is an sql database.
type SQLDB interface {
	Database

	// DB return the sql database handle.
	DB() *sql.DB

	// StartTransaction starts a transaction.
	StartTransaction(ctx context.Context, opts *sql.TxOptions) (SQLTx, error)

	// VersionTx returns the database version and is called in a transaction.
	VersionTx(tx SQLTx) (Version, error)

	// SetVersion changes the database version from info.From to info.To.
	SetVersion(ctx context.Context, info StepInfo, dryRun bool, log Logger) error

	// SetVersionTx changes the database version from info.From to info.To.
	// dryRun is provided for information purpose only.
	SetVersionTx(tx SQLTx, info StepInfo, dryRun bool, log Logger) error

	// Queries returns the database specific queries.
	Queries() *Queries
}

// Logger is a common logging interface.
type Logger interface {
	// Error logs an error level message.
	Error(msg string, fields ...Field)

	// Warn logs a warning level message.
	Warn(msg string, fields ...Field)

	// Info logs an info level message.
	Info(msg string, fields ...Field)

	// Debug logs an debug level message.
	Debug(msg string, fields ...Field)

	// SetLevel sets the log level.
	SetLevel(level LogLevel)

	// Level returns the log level.
	Level() LogLevel
}
