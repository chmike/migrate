package migrate

type Error string

const (
	// ErrBadParameters is returned when a function received invalid parameters.
	ErrBadParameters Error = "bad parameters"

	// ErrAlreadyInitialized is returned by Init if the database is already initialized.
	ErrAlreadyInitialized Error = "already initialized"

	// ErrBadVersionID is returned when Init found an out of range version ID in the database.
	ErrBadVersionID Error = "bad version identifier"

	// ErrBadVersionChecksum is returned when Init found a version with invalid checksum which is a
	// hint that the migration steps don't match the one used for the database. Don't change
	// any migration steps as it will result in changing checksums.
	ErrBadVersionChecksum Error = "bad version checksum"

	// ErrNotInitialized is returned when Init has not been called.
	ErrNotInitialized Error = "not initialized"

	// ErrBadVersion is returned when the database is not in the expected version.
	ErrBadVersion Error = "bad version"

	// ErrEndOfSteps is returned when OneUp or OneDown has no more more migration steps to perform.
	ErrEndOfSteps Error = "end of steps"

	// ErrNotSQLDB is returned a database is not an SQL database.
	ErrNotSQLDB Error = "not an SQL database"

	// ErrBeginTx is returned when starting a transaction fails.
	ErrBeginTx Error = "begin transaction"

	// ErrCommitTx is returned when a committing a transaction fails.
	ErrCommitTx Error = "commit transaction"

	// ErrRollbackTx is return when the transaction rollback failed.
	ErrRollbackTx Error = "rollback transaction"

	// ErrCancel is returned by a user function to force a dry run.
	ErrCancel Error = "cancel transaction"

	// ErrAbort is returned by a user function to aborts a transaction.
	ErrAbort Error = "abort transaction"
)

func (e Error) Error() string {
	return string(e)
}
