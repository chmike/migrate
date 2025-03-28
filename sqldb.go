package migrate

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Replace replaces all occurrences of defaultTableName with newTableName in the queries.
func (q *Queries) Replace(defaultTableName, newTableName string) {
	q.CreateTableQuery = strings.ReplaceAll(q.CreateTableQuery, defaultTableName, newTableName)
	q.InitTableQuery = strings.ReplaceAll(q.InitTableQuery, defaultTableName, newTableName)
	q.VersionQuery = strings.ReplaceAll(q.VersionQuery, defaultTableName, newTableName)
	q.SetVersionQuery = strings.ReplaceAll(q.SetVersionQuery, defaultTableName, newTableName)
}

// NewSQLDB returns an SQLDB
func NewSQLDB(db *sql.DB, q *Queries) *sqlDB {
	return &sqlDB{db: db, q: q}
}

type sqlTx struct {
	tx *sql.Tx
}

func (s sqlTx) Tx() *sql.Tx {
	return s.tx
}

var _ SQLDB = &sqlDB{}

type sqlDB struct {
	db *sql.DB  // DB is an sql database.
	q  *Queries // DB specific queries
}

func (db *sqlDB) DB() *sql.DB       { return db.db }
func (db *sqlDB) Queries() *Queries { return db.q }

// StartTransaction starts a transaction. It must be followed by a defer FinalizeTransaction.
func (db *sqlDB) StartTransaction(ctx context.Context, opts *sql.TxOptions) (SQLTx, error) {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBeginTx, err)
	}
	return &sqlTx{tx: tx}, nil
}

// FinalizeTransaction is intended to be called as deferred function after a successful call
// to StartTransaction.
func (tx *sqlTx) FinalizeTransaction(err *error, dryRun bool) {
	if *err != nil || dryRun {
		if rollbackErr := tx.Tx().Rollback(); rollbackErr != nil {
			if *err != nil {
				*err = fmt.Errorf("%w; %w: %w", *err, ErrRollbackTx, rollbackErr)
			} else {
				*err = fmt.Errorf("%w: %w", ErrRollbackTx, rollbackErr)
			}
		}
	} else {
		if commitErr := tx.Tx().Commit(); commitErr != nil {
			*err = fmt.Errorf("%w: %w", ErrCommitTx, commitErr)
		}
	}
}

// InitVersion initialize the version information. Returns ErrAlreadyInitialized
// if the database is already initialized. The ID of the given version MUST be 0.
func (db *sqlDB) InitVersion(ctx context.Context, v Version, dryRun bool) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", ErrNotInitialized, err)
		}
	}()
	tx, err := db.StartTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return
	}
	defer tx.FinalizeTransaction(&err, dryRun)

	_, err = tx.Tx().Exec(db.q.CreateTableQuery)
	if err != nil {
		return err
	}

	_, err = tx.Tx().Exec(db.q.InitTableQuery, v.ID, hex.EncodeToString(v.Checksum[:]))
	if err != nil {
		return err
	}

	return nil
}

// Version returns the current database version. Returns ErrNotInitialized if
// the database is not initialized.
func (db *sqlDB) Version(ctx context.Context) (v Version, err error) {
	tx, err := db.StartTransaction(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
	if err != nil {
		return badVersion, err
	}
	defer tx.FinalizeTransaction(&err, false)
	return db.VersionTx(tx)
}

// Version returns the current database version. Returns ErrNotInitialized if
// the database is not initialized.
func (db *sqlDB) VersionTx(tx SQLTx) (Version, error) {
	var id int
	var checksum string
	err := tx.Tx().QueryRow(db.q.VersionQuery).Scan(&id, &checksum)
	if err != nil {
		return Version{}, fmt.Errorf("%w: %w", ErrNotInitialized, err)
	}
	return MakeVersion(id, checksum)
}

// DefaultStepFunc is called when the step function is nil. It sets the version to info.To()
// when the database version is info.From() and dryRun is false, otherwise it returns ErrBadVersion.
func (db *sqlDB) DefaultStepFunc(ctx context.Context, info StepInfo, dryRun bool, log Logger) error {
	if log.Level() >= LevelDebug {
		log.Debug("nil migration step", F("name", info.Name()), F("from", info.From()), F("to", info.To()))
	}
	return db.SetVersion(ctx, info, dryRun, log)
}

// SetVersion is called when the step function is nil. It sets the version to info.To()
// when the database version is info.From() and dryRun is false, otherwise it returns ErrBadVersion.
func (db *sqlDB) SetVersion(ctx context.Context, info StepInfo, dryRun bool, log Logger) (err error) {
	tx, err := db.StartTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.FinalizeTransaction(&err, dryRun)

	return db.SetVersionTx(tx, info, dryRun, log)
}

// SetVersionTx in a transaction to set the version to info.To() if it is
// info.From() otherwise, returns an error.
func (db *sqlDB) SetVersionTx(tx SQLTx, info StepInfo, dryRun bool, log Logger) error {
	result, err := tx.Tx().Exec(db.q.SetVersionQuery,
		info.To().ID, info.To().ChecksumString(),
		info.From().ID, info.From().ChecksumString(),
	)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err == nil && rowsAffected != 1 {
		err = ErrBadVersion
	}
	return err
}

// SQLCommand is an SQL query instruction with arguments.
type SQLCommand struct {
	Cmd  string
	Args []any
}

func (c SQLCommand) String() string {
	if len(c.Args) == 0 {
		return c.Cmd
	} else {
		var buf strings.Builder
		buf.WriteRune('`')
		buf.WriteString(c.Cmd)
		buf.WriteRune('`')
		buf.WriteString(" args:[")
		for i, arg := range c.Args {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("%v", arg))
		}
		buf.WriteRune(']')
		return buf.String()
	}
}

// Cmd is a function simplifying the creation of a Command.
func Cmd(cmd string, args ...any) SQLCommand {
	return SQLCommand{Cmd: cmd, Args: args}
}

// Tx returns a migration step function that executes all the SQL commands in
// sequence wrapped in a transaction. The execution stops and rolls back as soon
// as an error is returned by one of the commands. It is also rolled back when dryRun
// is true.
func Tx(cmds ...SQLCommand) StepFunc {
	return func(ctx context.Context, gdb Database, info StepInfo, dryRun bool, log Logger) (err error) {
		db, ok := gdb.(SQLDB)
		if !ok {
			return fmt.Errorf("tx: %w", ErrNotSQLDB)
		}
		defer func() {
			if err != nil {
				log.Error("tx sql command", F("name", info.Name()), F("from", info.From()), F("to", info.To()), F("error", err.Error()))
			}
		}()
		defer func() {
			if err != nil {
				if dryRun {
					err = fmt.Errorf("tx %v -> %v dry run: %w", info.From(), info.To(), err)
				} else {
					err = fmt.Errorf("tx %v -> %v: %w", info.From(), info.To(), err)
				}
			}
		}()

		tx, err := db.StartTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.FinalizeTransaction(&err, dryRun)

		dbv, err := db.VersionTx(tx)
		if err != nil {
			return err
		}
		if dbv != info.From() {
			return fmt.Errorf("db is %v", dbv)
		}

		for _, cmd := range cmds {
			if log.Level() >= LevelDebug {
				log.Debug("tx sql command", F("cmd", cmd))
			}
			if _, err = tx.Tx().Exec(cmd.Cmd, cmd.Args...); err != nil {
				return err
			}
		}

		if err := db.SetVersionTx(tx, info, dryRun, log); err != nil {
			return err
		}
		log.Info("migrate step", F("name", info.Name()), F("from", info.From()), F("to", info.To()), F("dryRun", dryRun))
		return nil
	}
}

// NoTx returns a migration step function that executes the SQL commands in sequence
// without a wrapping transaction. It terminates as soon as a command returns an error.
// It doesn't execute any cmds when dryRun is true.
func NoTx(cmds ...SQLCommand) StepFunc {
	return func(ctx context.Context, gdb Database, info StepInfo, dryRun bool, log Logger) (err error) {
		db, ok := gdb.(SQLDB)
		if !ok {
			return fmt.Errorf("sql: %w", ErrNotSQLDB)
		}
		if dryRun {
			return nil
		}
		defer func() {
			if err != nil {
				log.Error("tx sql command", F("name", info.Name()), F("from", info.From()), F("to", info.To()), F("error", err.Error()))
			}
		}()
		defer func() {
			if err != nil {
				err = fmt.Errorf("sql %v -> %v: %w", info.From(), info.To(), err)
			}
		}()

		dbv, err := db.Version(ctx)
		if err != nil {
			return err
		}
		if dbv != info.From() {
			return fmt.Errorf("db is %v", dbv)
		}
		for _, cmd := range cmds {
			if log.Level() >= LevelDebug {
				log.Debug("no tx sql command", F("cmd", cmd))
			}
			if _, err = db.DB().ExecContext(ctx, cmd.Cmd, cmd.Args...); err != nil {
				return err
			}
		}

		if err := db.SetVersion(ctx, info, dryRun, log); err != nil {
			return err
		}
		log.Info("migrate step", F("name", info.Name()), F("from", info.From()), F("to", info.To()), F("dryRun", dryRun))
		return nil
	}
}

// TxFunc is a user provided function that is called wrapped in a transaction.
type TxFunc func(tx SQLTx, info StepInfo, dryRun bool, log Logger) error

// TxF returns a migration step function that executes all the user provided functions in
// sequence wrapped in a transaction. The execution stops and rolls back as soon
// as an error is returned by one of the function and the step function returns the error.
//
// A user function may return the ErrAbort pseudo error to force a termination of the
// function execution and the AllUp or AllDown execution which will return the ErrAbort
// error. To force a roll back of the transaction without terminating the execution of
// subsequent functions and migration steps, it must return the ErrCancel pseudo error.
// The migration step function will return nil as error.
func TxF(fs ...TxFunc) StepFunc {
	return func(ctx context.Context, gdb Database, info StepInfo, dryRun bool, log Logger) (err error) {
		db, ok := gdb.(SQLDB)
		if !ok {
			return fmt.Errorf("txf: %w", ErrNotSQLDB)
		}
		defer func() {
			if err != nil {
				log.Error("tx sql command", F("name", info.Name()), F("from", info.From()), F("to", info.To()), F("error", err.Error()))
			}
		}()
		defer func() {
			if err != nil {
				if errors.Is(err, ErrCancel) {
					err = nil
				} else if dryRun {
					err = fmt.Errorf("txf %v -> %v dry run: %w", info.From(), info.To(), err)
				} else {
					err = fmt.Errorf("txf %v -> %v: %w", info.From(), info.To(), err)
				}
			}
		}()

		tx, err := db.StartTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.FinalizeTransaction(&err, dryRun)

		dbv, err := db.VersionTx(tx)
		if err != nil {
			return err
		}
		if dbv != info.From() {
			return fmt.Errorf("db is %v", dbv)
		}

		if log.Level() >= LevelDebug {
			log.Debug("tx func commands", F("count", len(fs)))
		}
		var cancel bool
		for _, f := range fs {
			if err = f(tx, info, dryRun, log); err != nil {
				if !errors.Is(err, ErrCancel) {
					return err
				}
				cancel = true
			}
		}
		if cancel {
			return ErrCancel
		}

		if err := db.SetVersionTx(tx, info, dryRun, log); err != nil {
			return err
		}
		log.Info("migrate step", F("name", info.Name()), F("from", info.From()), F("to", info.To()), F("dryRun", dryRun))
		return nil
	}
}

// Func is a user provided function executed outside a transaction. It doesn't support dry run
// and the changes to the database won't be cancelled when the function returns an error.
type NoTxFunc func(ctx context.Context, db SQLDB, info StepInfo, log Logger) error

// NoTxF returns a migration step function that executes the user provided functions in sequence
// without a wrapping transaction. It terminates as soon as a function returns an error.
// It doesn't execute any function when dryRun is true. The pseudo error ErrCancel is
// treated as ErrAbort as operations can't be cancelled and database version remains info.From().
//
// Use with care as any error in the function may leave the database is an undefined state.
func NoTxF(fs ...NoTxFunc) StepFunc {
	return func(ctx context.Context, gdb Database, info StepInfo, dryRun bool, log Logger) (err error) {
		db, ok := gdb.(SQLDB)
		if !ok {
			return fmt.Errorf("f: %w", ErrNotSQLDB)
		}
		if dryRun {
			return nil
		}
		defer func() {
			if err != nil {
				log.Error("tx sql command", F("name", info.Name()), F("from", info.From()), F("to", info.To()), F("error", err.Error()))
			}
		}()
		defer func() {
			if err != nil {
				err = fmt.Errorf("sql %v -> %v: %w", info.From(), info.To(), err)
			}
		}()

		dbv, err := db.Version(ctx)
		if err != nil {
			return err
		}
		if dbv != info.From() {
			return fmt.Errorf("db is %v", dbv)
		}

		for _, f := range fs {
			if err = f(ctx, db, info, log); err != nil {
				return err
			}
		}

		if err := db.SetVersion(ctx, info, dryRun, log); err != nil {
			return err
		}
		log.Info("migrate step", F("name", info.Name()), F("from", info.From()), F("to", info.To()), F("dryRun", dryRun))
		return nil
	}
}
