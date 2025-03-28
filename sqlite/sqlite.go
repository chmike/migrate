package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/chmike/migrate"

	_ "github.com/mattn/go-sqlite3"
)

// NewSteps instantiates a new migration step sequence. The name should not be
// empty and ideally unique to the database as it is used to compute the root
// checksum identifying the database.
func NewSteps(name string) *migrate.Steps {
	return migrate.NewSteps(name)
}

type config struct {
	tableName string
}

// Option function.
type Option func(*config)

// WithTableName changes the default version table name.
func WithTableName(tableName string) Option {
	return func(c *config) {
		c.tableName = tableName
	}
}

// Open opens or create an SQLite database.
func Open(sourceName string, options ...Option) (migrate.SQLDB, error) {
	var c config
	for _, option := range options {
		option(&c)
	}
	if c.tableName != "" {
		validName := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
		if !validName.MatchString(c.tableName) {
			return nil, fmt.Errorf("new sqlite: invalid table name '%s'", c.tableName)
		}
	}

	db, err := fixedBrokenSqliteOpen(sourceName, createOrOpen)
	if err != nil {
		return nil, err
	}

	q := &migrate.Queries{
		CreateTableQuery: `CREATE TABLE "migrate_version" ("id" INTEGER NOT NULL, "checksum" TEXT NOT NULL)`,
		InitTableQuery:   `INSERT INTO "migrate_version" ("id", "checksum") VALUES (?, ?)`,
		VersionQuery:     `SELECT "id", "checksum" FROM "migrate_version" LIMIT 1`,
		SetVersionQuery:  `UPDATE "migrate_version" SET "id" = ?, "checksum" = ? WHERE "id" = ? AND "checksum" = ?`,
	}

	if c.tableName != "" {
		q.Replace("migrate_version", c.tableName)
	}
	return migrate.NewSQLDB(db, q), nil
}

// New returns a new migrator.
func NewMigrator(db migrate.SQLDB, s migrate.Stepper, l migrate.Logger) (*Migrator, error) {
	return migrate.New(db, s, l)
}

// Cmd is a function simplifying the creation of a Command.
func Cmd(cmd string, args ...any) migrate.SQLCommand {
	return migrate.SQLCommand{Cmd: cmd, Args: args}
}

// Tx returns a migration step function that executes all the SQL commands in
// sequence wrapped in a transaction. The execution stops and rolls back as soon
// as an error is returned by one of the commands. It is also rolled back when dryRun
// is true.
func Tx(cmds ...migrate.SQLCommand) migrate.StepFunc {
	return migrate.Tx(cmds...)
}

// NoTx returns a migration step function that executes the SQL commands in sequence
// without a wrapping transaction. It terminates as soon as a command returns an error.
// It doesn't execute any cmds when dryRun is true.
func NoTx(cmds ...migrate.SQLCommand) StepFunc {
	return migrate.NoTx(cmds...)
}

// TxFunc is an migrate.TxFunc.
type TxFunc = migrate.TxFunc

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
	return migrate.TxF(fs...)
}

// NoTxFunc is an migrate.NoTxFunc.
type NoTxFunc = migrate.NoTxFunc

// NoTxF returns a migration step function that executes the user provided functions in sequence
// without a wrapping transaction. It terminates as soon as a function returns an error.
// It doesn't execute any function when dryRun is true. The pseudo error ErrCancel is
// treated as ErrAbort as operations can't be cancelled and database version remains v1.
//
// Use with care as any error in the function may leave the database is an undefined state.
func NoTxF(fs ...NoTxFunc) migrate.StepFunc {
	return migrate.NoTxF(fs...)
}

type sqliteOpenOp int

const (
	createOnly sqliteOpenOp = iota
	openOnly
	createOrOpen
)

var forceSqlOpenError error

// fixedBrokenSqliteOpen opens the sqlite3 database. File creation are allowed when create
// only if (1) the file exist, it is a file,
// it has the sqlite3 database file signature, and is writable, or (2) the file doesn't exist
// and it can be created unless create is true.
func fixedBrokenSqliteOpen(path string, op sqliteOpenOp) (*sql.DB, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) || op == openOnly {
			return nil, err
		}

		dir := filepath.Dir(path)
		testFile := filepath.Join(dir, ".tmp_create_test")
		tmp, err := os.Create(testFile)
		if err != nil {
			return nil, fmt.Errorf("cannot create db file (no write access to dir '%s'): %w", dir, err)
		}
		tmp.Close()
		os.Remove(testFile)
	} else {
		if op == createOnly {
			return nil, fmt.Errorf("%w: file %v exist", os.ErrExist, path)
		}
		if !stat.Mode().IsRegular() {
			return nil, fmt.Errorf("%v is not a file", path)
		}
		if err := checkSQLiteHeader(path); err != nil {
			return nil, fmt.Errorf("invalid SQLite file: %w", err)
		}
	}
	db, err := sql.Open("sqlite3", path)
	if forceSqlOpenError != nil {
		err = forceSqlOpenError
	}
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("%w: file %v", err, path)
	}

	return db, nil
}

var forceReadError error

func checkSQLiteHeader(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	header := make([]byte, 16)
	_, err = f.Read(header)
	if forceReadError != nil {
		err = forceReadError
	}
	if err != nil {
		return err
	}

	if string(header[:15]) != "SQLite format 3" {
		return fmt.Errorf("bad sqlite3 file signature")
	}
	return nil
}
