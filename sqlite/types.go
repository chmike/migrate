package sqlite

import "github.com/chmike/migrate"

// Logger is a migration logger.
type Logger = migrate.Logger

// Steps are migration steps.
type Steps = migrate.Steps

// StepInfo is a migration step information.
type StepInfo = migrate.StepInfo

// StepFunc is a migration step function.
type StepFunc = migrate.StepFunc

// Migrator is a migration for migration steps.
type Migrator = migrate.Migrator

// SQLDB is a migration SQLDB.
type SQLDB = migrate.SQLDB

// SQLTx is a migration transaction.
type SQLTx = migrate.SQLTx
