# Migrate module

Migrate is a simple database migration management package. It is designed to support
non-sql databases as well as sql databases. Support sqlite is available with the
migrate/sqlite package. Adding support for other sql databases is trivial.

See the example program in `examples/simple` for a usage example. The intended usage
is to define migration steps in an init function and use a migrator to use them on a
database.

This module is in alpha stage and may change without notice. Contributions are welcome.

## Installation

To use this package, execute the instruction `go get -u github.com/chmike/migrate@latest`.

## Migration steps

A migration step has a name, a version and an up and down function. A version is the
step sequence number and a 16 byte checksum serving as a signature.

For sql databases, the up and down functions may be a transaction wrapped sequence
of sql commands of go functions. Non transaction wrapped sql commands and go functions
are also supported but the dry run flag is ignored. The intended use is to set some
database specific flags that can't be set from inside a transaction.

The following code is an example of migration step definition.

```go
import github.com/chmike/migrate/sqlite


var migrationSteps *sqlite.Steps

func init() {
    s := sqlite.NewSteps("my book database")

    // migration step with sql commands
    s.Append(
        // name
        "create some table",

        // up operation
        sqlite.Tx(sqlite.Cmd(`CREATE TABLE "example" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "text" TEXT NOT NULL)`)),

        // down operation
        sqlite.Tx(sqlite.Cmd(`DROP TABLE "example"`)),
    )

    // migration step with a go function as up operation and an sql command for down
    s.Append(
        // name
        "insert some rows",

        // up operation
        sqlite.TxF(func(tx sqlite.SQLTx, info sqlite.StepInfo, dryRun bool, log sqlite.Logger) error {
            for i := range 10 {
                text := fmt.Sprintf("%d test: %s: %v -> %v", i, info.Name(), info.From(), info.To())
                _, err := tx.Tx().Exec(`INSERT INTO "example" ("text") VALUES (?);`, text)
                if err != nil {
                    log.Error("failed inserting row", F("text", text))
                    return err // an error result in transaction rollback
                }
                log.Info("inserting row", F("text", text))
            }
            return nil // nil error result in commit unless dryRun is true
        }),

        // down operation
        sqlite.Tx(sqlite.Cmd(`DELETE FROM "example";`)),
    )

    // . . .

    migrationSteps = s
}
```

If the migration steps are defined in a dedicated file, it is also possible to import the
sqlite package with a dot so that the code is a little lighter.

```go
import . github.com/chmike/migrate/sqlite


var migrationSteps *Steps

func init() {
    s := NewSteps("my book database")

    // migration step with sql commands
    s.Append(
        // name
        "create some table",

        // up operation
        Tx(Cmd(`CREATE TABLE "example" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "text" TEXT NOT NULL)`)),

        // down operation
        Tx(Cmd(`DROP TABLE "example"`)),
    )

    // migration step with a go function as up operation and an sql command for down
    s.Append(
        // name
        "insert some rows",

        // up operation
        TxF(func(tx sSQLTx, info StepInfo, dryRun bool, log Logger) error {
            for i := range 10 {
                text := fmt.Sprintf("%d test: %s: %v -> %v", i, info.Name(), info.From(), info.To())
                _, err := tx.Tx().Exec(`INSERT INTO "example" ("text") VALUES (?);`, text)
                if err != nil {
                    log.Error("failed inserting row", F("text", text))
                    return err // an error result in transaction rollback
                }
                log.Info("inserting row", F("text", text))
            }
            return nil // nil error result in commit unless dryRun is true
        }),

        // down operation
        Tx(Cmd(`DELETE FROM "example";`)),
    )

    // . . .
}
```

## Migrator

The interaction with a database is performed by use of a migrator.
For an sql database, the migrator binds the steps with a small group of
database specific queries used by this migration module.

Once the migrator is instantiated, it is required to call the Version
or Init methods. It is safe to call the Init method as it will return
an error if the database is already initialized, otherwise it will
initialize the database. The version method will simply try ot read
the database version.

Once the Version or Init methods have been called, one may call any
of the methods that will perform a migration step. OneUp, OneDown,
AllUp, AllDown. They all have a version with a context argument.

There are also a OneUpDryRun and OneDownDryRun methods whose effect
should be obvious. Regardless if they return an error or not, the
transaction will be rolled back.

## Logger

The migrate logger is a wrapper for the different kind of loggers.
A logger wrapper for the std log, slog, zap and zerolog are provided.

Se the example above how to log messages. This module supports
Error, Warn, Info and Debug logging messages. It uses its own log level
filtering.
