package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/chmike/migrate"
	"github.com/chmike/migrate/sqlite"
)

// migrationSteps are the migration steps for the database.
var migrationSteps *sqlite.Steps

func init() {
	// v0: instantiate an empty sequence of migration steps
	s := migrate.NewSteps("example database")

	// v1: first step is to set the journal mode to WAL (faster). The undo operation is nil.
	// The NoTx means that the command won't be wrapped in a transaction. This may be required
	// for some operations.
	s.Append("set WAL mode", sqlite.NoTx(sqlite.Cmd("PRAGMA journal_mode=WAL")), nil)

	// v2: in a transaction, create some example table, and the undo function removes it.
	s.Append(
		"create example table",
		sqlite.Tx(sqlite.Cmd(`CREATE TABLE "example" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "msg" TEXT NOT NULL);`)),
		sqlite.Tx(sqlite.Cmd(`DROP TABLE "example";`)),
	)

	// v3: we insert some rows using go code.
	s.Append("insert row",
		sqlite.TxF(func(tx sqlite.SQLTx, info sqlite.StepInfo, dryRun bool, log sqlite.Logger) error {
			for i := range 10 {
				_, err := tx.Tx().Exec(`INSERT INTO "example" ("msg") VALUES (?);`,
					fmt.Sprintf("%d test: %s: %v -> %v", i, info.Name(), info.From(), info.To()))
				if err != nil {
					return err // this will roll back the transaction
				}
			}
			return nil // this will commit the transaction
		}),
		sqlite.Tx(sqlite.Cmd(`DELETE FROM "example";`)),
	)

	migrationSteps = s
}

func displayMigrationSteps() {
	log.Println("-- migration steps --")
	for i := range migrationSteps.Len() {
		v, err := migrationSteps.Version(i)
		if err != nil {
			log.Fatalf("migration step %d version: %v", i, err)
		}
		name, err := migrationSteps.Name(i)
		if err != nil {
			log.Fatalf("migration step %d name: %v", i, err)
		}
		log.Printf("%d '%s' %v\n", i, name, v)
	}
	log.Println("---------------------")
}

func main() {
	// display migration steps
	displayMigrationSteps()

	// create a temporary working directory that we remove when done
	tempDir, err := os.MkdirTemp("", "sqlite_test")
	if err != nil {
		log.Fatalf("create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// create the database that we close when done
	db, err := sqlite.Open(filepath.Join(tempDir, "data.db"))
	if err != nil {
		log.Fatalf("create database: %v", err)
	}
	defer db.DB().Close()

	// instantiate a migrator
	migrator, err := sqlite.NewMigrator(db, migrationSteps, migrate.NewLogLogger(migrate.LevelDebug))
	if err != nil {
		log.Fatalf("create migrator error: %v", err)
	}

	// try reading version of uninitialized database
	_, err = migrator.Version()
	if err == nil {
		log.Fatal("expect error")
	} else if !errors.Is(err, migrate.ErrNotInitialized) {
		log.Fatalf("version of uninitialized database error: %v", err)
	}

	// initialize database for migration
	err = migrator.Init()
	if err != nil {
		log.Fatalf("init database migration error: %v", err)
	}

	// iterate over all migration steps
	// it is equivalent to AllUp()
	for {
		// get current version
		v, err := migrator.Version()
		if err != nil {
			log.Fatalf("database version error: %v", err)
		}
		log.Println("version is", v)

		err = migrator.OneUp()
		if err != nil {
			if errors.Is(err, migrate.ErrEndOfSteps) {
				break
			}
			log.Fatalf("migrate one step up error: %v", err)
		}
	}

	// get current version
	v, err := migrator.Version()
	if err != nil {
		log.Fatalf("database version error: %v", err)
	}
	log.Println("after all up, version is", v)

	// migrate all down
	err = migrator.AllDown()
	if err != nil {
		log.Fatalf("migrate all down error: %v", err)
	}

	// get current version
	v, err = migrator.Version()
	if err != nil {
		log.Fatalf("database version error: %v", err)
	}
	log.Println("after all down, version is", v)
}
