package migrate

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Migrator is a Migrater for the given database, stepper and logger.
type Migrator struct {
	mu            sync.Mutex // common mutex.
	db            Database   // database
	steps         Stepper    // migration stepper
	logger        Logger     // logger
	cachedVersion Version    // cached version
}

// New creates a new migrator. Returns ErrBadParameters if the parameters are invalid,
// or ErrBadVersion if the version in the database isn't found in the stepper.
func New(db Database, steps Stepper, l Logger) (*Migrator, error) {
	if steps == nil || db == nil {
		return nil, fmt.Errorf("%w: nil database or stepper", ErrBadParameters)
	}
	if l == nil {
		l = NewNilLogger()
	}

	return &Migrator{
		db:            db,
		steps:         steps,
		logger:        l,
		cachedVersion: badVersion,
	}, nil
}

// Version returns the current version of the database.
func (m *Migrator) Version() (v Version, err error) {
	return m.VersionCtx(context.Background())
}

// VersionCtx returns the current version of the database after checking its
// validity against the migrations steps.
func (m *Migrator) VersionCtx(ctx context.Context) (Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.versionCtx(ctx)
}

// VersionCtx returns the current version of the database after checking its
// validity against the migrations steps.
func (m *Migrator) versionCtx(ctx context.Context) (Version, error) {
	m.cachedVersion = badVersion
	v, err := m.db.Version(ctx)
	if err != nil {
		return m.cachedVersion, err
	}
	if err := m.steps.Check(v); err != nil {
		return m.cachedVersion, err
	}
	m.cachedVersion = v
	return m.cachedVersion, nil
}

func (m *Migrator) initCtx(ctx context.Context, dryRun bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.versionCtx(ctx)
	if err == nil {
		return fmt.Errorf("%w as %v", ErrAlreadyInitialized, m.cachedVersion)
	}

	v, err := m.steps.Version(0)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNotInitialized, err)
	}
	if err := m.db.InitVersion(ctx, v, dryRun); err != nil {
		return fmt.Errorf("%w: %w", ErrNotInitialized, err)
	}
	if !dryRun {
		m.cachedVersion = v
	}
	return nil
}

// Init initializes the database version to v0 after verifying that it is not initialized.
func (m *Migrator) Init() error {
	return m.InitCtx(context.Background())
}

// InitCtx initializes the database version to v0 after verifying that it is not initialized.
func (m *Migrator) InitCtx(ctx context.Context) error {
	return m.initCtx(ctx, false)
}

// InitDryRun simulates the database initialization to version v0 after verifying that
// it is not initialized.
func (m *Migrator) InitDryRun() error {
	return m.InitDryRunCtx(context.Background())
}

// InitDryRunCtx simulates the database initialization to version v0 after verifying that
// it is not initialized.
func (m *Migrator) InitDryRunCtx(ctx context.Context) error {
	return m.initCtx(ctx, true)
}

// oneUp attempts to execute one migration step up. It requires that the migrator is locked.
// It is the user's responsibility to ensure that another migrator doesn't migrate the
// database at the same time.
func (m *Migrator) oneUp(ctx context.Context, dryRun bool) error {
	info, up, err := m.steps.Up(m.cachedVersion)
	if err != nil {
		return err
	}
	if up == nil {
		err = m.db.DefaultStepFunc(ctx, info, dryRun, m.logger)
	} else {
		err = up(ctx, m.db, info, dryRun, m.logger)
	}
	if err == nil && !dryRun {
		m.cachedVersion = info.To()
	}
	return err
}

// oneDown attempts to execute one migration step up. It requires that the migrator is locked.
// It is the user's responsibility to ensure that another migrator doesn't migrate the
// database at the same time.
func (m *Migrator) oneDown(ctx context.Context, dryRun bool) error {
	info, down, err := m.steps.Down(m.cachedVersion)
	if err != nil {
		return err
	}
	if down == nil {
		err = m.db.DefaultStepFunc(ctx, info, dryRun, m.logger)
	} else {
		err = down(ctx, m.db, info, dryRun, m.logger)
	}
	if err == nil && !dryRun {
		m.cachedVersion = info.To()
	}
	return err
}

// OneUp attempts to execute one migration step up.
func (m *Migrator) OneUp() error {
	return m.OneUpCtx(context.Background())
}

// OneUpCtx attempts to execute one migration step up.
func (m *Migrator) OneUpCtx(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.oneUp(ctx, false); err != nil {
		return fmt.Errorf("one up: %w", err)
	}
	return nil
}

// OneDown attempts to execute one migration step down.
func (m *Migrator) OneDown() error {
	return m.OneDownCtx(context.Background())
}

// OneDownCtx attempts to execute one migration step down.
func (m *Migrator) OneDownCtx(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.oneDown(ctx, false); err != nil {
		return fmt.Errorf("one down: %w", err)
	}
	return nil
}

// OneUpDryRun attempts to execute one migration step up.
func (m *Migrator) OneUpDryRun() error {
	return m.OneUpDryRunCtx(context.Background())
}

// OneUpDryRunCtx attempts to execute one migration step up.
func (m *Migrator) OneUpDryRunCtx(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.oneUp(ctx, true); err != nil {
		return fmt.Errorf("one up dry run: %w", err)
	}
	return nil
}

// OneDownDryRun attempts to execute one migration step down.
func (m *Migrator) OneDownDryRun() error {
	return m.OneDownDryRunCtx(context.Background())
}

// OneDownDryRunCtx attempts to execute one migration step down.
func (m *Migrator) OneDownDryRunCtx(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.oneDown(ctx, true); err != nil {
		return fmt.Errorf("one down dry run: %w", err)
	}
	return nil
}

// AllUp attempts to executes all migration steps up.
func (m *Migrator) AllUp() error {
	return m.AllUpCtx(context.Background())
}

// AllUpCtx attempts to executes all migration steps up.
func (m *Migrator) AllUpCtx(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if err := m.oneUp(ctx, false); err != nil {
			if errors.Is(err, ErrEndOfSteps) {
				return nil
			}
			return fmt.Errorf("all up: %w", err)
		}
	}
}

// AllDown attempts to executes all migration steps down.
func (m *Migrator) AllDown() error {
	return m.AllDownCtx(context.Background())
}

// AllDownCtx attempts to executes all migration steps down.
func (m *Migrator) AllDownCtx(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if err := m.oneDown(ctx, false); err != nil {
			if errors.Is(err, ErrEndOfSteps) {
				return nil
			}
			return fmt.Errorf("all down: %w", err)
		}
	}
}
