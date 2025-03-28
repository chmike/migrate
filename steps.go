package migrate

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
)

// StepInfo is a step information.
type stepInfo struct {
	name string
	from Version
	to   Version
}

func (s *stepInfo) Name() string   { return s.name }
func (s *stepInfo) From() Version  { return s.from }
func (s *stepInfo) To() Version    { return s.to }
func (s *stepInfo) String() string { return fmt.Sprintf("'%s' %v -> %v", s.name, s.from, s.to) }

// Step is a migration step with its Up and Down operations.
type step struct {
	name    string   // name is the step name.
	up      StepFunc // up is executed to migrate on step up to this version.
	down    StepFunc // down is executed to to migrate one step down to the version below.
	version Version  // version is version of this migration step.
}

// Steps is a read only sequence of migration steps.
type Steps struct {
	mu    sync.RWMutex
	steps []step
}

// NewSteps instantiates a new migration step sequence. The name should not be
// empty and ideally unique to the database as it is used to compute the root
// checksum identifying the database.
func NewSteps(name string) *Steps {
	return &Steps{
		steps: []step{
			{
				name: name,
				version: Version{
					Checksum: sha256.Sum256([]byte(name)),
				},
			},
		},
	}
}

// Append appends a new migration step to the list. Name must not be empty as it
// is used to compute a checksum. The functions up or down may be nil.
func (s *Steps) Append(name string, up StepFunc, down StepFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == "" {
		return fmt.Errorf("append step: name is empty")
	}
	ID := len(s.steps)
	var b []byte
	b = append(b, s.steps[ID-1].version.Checksum[:]...)
	b = binary.LittleEndian.AppendUint64(b, uint64(ID))
	b = append(b, name...)
	s.steps = append(s.steps, step{
		name:    name,
		up:      up,
		down:    down,
		version: Version{ID: ID, Checksum: sha256.Sum256(b)},
	})
	return nil
}

// Len returns the number of Steps.
func (s *Steps) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.steps)
}

// checkID returns an error if ID is invalid.
func (s *Steps) checkID(ID int) error {
	if ID < 0 || ID >= len(s.steps) {
		return fmt.Errorf("%w: id %d", ErrBadVersionID, ID)
	}
	return nil
}

// Version returns the version of step ID. ID 0 is the initial state
// of the database after it is initialized for migration. It is also
// the state to which the database is returned with the migration
// AllDown. It is the state where the database should be empty.
func (s *Steps) Version(ID int) (v Version, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	err = s.checkID(ID)
	if err != nil {
		return badVersion, err
	}
	return s.steps[ID].version, nil
}

// Name returns the migration step name. ID 0 is the initial state
// of the database after it is initialized for migration. It is also
// to state to which the database is returned with the migration
// AllDown. It is the state where the database should be empty.
func (s *Steps) Name(ID int) (name string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	err = s.checkID(ID)
	if err != nil {
		return "", err
	}
	return s.steps[ID].name, nil
}

// Check returns an error if the version is invalid.
func (s *Steps) check(v Version) error {
	if err := s.checkID(v.ID); err != nil {
		return err
	}
	if ev := s.steps[v.ID].version; ev != v {
		return fmt.Errorf("%w: expect '%.10s...', got '%.10s...'", ErrBadVersionChecksum,
			hex.EncodeToString(ev.Checksum[:]), hex.EncodeToString(v.Checksum[:]))
	}
	return nil
}

// Check returns an error if the given version is invalid. It may be
// one of ErrBadVersionID when the ID is out of range, or ErrBadVersionChecksum
// if the checksum doesn't match the one in the stepper.
func (s *Steps) Check(v Version) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.check(v)
}

// Up returns the StepInfo and function for one step up migration
// from the given version to the next version.
// It returns a ErrBadVersion if the version is invalid or
// ErrEndOfSteps if there are no more steps.
func (s *Steps) Up(v Version) (nfo StepInfo, f StepFunc, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err = s.check(v); err != nil {
		err = fmt.Errorf("%w: %v", ErrBadVersion, v)
		return
	}
	if v.ID == len(s.steps)-1 {
		err = fmt.Errorf("%w: %v", ErrEndOfSteps, v)
		return
	}
	from := &s.steps[v.ID]
	to := &s.steps[v.ID+1]
	return &stepInfo{from: from.version, to: to.version, name: to.name}, to.up, nil
}

// Down returns the StepInfo and function for one step down migration
// from the given version to the next version.
// It returns a ErrBadVersion if the version is invalid or
// ErrEndOfSteps if there are no more steps.
func (s *Steps) Down(v Version) (nfo StepInfo, f StepFunc, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err = s.check(v); err != nil {
		err = fmt.Errorf("%w: %v", ErrBadVersion, v)
		return
	}
	if v.ID == 0 {
		err = fmt.Errorf("%w: %v", ErrEndOfSteps, v)
		return
	}
	from := &s.steps[v.ID]
	to := &s.steps[v.ID-1]
	return &stepInfo{from: from.version, to: to.version, name: from.name}, from.down, nil
}
