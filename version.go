package migrate

import (
	"encoding/hex"
	"fmt"
)

// Version is a migration version.
type Version struct {
	ID       int
	Checksum [32]byte
}

func (v Version) String() string {
	return fmt.Sprintf("v%d:%.10s...", v.ID, hex.EncodeToString(v.Checksum[:]))
}

func (v Version) ChecksumString() string {
	return hex.EncodeToString(v.Checksum[:])
}

// BadVersion represent an invalid version.
var badVersion = Version{ID: -1}
var BadVersion = badVersion

// MakeVersion make a version from an id and checksum value.
func MakeVersion(id int, checksum string) (Version, error) {
	var v Version
	if id < 0 {
		return badVersion, fmt.Errorf("%w: id %d", ErrBadVersionID, id)
	}
	b, err := hex.DecodeString(checksum)
	if err != nil {
		return badVersion, fmt.Errorf("%w: %w", ErrBadVersionChecksum, err)
	}
	if len(b) != len(v.Checksum) {
		return badVersion, fmt.Errorf("%w: invalid length", ErrBadVersionChecksum)
	}
	copy(v.Checksum[:], b)
	v.ID = id
	return v, nil
}
