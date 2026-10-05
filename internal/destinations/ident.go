package destinations

import "fmt"

// maxIdentLen is Oracle's identifier limit for 12.2 and later. Older servers
// cap at 30; a name over 30 is accepted here and will fail at DDL time on such
// a server, which is the right place for that to surface.
const maxIdentLen = 128

// validIdent checks that s is a safe, unquoted Oracle identifier.
//
// This runs on every table and column name BEFORE that name can reach a SQL
// string. Statements are assembled at runtime from this mapping file, so a
// name is the one piece of statement text that does not come from a Go
// literal. Validating it here, once at load, is what keeps "built dynamically"
// from meaning "built from whatever the file said".
//
// The rule is deliberately narrower than Oracle's own: a leading letter, then
// letters, digits and underscores. No quoted identifiers, no dollar or hash,
// no dots. Everything the three destinations need fits, and anything that does
// not is a mapping the operator should write differently rather than one we
// should learn to escape.
func validIdent(s string) error {
	if s == "" {
		return fmt.Errorf("empty identifier")
	}
	if len(s) > maxIdentLen {
		return fmt.Errorf("identifier %q is %d bytes, over the %d-byte limit", s, len(s), maxIdentLen)
	}

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c == '_', c >= '0' && c <= '9':
			if i == 0 {
				return fmt.Errorf("identifier %q must start with a letter", s)
			}
		default:
			return fmt.Errorf("identifier %q contains %q; only letters, digits and "+
				"underscore are allowed", s, string(rune(c)))
		}
	}
	return nil
}
