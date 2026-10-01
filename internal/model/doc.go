// Package model holds the value types every other package speaks: UpdateUnit,
// Contribution, Entry, RowKey, ColMask, Batch, BatchRow, RowOutcome, DestID,
// TransCode, MsgHeader, HeaderKind and Class. Pure data, with no behaviour
// beyond accessors and String methods.
//
// model MUST NOT import anything outside the standard library. Not database/sql,
// not go-ora, not log, and no other internal package.
//
// The rule is stricter than it looks. A sql.NullString or a time.Time carrying a
// location pointer inside Batch would drag the database into the receive path and
// break the pointer-free arena rule in the same stroke: Entry, UpdateUnit,
// BatchRow and Datagram live in pre-allocated arenas that the garbage collector
// must never have to scan.
//
// Seven of these types have byte-exact sizes pinned by compile-time constant
// expressions -- Entry 424, UpdateUnit 360, BatchRow 288, Datagram 1040,
// MsgHeader 32, Contribution 24, SessionEvent 16. Padding is written out
// explicitly rather than left to the compiler, so the declared offset tables and
// the real layout cannot drift apart.
//
// Specified by docs/lld/00-foundations.md section 3. Built in P1-03.
package model
