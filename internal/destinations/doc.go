// Package destinations owns the destination table: the name, the target table,
// the row-key rule, the ordered column list, the image layout, the ColMask bit
// assignment, the contributor slot assignment, and the mapping from transaction
// code to contribution.
//
// destinations MUST NOT import oradb, writer, mirror or database/sql.
//
// That restriction is why the per-column pre-validation metadata -- SQL
// precision, maximum byte width, NOT NULL, and the permitted domain -- lives on
// Column in this package rather than somewhere more natural-looking next to the
// database layer. The mirror validates a contribution before merging it, and the
// mirror may not import the database packages.
//
// ColMask bit indices are declared explicitly, never derived from a column's
// position in the list. A reordered list must not silently re-map live entries.
//
// Contributor slots are assigned in ascending transaction code among a
// destination's declared contributors and fixed at startup, with at most
// maxContributors of them. Every table and column name is validated as an
// identifier before it can reach a SQL string.
//
// Two gates guard the layout. G-LAY-3 asserts at build time that each compiled-in
// destination's image fits imageSize, as a one-sided constant expression. The
// -selfcheck rule set asserts the same thing for configured destinations, plus
// the rule that matters most: every destination's row key must be derivable from
// the shard key of every contributing transaction code. Violate that and two
// shards write one row, which produces intermittent lost updates and is
// invisible until production.
//
// Specified by docs/lld/00-foundations.md section 4. Built in P1-04.
package destinations
