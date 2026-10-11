package obs

// Hooks for the external test package in crosscheck_test.go, which has to
// import model and therefore cannot be `package obs`.
//
// They live in a _test.go file so they are absent from the production build:
// nothing outside obs needs the ledger as strings, and exporting it for real
// would invite a caller to look an instrument up by name -- the thing Vol 0
// section 8.4's MUST forbids.

// MaxDestinationsForTest exposes the locally re-declared copy of
// model.MaxDestinations for the drift check.
const MaxDestinationsForTest = maxDestinations

// MaxColumnsForTest exposes the locally re-declared copy of model.MaxColumns.
const MaxColumnsForTest = maxColumns

// LedgerNamesForTest returns every declared instrument name.
func LedgerNamesForTest() []string {
	out := make([]string, 0, numInstruments)
	for id := range numInstruments {
		out = append(out, ledger[id].name)
	}
	return out
}
