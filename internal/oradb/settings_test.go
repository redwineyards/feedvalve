package oradb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const probe = "hunter2-UNIQUE-TOKEN"

func sample() DBSettings {
	return DBSettings{
		Host: "oradb.example.internal", Port: 1521,
		Service: "FEEDPRD", User: "feedvalve",
		ConnectTimeoutSec: 10, QueryTimeoutSec: 30, ConnMaxLifetimeMinutes: 30,
	}
}

// TestDBSettingsHasNoStringer is gate G-SEC-1, and it is a type assertion
// rather than an output check on purpose.
//
// Checking that String() does not print the password would pass for a
// String() that happens not to today. What must hold is that the method does
// not EXIST: a String() added later for convenience puts the DSN into an
// append-only, world-readable log the first time someone writes %v in a
// hurry, and on this host that is permanent.
func TestDBSettingsHasNoStringer(t *testing.T) {
	var s any = sample()

	if _, ok := s.(fmt.Stringer); ok {
		t.Error("DBSettings has a String() method. Describe is meant to be the only " +
			"rendering, so that a hurried verb produces a struct dump of non-secret " +
			"fields rather than anything containing a DSN.")
	}
	if _, ok := s.(json.Marshaler); ok {
		t.Error("DBSettings has MarshalJSON. Same reason: it must not acquire a " +
			"custom rendering that a future field could leak through.")
	}

	// And the pointer form, since a method on *DBSettings would be found by
	// fmt for an addressable value.
	var p any = &DBSettings{}
	if _, ok := p.(fmt.Stringer); ok {
		t.Error("*DBSettings has a String() method")
	}
	if _, ok := p.(json.Marshaler); ok {
		t.Error("*DBSettings has MarshalJSON")
	}
}

// TestNoPasswordField walks the struct. The password must have nowhere to
// live, not merely be absent by convention.
func TestNoPasswordField(t *testing.T) {
	typ := reflect.TypeOf(DBSettings{})
	for i := range typ.NumField() {
		name := strings.ToLower(typ.Field(i).Name)
		if strings.Contains(name, "password") || strings.Contains(name, "secret") {
			t.Errorf("DBSettings.%s could hold the password. poc's DBConfig had such a "+
				"field; this type deliberately does not, which is what lets Config be "+
				"logged wholesale", typ.Field(i).Name)
		}
	}
}

// TestRenderingsNeverLeak covers the formattings a tired operator or a hurried
// log line would actually produce.
func TestRenderingsNeverLeak(t *testing.T) {
	t.Setenv(PasswordEnv, probe)
	s := sample()

	for _, r := range []struct{ how, out string }{
		{"Describe()", s.Describe()},
		{"%v", fmt.Sprintf("%v", s)},
		{"%+v", fmt.Sprintf("%+v", s)},
		{"%#v", fmt.Sprintf("%#v", s)},

		// Routed through any() deliberately. Written directly, go vet
		// REFUSES this line -- "%s has arg s of wrong type" -- because
		// DBSettings has no String(). That refusal is a second, free line
		// of defence: the day someone adds a String(), this call stops
		// being a vet error and starts being a leak, and the assertion
		// above is what catches it.
		{"%s", fmt.Sprintf("%s", any(s))},

		{"json.Marshal", mustJSON(t, s)},
		{"%v on pointer", fmt.Sprintf("%v", &s)},
	} {
		if strings.Contains(r.out, probe) {
			t.Errorf("%s leaked the password: %s", r.how, r.out)
		}
	}

	// Describe must still be useful, or people will reach for something else.
	d := s.Describe()
	for _, want := range []string{"feedvalve", "oradb.example.internal", "1521", "FEEDPRD"} {
		if !strings.Contains(d, want) {
			t.Errorf("Describe() omits %q, making it useless for diagnosis: %s", want, d)
		}
	}
}

func TestPasswordComesOnlyFromTheEnvironment(t *testing.T) {
	t.Setenv(PasswordEnv, "")
	if HasPassword() {
		t.Error("HasPassword true with the variable unset")
	}
	if _, err := Password(); !errors.Is(err, ErrNoPassword) {
		t.Errorf("Password() = %v, want ErrNoPassword", err)
	}

	t.Setenv(PasswordEnv, probe)
	if !HasPassword() {
		t.Error("HasPassword false with the variable set")
	}
	got, err := Password()
	if err != nil || got != probe {
		t.Errorf("Password() = %q, %v", got, err)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
