package oradb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestReloadedLifetimeReachesPoolAndNewSession pins the P5-05 rule: a lowered
// connection lifetime reaches the *sql.DB AND the next pinned Session; the
// atomic in DB, not Params.ConnHold, is what Acquire follows.
//
// SETUP: fake DB opened with a 30 minute lifetime.
// STIMULUS: SetConnMaxLifetime(20ms); Acquire; hold; a pooled connection is
// idled past 20ms and reused.
// ASSERTION: the new Session is Expired after 60ms (a Session acquired BEFORE
// the call is not: it keeps its 30 min); sql.DB retired the pooled connection
// (Stats.MaxLifetimeClosed > 0).
// WOULD HAVE CAUGHT: a setter that updates only one of the two sinks.
func TestReloadedLifetimeReachesPoolAndNewSession(t *testing.T) {
	d, _ := fakeDB(t, poolSettings())
	old, err := d.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()

	d.SetConnMaxLifetime(20 * time.Millisecond)
	ss, err := d.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Second) // explicit clock: no sleep needed
	if !ss.Expired(later) {
		t.Error("a Session acquired after the reload still holds the old lifetime")
	}
	if old.Expired(later) {
		t.Error("non-vacuity: a Session pinned before the reload must keep its lifetime")
	}
	ss.Release()
	time.Sleep(150 * time.Millisecond) // coarse: only needs to exceed the 20ms lifetime
	again, err := d.Acquire(context.Background()) // database/sql judges the idle conn here
	if err != nil {
		t.Fatal(err)
	}
	again.Release()
	if d.Stats().MaxLifetimeClosed == 0 {
		t.Error("sql.DB never retired a connection: SetConnMaxLifetime did not reach it")
	}
}

// TestSetConnectTimeoutAppliesToPingAndAcquire: the timeout in force is the
// atomic's, not the one the DB was opened with, and the setters are race-free
// against Ping and Acquire (meaningful under -race).
func TestSetConnectTimeoutAppliesToPingAndAcquire(t *testing.T) {
	// One-connection pool, held by a Session: Ping and Acquire must wait for a
	// connection, so only their timeout can end the wait.
	d, _ := fakeDB(t, oneConn())
	held, err := d.Acquire(context.Background())
	if err != nil {
		t.Fatalf("non-vacuity: first acquire under 10 s: %v", err)
	}
	d.SetConnectTimeout(30 * time.Millisecond)
	start := time.Now()
	if err := d.PingContext(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ping under 30ms: %v, want deadline exceeded", err)
	}
	if _, err := d.Acquire(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("acquire under 30ms: %v, want deadline exceeded", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("waited on the 10 s timeout the DB was opened with")
	}
	held.Release()
	d.SetConnectTimeout(10 * time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				d.SetConnMaxLifetime(time.Duration(j+1) * time.Minute)
				d.SetConnectTimeout(time.Duration(j+1) * time.Second)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if s, err := d.Acquire(context.Background()); err == nil {
					s.Release()
				}
				_ = d.PingContext(context.Background())
			}
		}()
	}
	wg.Wait()
}
