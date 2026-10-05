package config

import "sync/atomic"

// Store is the published configuration pointer.
//
// Readers load it at a natural boundary and hold that pointer for the whole
// unit of work -- one datagram for the receiver, one flush tick for a shard,
// one batch for a writer. Re-reading mid-unit would let one Config govern the
// first half of an operation and another the second; for a shard that means
// two different batch-size limits inside one epoch.
type Store struct {
	p   atomic.Pointer[Config]
	gen atomic.Uint64
}

// NewStore publishes an initial configuration as generation 1.
//
// Generations start at 1 so that zero is unambiguously "nothing published
// yet" -- a shard comparing against a zero-valued generation must not
// conclude it has already seen the current config.
func NewStore(c *Config) *Store {
	s := &Store{}
	s.publish(c)
	return s
}

// Load returns the current configuration. Never nil once the store is built.
func (s *Store) Load() *Config { return s.p.Load() }

// Gen is the generation of the currently published configuration.
func (s *Store) Gen() uint64 { return s.gen.Load() }

// Swap validates a candidate against the running configuration and publishes
// it, returning the configuration it replaced.
//
// On failure NOTHING changes: the candidate is discarded whole and the
// running configuration stays published. That is the single most important
// property of this type -- a reload is all-or-nothing, because a partially
// applied configuration is worse than a stale one.
func (s *Store) Swap(c *Config) (*Config, error) {
	prev := s.p.Load()
	if err := Validate(c, prev); err != nil {
		return prev, err
	}
	s.publish(c)
	return prev, nil
}

// publish assigns the next generation and swaps the pointer. The generation
// is written into the Config before it is published, so any reader that can
// see the pointer can see a consistent Gen.
func (s *Store) publish(c *Config) {
	c.Gen = s.gen.Add(1)
	s.p.Store(c)
}
