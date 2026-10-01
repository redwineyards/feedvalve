package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// config mirrors the command-line flags. Pointer fields distinguish "absent
// from the file" from "explicitly set to the zero value", so a flag only
// gets a config-file value when the file actually specifies it.
type config struct {
	UDP    *string   `json:"udp"`
	Out    *string   `json:"out"`
	Debug  *bool     `json:"debug"`
	NetID  *int      `json:"netid"`
	MaxOut *int64    `json:"maxout"`
	DB     *dbConfig `json:"db"`
}

// dbConfig is the "db" object in the config file. It is nested rather than
// flattened into config so that main's flag-override chain does not double in
// length, and so that "is the database configured at all?" is one nil check.
//
// There is deliberately no password field: the password comes only from
// FEEDVALVE_DB_PASSWORD. A config file is the wrong place for it -- these get
// committed, copied between environments, and read into log lines.
type dbConfig struct {
	Server  *string `json:"server"`
	Port    *int    `json:"port"`
	Service *string `json:"service"`
	User    *string `json:"user"`

	MaxOpenConns    *int    `json:"maxopenconns"`
	MaxIdleConns    *int    `json:"maxidleconns"`
	ConnMaxLifetime *string `json:"connmaxlifetime"`

	ConnectTimeout *string `json:"connecttimeout"`
	QueryTimeout   *string `json:"querytimeout"`
}

// toDBConfig converts the file representation into a DBConfig, parsing the
// duration strings and taking the password from the environment. Fields the
// file omits are left zero for DBConfig.withDefaults to fill in.
func (c *dbConfig) toDBConfig() (DBConfig, error) {
	var out DBConfig
	if c == nil {
		return out, nil
	}

	if c.Server != nil {
		out.Server = *c.Server
	}
	if c.Port != nil {
		out.Port = *c.Port
	}
	if c.Service != nil {
		out.Service = *c.Service
	}
	if c.User != nil {
		out.User = *c.User
	}
	if c.MaxOpenConns != nil {
		out.MaxOpenConns = *c.MaxOpenConns
	}
	if c.MaxIdleConns != nil {
		out.MaxIdleConns = *c.MaxIdleConns
	}

	for _, d := range []struct {
		name  string
		value *string
		dst   *time.Duration
	}{
		{"connmaxlifetime", c.ConnMaxLifetime, &out.ConnMaxLifetime},
		{"connecttimeout", c.ConnectTimeout, &out.ConnectTimeout},
		{"querytimeout", c.QueryTimeout, &out.QueryTimeout},
	} {
		if d.value == nil {
			continue
		}
		parsed, err := time.ParseDuration(*d.value)
		if err != nil {
			return out, fmt.Errorf("db.%s: %w", d.name, err)
		}
		if parsed < 0 {
			return out, fmt.Errorf("db.%s: negative duration %s", d.name, parsed)
		}
		*d.dst = parsed
	}

	out.Password = dbPasswordFromEnv()
	return out, nil
}

// defaultMaxOutBytes caps the packet log at 1 GiB by default. Large enough
// that a normal debugging session never notices it, small enough that an
// unattended relay left running against a live feed cannot fill a disk.
const defaultMaxOutBytes = 1 << 30

func loadConfig(path string) (config, error) {
	var cfg config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config file: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config file: %w", err)
	}
	return cfg, nil
}
