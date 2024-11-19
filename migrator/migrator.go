// Copyright (c) 2024 Bryan Frimin <bryan@frimin.fr>.
//
// Permission to use, copy, modify, and/or distribute this software
// for any purpose with or without fee is hereby granted, provided
// that the above copyright notice and this permission notice appear
// in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
// WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
// AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
// CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
// OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
// NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
// CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

package migrator

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"

	"go.gearno.de/kit/pg"
)

type (
	Migrator struct {
		pg   *pg.Client
		path string
	}

	Migration struct {
		Version string
		SQL     string
	}

	Migrations []*Migration
)

const (
	MigrationAdvisoryLock pg.AdvisoryLock = 0
)

func NewMigrator(pg *pg.Client, dirname string) *Migrator {
	return &Migrator{
		pg:   pg,
		path: dirname,
	}
}

func (m *Migrator) Run(ctx context.Context) error {
	var migrations Migrations
	if err := migrations.LoadFromDir(m.path); err != nil {
		return fmt.Errorf("cannot load migrations: %w", err)
	}

	migrations.Sort()

	if len(migrations) == 0 {
		return nil
	}

	err := m.pg.WithAdvisoryLock(
		ctx,
		MigrationAdvisoryLock,
		func(conn pg.Conn) error {
			err := m.pg.WithConn(
				ctx,
				func(conn pg.Conn) error {
					return createIfNotExistVersionsTable(ctx, conn)
				},
			)
			if err != nil {
				return fmt.Errorf("cannot create schema version table: %w", err)
			}

			appliedVersions, err := loadSchemaVersions(ctx, conn)
			if err != nil {
				return fmt.Errorf("cannot load schema versions: %w", err)
			}

			for _, migration := range migrations {
				if _, found := appliedVersions[migration.Version]; found {
					continue
				}

				// c.logger.Info("applying migration", zap.String("version", migration.Version))

				err := m.pg.WithTx(
					ctx,
					func(conn pg.Conn) error {
						return migration.Apply(ctx, conn)
					},
				)
				if err != nil {
					return fmt.Errorf("cannot apply migration %v: %w", migration, err)
				}
			}

			return nil
		},
	)

	if err != nil {
		return err
	}

	if err := m.pg.RefreshTypes(ctx); err != nil {
		return fmt.Errorf("cannot refresh types: %w", err)
	}

	return nil
}

func (ms Migrations) Sort() {
	sort.Slice(
		ms,
		func(i, j int) bool {
			return ms[i].Version < ms[j].Version
		},
	)
}

func (pms *Migrations) LoadFromDir(pathname string) error {
	var ms Migrations

	entries, err := os.ReadDir(pathname)
	if err != nil {
		return fmt.Errorf("cannot read directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}

		name := entry.Name()
		ext := path.Ext(name)
		if ext != ".sql" {
			continue
		}

		filepath := path.Join(pathname, name)

		m := &Migration{}
		if err := m.LoadFromFile(filepath); err != nil {
			return fmt.Errorf("cannot load migration from %q: %w", filepath, err)
		}

		ms = append(ms, m)
	}

	*pms = ms
	return nil
}

func (m *Migration) Apply(ctx context.Context, conn pg.Conn) error {
	_, err := conn.Exec(ctx, m.SQL)
	if err != nil {
		return fmt.Errorf("cannot execute migration: %w", err)
	}

	q := "INSERT INTO schema_versions (version) VALUES ($1)"
	_, err = conn.Exec(ctx, q, m.Version)
	if err != nil {
		return fmt.Errorf("cannot insert schema version: %w", err)
	}

	return nil
}

func (m *Migration) LoadFromFile(pathname string) error {
	base := path.Base(pathname)
	ext := path.Ext(base)
	version := base[:len(base)-len(ext)]

	code, err := os.ReadFile(pathname)
	if err != nil {
		return err
	}

	m.Version = version
	m.SQL = string(code)

	return nil
}

func createIfNotExistVersionsTable(ctx context.Context, conn pg.Conn) error {
	q := `
CREATE TABLE IF NOT EXISTS schema_versions (
  version VARCHAR PRIMARY KEY,
  executed_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
)
`

	_, err := conn.Exec(ctx, q)
	return err
}

func loadSchemaVersions(ctx context.Context, conn pg.Conn) (map[string]struct{}, error) {
	q := "SELECT version FROM schema_versions"
	r, err := conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("cannot exec query: %w", err)
	}
	defer r.Close()

	versions := make(map[string]struct{})
	for r.Next() {
		var v string
		if err := r.Scan(&v); err != nil {
			return nil, fmt.Errorf("cannot scan row: %w", err)
		}

		versions[v] = struct{}{}
	}

	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("cannot read query: %w", err)
	}

	return versions, nil
}
