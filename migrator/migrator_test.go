package migrator_test

import (
	"context"
	"io"
	"testing"
	"testing/fstest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gearno.de/kit/log"
	"go.gearno.de/kit/migrator"
	"go.gearno.de/kit/pg"
)

func newTestPGClient(t *testing.T) *pg.Client {
	t.Helper()

	client, err := pg.NewClient(
		pg.WithAddr("localhost:5432"),
		pg.WithUser("kit"),
		pg.WithPassword("kit"),
		pg.WithDatabase("kit_test"),
		pg.WithLogger(log.NewLogger(log.WithOutput(io.Discard))),
		pg.WithRegisterer(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Skipf("skipping: cannot create PostgreSQL client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.WithConn(ctx, func(ctx context.Context, conn pg.Querier) error {
		_, err := conn.Exec(ctx, "SELECT 1")
		return err
	})
	if err != nil {
		client.Close()
		t.Skipf("skipping: cannot connect to PostgreSQL: %v", err)
	}

	t.Cleanup(client.Close)

	return client
}

func dropTables(t *testing.T, client *pg.Client, tables ...string) {
	t.Helper()
	ctx := context.Background()
	_ = client.WithConn(ctx, func(ctx context.Context, conn pg.Querier) error {
		for _, tbl := range tables {
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl+" CASCADE")
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Unit tests (no database required)
// ---------------------------------------------------------------------------

func TestMigration_LoadFromFile(t *testing.T) {
	disk := fstest.MapFS{
		"migrations/001_create_users.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id serial PRIMARY KEY);"),
		},
	}

	var m migrator.Migration
	err := m.LoadFromFile(disk, "migrations/001_create_users.sql")
	require.NoError(t, err)
	assert.Equal(t, "001_create_users", m.Version)
	assert.Equal(t, "CREATE TABLE users (id serial PRIMARY KEY);", m.SQL)
}

func TestMigrations_LoadFromDir(t *testing.T) {
	disk := fstest.MapFS{
		"migrations/002_add_email.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users ADD COLUMN email TEXT;"),
		},
		"migrations/001_create_users.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id serial PRIMARY KEY);"),
		},
		"migrations/README.md": &fstest.MapFile{
			Data: []byte("Not a migration"),
		},
	}

	var ms migrator.Migrations
	err := ms.LoadFromDir(disk, "migrations")
	require.NoError(t, err)
	require.Len(t, ms, 2)

	versions := map[string]bool{}
	for _, m := range ms {
		versions[m.Version] = true
	}
	assert.True(t, versions["001_create_users"])
	assert.True(t, versions["002_add_email"])
}

func TestMigrations_Sort(t *testing.T) {
	ms := migrator.Migrations{
		{Version: "003_add_index", SQL: "CREATE INDEX ...;"},
		{Version: "001_create_users", SQL: "CREATE TABLE ...;"},
		{Version: "002_add_email", SQL: "ALTER TABLE ...;"},
	}

	ms.Sort()

	require.Len(t, ms, 3)
	assert.Equal(t, "001_create_users", ms[0].Version)
	assert.Equal(t, "002_add_email", ms[1].Version)
	assert.Equal(t, "003_add_index", ms[2].Version)
}

// ---------------------------------------------------------------------------
// Integration tests (require a running PostgreSQL instance)
// ---------------------------------------------------------------------------

func TestMigrator_Run(t *testing.T) {
	client := newTestPGClient(t)
	ctx := context.Background()
	logger := log.NewLogger(log.WithOutput(io.Discard))

	t.Run("applies migrations", func(t *testing.T) {
		dropTables(t, client, "schema_versions", "test_mig_users")
		t.Cleanup(func() { dropTables(t, client, "schema_versions", "test_mig_users") })

		disk := fstest.MapFS{
			"migrations/001_create_users.sql": &fstest.MapFile{
				Data: []byte("CREATE TABLE test_mig_users (id serial PRIMARY KEY, name text NOT NULL);"),
			},
		}

		m := migrator.NewMigrator(client, disk, logger)
		require.NoError(t, m.Run(ctx, "migrations"))

		err := client.WithConn(ctx, func(ctx context.Context, conn pg.Querier) error {
			var exists bool
			err := conn.QueryRow(ctx,
				"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'test_mig_users')").
				Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists)

			var version string
			err = conn.QueryRow(ctx, "SELECT version FROM schema_versions").Scan(&version)
			require.NoError(t, err)
			assert.Equal(t, "001_create_users", version)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("idempotent", func(t *testing.T) {
		dropTables(t, client, "schema_versions", "test_mig_idem")
		t.Cleanup(func() { dropTables(t, client, "schema_versions", "test_mig_idem") })

		disk := fstest.MapFS{
			"migrations/001_create_table.sql": &fstest.MapFile{
				Data: []byte("CREATE TABLE test_mig_idem (id serial PRIMARY KEY);"),
			},
		}

		m := migrator.NewMigrator(client, disk, logger)
		require.NoError(t, m.Run(ctx, "migrations"))
		require.NoError(t, m.Run(ctx, "migrations"))

		err := client.WithConn(ctx, func(ctx context.Context, conn pg.Querier) error {
			var count int
			err := conn.QueryRow(ctx, "SELECT count(*) FROM schema_versions").Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 1, count)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("applies multiple migrations in order", func(t *testing.T) {
		dropTables(t, client, "schema_versions", "test_mig_ordered")
		t.Cleanup(func() { dropTables(t, client, "schema_versions", "test_mig_ordered") })

		disk := fstest.MapFS{
			"migrations/002_add_column.sql": &fstest.MapFile{
				Data: []byte("ALTER TABLE test_mig_ordered ADD COLUMN email TEXT;"),
			},
			"migrations/001_create_table.sql": &fstest.MapFile{
				Data: []byte("CREATE TABLE test_mig_ordered (id serial PRIMARY KEY, name text NOT NULL);"),
			},
		}

		m := migrator.NewMigrator(client, disk, logger)
		require.NoError(t, m.Run(ctx, "migrations"))

		err := client.WithConn(ctx, func(ctx context.Context, conn pg.Querier) error {
			var count int
			err := conn.QueryRow(ctx, "SELECT count(*) FROM schema_versions").Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 2, count)

			var exists bool
			err = conn.QueryRow(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM information_schema.columns
					WHERE table_name = 'test_mig_ordered' AND column_name = 'email'
				)
			`).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "email column should exist after migration 002")

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("no migrations to apply", func(t *testing.T) {
		disk := fstest.MapFS{
			"migrations/readme.txt": &fstest.MapFile{
				Data: []byte("no sql files here"),
			},
		}

		m := migrator.NewMigrator(client, disk, logger)
		require.NoError(t, m.Run(ctx, "migrations"))
	})

	t.Run("incremental run only applies new migrations", func(t *testing.T) {
		dropTables(t, client, "schema_versions", "test_mig_incr")
		t.Cleanup(func() { dropTables(t, client, "schema_versions", "test_mig_incr") })

		disk1 := fstest.MapFS{
			"migrations/001_create_table.sql": &fstest.MapFile{
				Data: []byte("CREATE TABLE test_mig_incr (id serial PRIMARY KEY);"),
			},
		}

		m1 := migrator.NewMigrator(client, disk1, logger)
		require.NoError(t, m1.Run(ctx, "migrations"))

		disk2 := fstest.MapFS{
			"migrations/001_create_table.sql": &fstest.MapFile{
				Data: []byte("CREATE TABLE test_mig_incr (id serial PRIMARY KEY);"),
			},
			"migrations/002_add_name.sql": &fstest.MapFile{
				Data: []byte("ALTER TABLE test_mig_incr ADD COLUMN name TEXT;"),
			},
		}

		m2 := migrator.NewMigrator(client, disk2, logger)
		require.NoError(t, m2.Run(ctx, "migrations"))

		err := client.WithConn(ctx, func(ctx context.Context, conn pg.Querier) error {
			var count int
			err := conn.QueryRow(ctx, "SELECT count(*) FROM schema_versions").Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 2, count)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("returns error on invalid SQL", func(t *testing.T) {
		dropTables(t, client, "schema_versions")
		t.Cleanup(func() { dropTables(t, client, "schema_versions") })

		disk := fstest.MapFS{
			"migrations/001_bad.sql": &fstest.MapFile{
				Data: []byte("THIS IS NOT VALID SQL;"),
			},
		}

		m := migrator.NewMigrator(client, disk, logger)
		err := m.Run(ctx, "migrations")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot apply migration")
	})
}

func TestMigrations_LoadFromDir_InvalidDir(t *testing.T) {
	disk := fstest.MapFS{}

	var ms migrator.Migrations
	err := ms.LoadFromDir(disk, "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot read directory")
}

func TestMigration_LoadFromFile_NotFound(t *testing.T) {
	disk := fstest.MapFS{}

	var m migrator.Migration
	err := m.LoadFromFile(disk, "nonexistent.sql")
	require.Error(t, err)
}
