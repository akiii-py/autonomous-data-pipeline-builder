package database

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"time"

	_ "github.com/lib/pq"
)

func Connect(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	log.Println("connected to PostgreSQL")
	return db, nil
}

const schemaMigrationsDDL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version     INT PRIMARY KEY,
	name        TEXT NOT NULL,
	checksum    TEXT NOT NULL,
	applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

// AppliedMigration is one row of the schema history.
type AppliedMigration struct {
	Version   int
	Name      string
	Checksum  string
	AppliedAt time.Time
}

// RunMigrations applies every unapplied migration in version order, inside a
// transaction per migration, and records it in schema_migrations (D-11).
//
// A migration whose recorded checksum no longer matches its source is a hard
// error: it means an applied migration was edited, and the database's actual
// shape can no longer be derived from this file.
func RunMigrations(db *sql.DB) error {
	if _, err := db.Exec(schemaMigrationsDDL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := appliedMigrations(db)
	if err != nil {
		return err
	}

	pending := append([]Migration(nil), migrations...)
	sort.Slice(pending, func(i, j int) bool { return pending[i].Version < pending[j].Version })

	count := 0
	for _, m := range pending {
		sum := checksum(m.SQL)

		if prev, ok := applied[m.Version]; ok {
			if prev.Checksum != sum {
				return fmt.Errorf(
					"migration %d (%s) was modified after being applied: recorded checksum %s, current %s",
					m.Version, m.Name, prev.Checksum, sum,
				)
			}
			continue
		}

		if err := applyOne(db, m, sum); err != nil {
			return err
		}
		count++
		log.Printf("applied migration %d: %s", m.Version, m.Name)
	}

	if count == 0 {
		log.Printf("database schema up to date (version %d)", currentVersion(applied, pending))
	} else {
		log.Printf("database migrations applied: %d", count)
	}
	return nil
}

func applyOne(db *sql.DB, m Migration, sum string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.Version, err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(m.SQL); err != nil {
		return fmt.Errorf("migration %d (%s): %w", m.Version, m.Name, err)
	}

	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
		m.Version, m.Name, sum,
	); err != nil {
		return fmt.Errorf("record migration %d: %w", m.Version, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %d: %w", m.Version, err)
	}
	return nil
}

func appliedMigrations(db *sql.DB) (map[int]AppliedMigration, error) {
	rows, err := db.Query(`SELECT version, name, checksum, applied_at FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()

	out := make(map[int]AppliedMigration)
	for rows.Next() {
		var m AppliedMigration
		if err := rows.Scan(&m.Version, &m.Name, &m.Checksum, &m.AppliedAt); err != nil {
			return nil, fmt.Errorf("scan schema_migrations: %w", err)
		}
		out[m.Version] = m
	}
	return out, rows.Err()
}

// SchemaVersion answers "what schema am I at?", which was unanswerable while
// migrations were unversioned constants.
func SchemaVersion(db *sql.DB) (int, error) {
	var v sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&v); err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return int(v.Int64), nil
}

func currentVersion(applied map[int]AppliedMigration, all []Migration) int {
	max := 0
	for v := range applied {
		if v > max {
			max = v
		}
	}
	for _, m := range all {
		if m.Version > max {
			max = m.Version
		}
	}
	return max
}

func checksum(sqlText string) string {
	sum := sha256.Sum256([]byte(sqlText))
	return hex.EncodeToString(sum[:])
}
