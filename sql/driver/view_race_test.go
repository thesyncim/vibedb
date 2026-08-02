package driver

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestOrdinaryViewConcurrentDropTruncateAndIndexPublication(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(8)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`,
		`INSERT INTO docs VALUES ({"id":"seed","n":0})`,
		`CREATE VIEW live_docs AS SELECT id, n FROM docs`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	const readers = 4
	const iterations = 20
	start := make(chan struct{})
	errorsOut := make(chan error, readers+1)
	var wait sync.WaitGroup
	for reader := 0; reader < readers; reader++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for iteration := 0; iteration < iterations*3; iteration++ {
				rows, err := db.Query(`SELECT id, n FROM live_docs ORDER BY id`)
				if err != nil {
					if errors.Is(err, ErrViewChanged) || errors.Is(err, ErrTableNotFound) {
						continue
					}
					errorsOut <- err
					return
				}
				for rows.Next() {
					var id string
					var n float64
					if err := rows.Scan(&id, &n); err != nil {
						rows.Close()
						errorsOut <- err
						return
					}
				}
				if err := rows.Close(); err != nil {
					errorsOut <- err
					return
				}
				if err := rows.Err(); err != nil {
					errorsOut <- err
					return
				}
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		for iteration := 0; iteration < iterations; iteration++ {
			statements := []string{
				`DROP VIEW live_docs`,
				`CREATE VIEW live_docs AS SELECT id, n FROM docs`,
				`TRUNCATE docs`,
				fmt.Sprintf(
					`INSERT INTO docs VALUES ({"id":"row-%d","n":%d})`,
					iteration, iteration,
				),
				`CREATE INDEX by_n ON docs (n)`,
				`DROP INDEX by_n ON docs`,
			}
			for _, statement := range statements {
				if _, err := db.Exec(statement); err != nil {
					errorsOut <- fmt.Errorf("%s: %w", statement, err)
					return
				}
			}
		}
	}()
	close(start)
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
}
