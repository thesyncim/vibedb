package query_test

import (
	"fmt"
	"log"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

func exampleSegment() *store.Segment {
	set := &store.Segment{}
	for _, doc := range []string{
		`{"team":"infra","tier":"pro","score":7,"active":true}`,
		`{"team":"infra","tier":"free","score":3,"active":true}`,
		`{"team":"data","tier":"pro","score":9,"active":true}`,
		`{"team":"data","tier":"team","score":4,"active":false}`,
		`{"team":"web","tier":"free","score":5,"active":true}`,
	} {
		if _, err := set.Append([]byte(doc)); err != nil {
			log.Fatal(err)
		}
	}
	return set
}

// A query written as a JSON document. Sibling keys of a filter conjoin, so an
// all-of condition needs no explicit operator.
func ExampleNew() {
	q, err := query.New(query.M{
		"select": query.A{
			"team",
			query.M{"total": query.M{"$sum": "score"}},
			query.M{"n": query.M{"$count": nil}},
		},
		"where": query.M{
			"active": true,
			"tier":   query.M{"$in": query.A{"pro", "team"}},
		},
		"groupBy": "team",
		"orderBy": query.A{query.M{"team": "asc"}},
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := q.Run(query.FromSegment(exampleSegment()))
	if err != nil {
		log.Fatal(err)
	}
	teams, _ := result.Column("team")
	totals, _ := result.Column("total")
	counts, _ := result.Column("n")
	for row := range result.RowCount {
		fmt.Printf("%s total=%s n=%s\n", teams.Cells[row], totals.Cells[row], counts.Cells[row])
	}
	// Output:
	// "data" total=9 n=1
	// "infra" total=7 n=1
}

// The same query as JSON text. A query can therefore be stored in a config
// file or received over a wire and compiled to the same plan; every number
// keeps its original spelling, so an integer past float64's exact range
// compiles to an exact literal.
func ExampleParse() {
	q, err := query.Parse([]byte(`{
		"select":  ["team", {"best": {"$max": "score"}}],
		"where":   {"score": {"$gte": 4}},
		"groupBy": ["team"],
		"orderBy": [{"team": "asc"}]
	}`))
	if err != nil {
		log.Fatal(err)
	}

	result, err := q.Run(query.FromSegment(exampleSegment()))
	if err != nil {
		log.Fatal(err)
	}
	teams, _ := result.Column("team")
	best, _ := result.Column("best")
	for row := range result.RowCount {
		fmt.Printf("%s best=%s\n", teams.Cells[row], best.Cells[row])
	}
	// Output:
	// "data" best=9
	// "infra" best=7
	// "web" best=5
}

// A hot loop retains one Exec. Its Result and Workspace grow to the largest
// execution they have served and are then reused, so a warmed run over an
// unchanged shape allocates nothing. The Source is what names the collection,
// so the same loop reaches a heap snapshot or a durable one by swapping
// FromSegment for FromSnapshot or FromFile.
func ExampleQuery_RunInto() {
	q := query.Select(query.Path("team"), query.Sum("score")).
		Where(query.Cmp("active", query.Eq, true)).
		GroupBy("team").
		OrderBy("team", query.Asc)

	src := query.FromSegment(exampleSegment())
	var e query.Exec
	for range 3 { // the second and third runs reuse every buffer the first grew
		if err := q.RunInto(&e, src); err != nil {
			log.Fatal(err)
		}
	}
	teams, _ := e.Result.Column("team")
	totals, _ := e.Result.Column("sum(score)")
	for row := range e.Result.RowCount {
		fmt.Printf("%s %s\n", teams.Cells[row], totals.Cells[row])
	}
	// Output:
	// "data" 9
	// "infra" 10
	// "web" 5
}

// In is the membership form of an equality. It compiles to a sorted set that
// the executor binary-searches once per row, so its cost grows with the log of
// the alternatives rather than with their number.
func ExampleIn() {
	q := query.Select(query.Path("team"), query.Path("score")).
		Where(query.In("tier", "pro", "team")).
		OrderBy("score", query.Desc)

	result, err := q.Run(query.FromSegment(exampleSegment()))
	if err != nil {
		log.Fatal(err)
	}
	teams, _ := result.Column("team")
	scores, _ := result.Column("score")
	for row := range result.RowCount {
		fmt.Printf("%s %s\n", teams.Cells[row], scores.Cells[row])
	}
	// Output:
	// "data" 9
	// "infra" 7
	// "data" 4
}

// exampleDatabase publishes two collections into one catalog, so a single
// Database.Snapshot captures both at the same instant.
func exampleDatabase() *store.Database {
	db := &store.Database{}
	orders, err := db.CreateCollection("orders", store.Options{})
	if err != nil {
		log.Fatal(err)
	}
	for key, doc := range map[string]string{
		"o1": `{"id":1,"customer_id":"c1","total":30,"active":true}`,
		"o2": `{"id":2,"customer_id":"c2","total":10,"active":true}`,
		"o3": `{"id":3,"customer_id":"c3","total":50,"active":true}`,
		"o4": `{"id":4,"customer_id":"c1","total":20,"active":false}`,
	} {
		if _, err := orders.Put(key, []byte(doc)); err != nil {
			log.Fatal(err)
		}
	}
	customers, err := db.CreateCollection("customers", store.Options{})
	if err != nil {
		log.Fatal(err)
	}
	for key, doc := range map[string]string{
		"c1": `{"tier":"pro","region":"eu"}`,
		"c2": `{"tier":"free","region":"eu"}`,
		"c3": `{"tier":"pro","region":"us"}`,
	} {
		if _, err := customers.Put(key, []byte(doc)); err != nil {
			log.Fatal(err)
		}
	}
	return db
}

// A cross-collection semi-join keeps the orders whose customer is a pro
// customer. The join filters and returns no column of its own; "$key" names the
// joined collection's primary key, the common foreign-key case.
func ExampleQuery_Join() {
	q := query.Select(query.Path("id"), query.Path("total")).
		Where(query.Cmp("active", query.Eq, true)).
		Join(query.JoinOn("customers", "customer_id", query.JoinKey).
			Where(query.Cmp("tier", query.Eq, "pro"))).
		OrderBy("id", query.Asc)

	// Both sides come from one DatabaseSnapshot, so the join cannot resolve the
	// inner collection against a state the driving side never saw.
	catalog := exampleDatabase().Snapshot()
	result, err := q.Run(query.FromDatabase(catalog, "orders"))
	if err != nil {
		log.Fatal(err)
	}
	for row := 0; row < result.RowCount; row++ {
		fmt.Printf("order %s total %s\n",
			result.Columns[0].Cells[row].JSON(), result.Columns[1].Cells[row].JSON())
	}
	// Output:
	// order 1 total 30
	// order 3 total 50
}

// The same join written as a query document, so it can be stored, logged, or
// received over a wire.
func ExampleParse_join() {
	q, err := query.Parse([]byte(`{
		"select": ["id"],
		"where":  {"active": true},
		"join":   [{"from": "customers", "on": {"customer_id": "$key"}, "where": {"region": "eu"}}],
		"orderBy": ["id"]
	}`))
	if err != nil {
		log.Fatal(err)
	}
	catalog := exampleDatabase().Snapshot()
	result, err := q.Run(query.FromDatabase(catalog, "orders"))
	if err != nil {
		log.Fatal(err)
	}
	for row := 0; row < result.RowCount; row++ {
		fmt.Printf("%s\n", result.Columns[0].Cells[row].JSON())
	}
	// Output:
	// 1
	// 2
}
