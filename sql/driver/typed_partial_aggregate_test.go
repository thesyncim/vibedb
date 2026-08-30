package driver

import (
	"context"
	"testing"
)

func TestPreparePartialAggregateWithParameterTypes(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	create := runtimePrepare(t, session,
		`CREATE TABLE typed_partial_aggregate (id STRING PRIMARY KEY, flag BOOLEAN NOT NULL)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		create.Close()
		t.Fatal(err)
	}
	create.Close()

	const source = `SELECT flag, COUNT(*) FROM typed_partial_aggregate ` +
		`WHERE flag IN (SELECT ? UNION ALL SELECT ?) GROUP BY flag`
	prepared, err := session.PreparePartialAggregateWithParameterTypes(
		context.Background(), source,
		[]ParamType{ParamTypeBool, ParamTypeUnspecified},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if prepared.ParamType(0) != ParamTypeBool ||
		prepared.ParamType(1) != ParamTypeBool {
		t.Fatalf("partial aggregate parameter types = %s/%s, want boolean/boolean",
			prepared.ParamType(0), prepared.ParamType(1))
	}
}
