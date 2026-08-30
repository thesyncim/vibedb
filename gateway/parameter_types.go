package gateway

import (
	"context"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

const (
	maxGatewaySQLParameters      = 1 << 16
	maxGatewaySQLBatchStatements = 10_000_000
	maxGatewaySQLBatchBytes      = uint64(1 << 30)
)

func validateQueryBatchAdmission(queries []Query, profile Profile) error {
	statements := profile.MaxTransactionMutations
	if statements > maxGatewaySQLBatchStatements {
		statements = maxGatewaySQLBatchStatements
	}
	bytes := profile.MaxTransactionBytes
	if bytes > maxGatewaySQLBatchBytes {
		bytes = maxGatewaySQLBatchBytes
	}
	return validateQueryBatchPhysicalAdmission(queries, statements, bytes)
}

// validateDurableSQLReplayAdmission is deliberately independent of mutable
// workload profiles and SQL analysis. It admits only caller bytes that fit the
// immutable digest envelope and have a canonical transport representation, so
// a retained request remains replayable after profiles or analyzers change.
func validateDurableSQLReplayAdmission(queries []Query) error {
	return validateQueryBatchPhysicalAdmission(
		queries, maxGatewaySQLBatchStatements, maxGatewaySQLBatchBytes,
	)
}

// validateQueryBatchPhysicalAdmission makes one complete, non-parsing pass
// over a batch. Callers run it before catalog pinning or semantic preparation,
// preventing an early statement from consuming parser/catalog work before a
// later statement's malformed metadata or oversized payload is discovered.
func validateQueryBatchPhysicalAdmission(
	queries []Query,
	maxStatements uint64,
	maxBytes uint64,
) error {
	if len(queries) == 0 || maxStatements == 0 || maxBytes == 0 ||
		uint64(len(queries)) > maxStatements {
		return ErrTransactionMutationLimit
	}
	class := queries[0].Class
	if class > ClassAdmin {
		return fmt.Errorf("%w: invalid operation class %d", ErrBatchClassMismatch, class)
	}
	var bytes uint64
	for index := range queries {
		candidate := &queries[index]
		if candidate.Class != class {
			return ErrBatchClassMismatch
		}
		if err := validateSQLParameterTypes(candidate.Params, candidate.ParamTypes); err != nil {
			return err
		}
		queryBytes := uint64(16) + uint64(len(candidate.SQL)) +
			uint64(len(candidate.ParamTypes))
		for parameter := range candidate.Params {
			payloadBytes := uint64(8) +
				uint64(len(candidate.Params[parameter].Bytes))
			if queryBytes > maxBytes || payloadBytes > maxBytes-queryBytes {
				return ErrTransactionByteLimit
			}
			queryBytes += payloadBytes
		}
		if bytes > maxBytes || queryBytes > maxBytes-bytes {
			return ErrTransactionByteLimit
		}
		bytes += queryBytes
	}
	return nil
}

func validateTypedQueries(ctx context.Context, queries []Query) error {
	for index := range queries {
		if err := validateTypedQuery(ctx, &queries[index]); err != nil {
			return err
		}
	}
	return nil
}

// validateSQLParameterTypes validates only the bounded transport metadata. It
// deliberately does not parse SQL, so callers can use it during physical
// admission before attacker-controlled text reaches the parser.
func validateSQLParameterTypes(
	params []shardservice.Param,
	parameterTypes []sqldriver.ParamType,
) error {
	if len(params) > maxGatewaySQLParameters ||
		len(parameterTypes) > maxGatewaySQLParameters {
		return fmt.Errorf(
			"%w: %d parameter types for %d values",
			ErrPlanParameters, len(parameterTypes), len(params),
		)
	}
	for index := range params {
		if !params[index].Valid() {
			return fmt.Errorf(
				"%w: parameter %d has an invalid payload",
				ErrPlanParameters, index+1,
			)
		}
	}
	if len(parameterTypes) == 0 {
		return nil
	}
	if len(parameterTypes) != len(params) {
		return fmt.Errorf(
			"%w: %d parameter types for %d values",
			ErrPlanParameters, len(parameterTypes), len(params),
		)
	}
	hasType := false
	for index, parameterType := range parameterTypes {
		if parameterType >= sqldriver.ParamTypeInvalid {
			return fmt.Errorf(
				"%w: parameter %d has invalid SQL type %d",
				ErrPlanParameters, index+1, parameterType,
			)
		}
		if parameterType != sqldriver.ParamTypeUnspecified {
			if params[index].Kind == shardservice.ParamDocument {
				return fmt.Errorf(
					"%w: document parameter %d has scalar SQL type %s",
					ErrPlanParameters, index+1, parameterType,
				)
			}
			hasType = true
		}
	}
	if !hasType {
		return fmt.Errorf("%w: parameter type vector is all unspecified", ErrPlanParameters)
	}
	return nil
}

// validateTypedQuery performs the optional typed path's complete semantic
// preparation before routing, durable staging, or hashing can make the request
// externally observable. Nil metadata returns immediately and preserves the
// ordinary parser/allocation path.
func validateTypedQuery(ctx context.Context, candidate *Query) error {
	if candidate == nil || len(candidate.ParamTypes) == 0 {
		return nil
	}
	if err := validateSQLParameterTypes(candidate.Params, candidate.ParamTypes); err != nil {
		return err
	}

	var parser sqlast.Parser
	if ctx != nil {
		parser.SetCancellationCheck(ctx.Err)
	}
	var parsed sqlast.Statement
	if err := parser.ParseStatement(&parsed, candidate.SQL); err != nil {
		return err
	}
	if parsed.Params() != len(candidate.Params) {
		return fmt.Errorf(
			"%w: SQL has %d parameters, request has %d",
			ErrPlanParameters, parsed.Params(), len(candidate.Params),
		)
	}
	parameterTypes, err := postgresQueryParameterTypes(
		candidate.ParamTypes, parsed.Params(),
	)
	if err != nil {
		return err
	}
	if parsed.Kind == sqlast.KindSelect {
		statement, err := query.PrepareParsedStatementWithParameterTypes(
			candidate.SQL, parsed.Select, parameterTypes,
		)
		if err != nil {
			return err
		}
		statement.Release()
		return nil
	}
	if parsed.Kind != sqlast.KindInsert && parsed.Kind != sqlast.KindUpdate &&
		parsed.Kind != sqlast.KindDelete {
		return fmt.Errorf(
			"%w: %s cannot carry SQL parameter type metadata",
			ErrPlanParameters, parsed.Kind,
		)
	}
	for index := range postgresWriteDocumentParameters(&parsed) {
		if index < len(candidate.ParamTypes) &&
			candidate.ParamTypes[index] != sqldriver.ParamTypeUnspecified {
			return fmt.Errorf(
				"%w: whole-document parameter %d has scalar SQL type %s",
				ErrPlanParameters, index+1, candidate.ParamTypes[index],
			)
		}
	}
	statement, err := query.PrepareParsedDMLWithParameterTypes(
		candidate.SQL, &parsed, parameterTypes,
	)
	if err != nil {
		return err
	}
	statement.Release()
	return nil
}
