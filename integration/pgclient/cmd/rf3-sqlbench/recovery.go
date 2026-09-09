package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
)

// The oracle is the client's independently updated expected state, not a
// database snapshot. Recovery verification never seeds rows or resets scores.
type recoveryOracle struct {
	Version           int      `json:"version"`
	Engine            string   `json:"engine"`
	Rows              int      `json:"rows"`
	Tables            []string `json:"tables"`
	Payload           string   `json:"payload"`
	PayloadMode       string   `json:"payload_mode,omitempty"`
	Indexes           string   `json:"indexes,omitempty"`
	SharedBytes       int      `json:"shared_bytes,omitempty"`
	SharedCardinality int      `json:"shared_cardinality,omitempty"`
	Scores            [][]int  `json:"scores"`
}

func writeRecoveryOracle(c config, tables []string, scores [][]int) error {
	file, err := os.OpenFile(c.recoveryOracle, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	oracle := recoveryOracle{
		Version: 1, Engine: c.engine, Rows: c.rows, Tables: tables,
		Payload: payload, PayloadMode: c.payloadMode,
		Indexes: normalizeIndexMode(c.indexes), Scores: scores,
	}
	if packIndexesEnabled(c) {
		oracle.SharedBytes = c.sharedBytes
		oracle.SharedCardinality = c.sharedCardinality
	}
	if err = json.NewEncoder(file).Encode(oracle); err != nil {
		return err
	}
	return file.Sync()
}

func readRecoveryOracle(c config, tables []string) ([][]int, error) {
	if c.recoveryOracle == "" {
		return nil, fmt.Errorf("recovery requires a client oracle")
	}
	file, err := os.Open(c.recoveryOracle)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 256<<20))
	decoder.DisallowUnknownFields()
	var oracle recoveryOracle
	if err = decoder.Decode(&oracle); err != nil {
		return nil, err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("trailing or oversized recovery oracle")
	}
	payloadMode := oracle.PayloadMode
	if payloadMode == "" {
		payloadMode = defaultPayloadMode
	}
	if oracle.Version != 1 || oracle.Engine != c.engine || oracle.Rows != c.rows ||
		(c.payloadMode != "varied-v1" && oracle.Payload != payload) ||
		payloadMode != c.payloadMode && !(c.payloadMode == "" && payloadMode == defaultPayloadMode) ||
		normalizeIndexMode(oracle.Indexes) != normalizeIndexMode(c.indexes) ||
		!slices.Equal(oracle.Tables, tables) || len(oracle.Scores) != len(tables) {
		return nil, fmt.Errorf("recovery oracle configuration mismatch")
	}
	if packIndexesEnabled(c) && (oracle.SharedBytes != c.sharedBytes || oracle.SharedCardinality != c.sharedCardinality) {
		return nil, fmt.Errorf("recovery oracle configuration mismatch")
	}
	for _, scores := range oracle.Scores {
		if len(scores) != c.rows {
			return nil, fmt.Errorf("recovery oracle row count mismatch")
		}
	}
	return oracle.Scores, nil
}
