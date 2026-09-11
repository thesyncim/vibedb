package main

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	indexModeNone           = "none"
	indexModePackLeading    = "pack-leading"
	indexModePackNonleading = "pack-nonleading"
	maxRows                 = 10_000_000
	packAlphabet            = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
	variedPayloadAlphabet   = packAlphabet
	defaultPayloadMode      = "constant"
)

func packIndexesEnabled(c config) bool {
	return c.indexes == indexModePackLeading || c.indexes == indexModePackNonleading
}

func normalizeIndexMode(mode string) string {
	if mode == "" {
		return indexModeNone
	}
	return mode
}

func tableDDL(c config, table string) string {
	ddl := "CREATE TABLE IF NOT EXISTS " + table + " (id TEXT PRIMARY KEY, bucket INTEGER NOT NULL, score INTEGER NOT NULL, payload TEXT NOT NULL"
	if packIndexesEnabled(c) {
		ddl += ", shared TEXT NOT NULL, a TEXT NOT NULL, b TEXT NOT NULL"
	}
	return ddl + ")"
}

func indexDDLs(c config, table string) []string {
	if !packIndexesEnabled(c) {
		return nil
	}
	if c.indexes == indexModePackNonleading {
		return []string{
			"CREATE INDEX IF NOT EXISTS " + table + "_pack_a ON " + table + " (bucket, a, shared)",
			"CREATE INDEX IF NOT EXISTS " + table + "_pack_b ON " + table + " (bucket, b, shared)",
		}
	}
	return []string{
		"CREATE INDEX IF NOT EXISTS " + table + "_pack_a ON " + table + " (bucket, shared, a)",
		"CREATE INDEX IF NOT EXISTS " + table + "_pack_b ON " + table + " (bucket, shared, b)",
	}
}

func packMix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
	value = (value ^ value>>27) * 0x94d049bb133111eb
	return value ^ value>>31
}

func packSharedValue(c config, row int) string {
	cardinality := c.sharedCardinality
	if cardinality < 1 {
		cardinality = 8
	}
	length := c.sharedBytes
	if length < 1 {
		length = 256
	}
	id := int(packMix(uint64(row)+1009) % uint64(cardinality))
	value := make([]byte, length)
	state := packMix(uint64(id) + 71)
	for i := range value {
		state = packMix(state + uint64(i))
		value[i] = packAlphabet[state%uint64(len(packAlphabet))]
	}
	return string(value)
}

func packA(row int) string { return fmt.Sprintf("a%04d", row%997) }
func packB(row int) string { return fmt.Sprintf("b%04d", row%991) }

func payloadFor(c config, row int) string {
	if c.payloadMode != "varied-v1" {
		return payload
	}
	var value [256]byte
	x := mixOrdinal(uint64(row) + 0x1d2b79f5aa33cc77)
	for i := range value {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 0x2545f4914f6cdd1d
		value[i] = variedPayloadAlphabet[(x>>58)&63]
	}
	return string(value[:])
}

func insertColumns(c config) string {
	if packIndexesEnabled(c) {
		return "(id,bucket,score,payload,shared,a,b)"
	}
	return "(id,bucket,score,payload)"
}

func appendInsertRow(sql *strings.Builder, c config, row int) {
	if packIndexesEnabled(c) {
		fmt.Fprintf(sql, "('%s',%d,%d,'%s','%s','%s','%s')",
			key(row), row%16, row%100, payloadFor(c, row), packSharedValue(c, row), packA(row), packB(row))
		return
	}
	fmt.Fprintf(sql, "('%s',%d,%d,'%s')", key(row), row%16, row%100, payloadFor(c, row))
}

func verifySelectSQL(c config, table string) string {
	if packIndexesEnabled(c) {
		return "SELECT id,bucket,score,payload,shared,a,b FROM " + table + " WHERE id >= $1 ORDER BY id LIMIT 512"
	}
	return "SELECT id,bucket,score,payload FROM " + table + " WHERE id >= $1 ORDER BY id LIMIT 512"
}

func verifyColumnCount(c config) int {
	if packIndexesEnabled(c) {
		return 7
	}
	return 4
}

func primaryRowMatches(c config, row [][]byte, group, id int, scores [][]int) bool {
	if len(row) < 4 {
		return false
	}
	if textCell(c, row[0]) != key(id) || string(row[1]) != strconv.Itoa(id%16) ||
		string(row[2]) != strconv.Itoa(scores[group][id]) || textCell(c, row[3]) != payloadFor(c, id) {
		return false
	}
	if !packIndexesEnabled(c) {
		return len(row) == 4
	}
	return len(row) == 7 &&
		textCell(c, row[4]) == packSharedValue(c, id) &&
		textCell(c, row[5]) == packA(id) &&
		textCell(c, row[6]) == packB(id)
}
