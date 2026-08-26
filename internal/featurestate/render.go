package featurestate

import (
	"bytes"
	"fmt"
	"strings"
)

func RenderMarkdown() []byte {
	var out bytes.Buffer
	out.WriteString("# Distributed feature state\n\n")
	out.WriteString("> Generated from `internal/featurestate.Distributed`. Do not edit this table by hand.\n\n")
	out.WriteString("VibeDB has no tagged release. This ledger describes the current repository and the single path toward its first release. It does not describe compatibility generations or promise unfinished work.\n\n")
	out.WriteString("A **Yes** in the proof column requires a fault or benchmark gate for the stated contract. Ordinary correctness tests produce **Partial**, not **Yes**. A command is shipped when `vibedb-gateway` or `vibedb-shard` constructs the path. Internal test harnesses do not count as shipped commands.\n\n")
	out.WriteString("| Feature | Primitive implemented | Internally integrated | Used by shipped commands | Proven under fault or benchmark gates |\n")
	out.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, feature := range Distributed {
		fmt.Fprintf(&out, "| %s | %s | %s | %s | %s |\n",
			escape(feature.Name), renderStage(feature.Primitive),
			renderStage(feature.Integrated), renderStage(feature.Shipped),
			renderStage(feature.Qualification))
	}
	out.WriteString("\n## How to update this page\n\n")
	out.WriteString("1. Change `internal/featurestate/manifest.go` and cite production code or an executable test.\n")
	out.WriteString("2. Run `go generate ./internal/featurestate`.\n")
	out.WriteString("3. Run `go test ./internal/featurestate`. The test rejects stale output, invalid state transitions, duplicate rows, and missing evidence files.\n")
	out.WriteString("\nThe embedded capability matrix remains in [capabilities.md](capabilities.md). Operational instructions for the current distributed commands remain in [operations/distributed.md](operations/distributed.md).\n")
	return out.Bytes()
}

func renderStage(stage Stage) string {
	var cell strings.Builder
	fmt.Fprintf(&cell, "**%s.** %s", stage.Status.label(), escape(stage.Contract))
	if len(stage.Evidence) != 0 {
		cell.WriteString("<br><sub>Evidence: ")
		for index, evidence := range stage.Evidence {
			if index != 0 {
				cell.WriteString(", ")
			}
			fmt.Fprintf(&cell, "[%s](../%s) — `%s`", escape(evidence.Path),
				escape(evidence.Path), escape(evidence.Symbol))
		}
		cell.WriteString("</sub>")
	}
	return cell.String()
}

func escape(value string) string {
	value = strings.ReplaceAll(value, "|", "&#124;")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}
