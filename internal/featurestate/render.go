package featurestate

import (
	"bytes"
	"fmt"
	"strings"
)

const markdownWrapWidth = 96

func RenderMarkdown() []byte {
	var out bytes.Buffer
	out.WriteString("# Distributed feature ledger\n\n")
	out.WriteString("> [!CAUTION]\n")
	writeWrapped(&out, "> ", "> ", "VibeDB is unreleased development software. Any commit may break APIs, commands, wire protocols, or disk formats. Use documentation and binaries from the exact same commit, and store only disposable or independently recoverable data. A **Yes** below does not mean supported, production-ready, or compatible with another commit. Consult [current status](status.md) for known failing tests and defects.")
	out.WriteByte('\n')
	out.WriteString("> [!NOTE]\n")
	writeWrapped(&out, "> ", "> ", "Generated from `internal/featurestate.Distributed`. Change the manifest, not this file.")
	out.WriteByte('\n')
	writeWrapped(&out, "", "", "This ledger separates code existence from integration, checked-in development commands, and external fault or performance qualification. It describes only the exact commit that generated it.")
	out.WriteByte('\n')

	out.WriteString("## State legend\n\n")
	writeWrapped(&out, "", "", "The state applies independently to every stage. Read the adjacent contract for the exact boundary.")
	out.WriteByte('\n')
	writeWrapped(&out, "- **Yes** — ", "  ", "The manifest makes the complete stage claim written in its contract and cites evidence.")
	writeWrapped(&out, "- **Partial** — ", "  ", "Only the subset stated in the contract is present or qualified; the contract names material gaps.")
	writeWrapped(&out, "- **No** — ", "  ", "The manifest makes no positive claim for that stage; the contract states the current boundary.")
	out.WriteByte('\n')
	writeWrapped(&out, "", "", "A qualification **Yes** requires the fault or benchmark evidence stated by that contract. Ordinary correctness tests alone remain **Partial**. A development-command **Yes** means a checked-in command constructs the path; it is not a release claim.")
	out.WriteByte('\n')

	out.WriteString("## Stage model\n\n")
	out.WriteString("| Stage | Question answered |\n")
	out.WriteString("| --- | --- |\n")
	out.WriteString("| Primitive | Does the underlying code, codec, or protocol exist? |\n")
	out.WriteString("| Integrated | Does a repository path compose and use it? |\n")
	out.WriteString("| Development command | Does a checked-in command or CLI construct the path? |\n")
	out.WriteString("| Qualification | Has the stated contract passed the cited fault or performance gate? |\n\n")

	out.WriteString("## Summary\n\n")
	out.WriteString("| Feature | Primitive | Integrated | Development command | Qualification |\n")
	out.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, feature := range Distributed {
		fmt.Fprintf(&out, "| %s | **%s** | **%s** | **%s** | **%s** |\n",
			escape(feature.Name), feature.Primitive.Status.label(),
			feature.Integrated.Status.label(), feature.Shipped.Status.label(),
			feature.Qualification.Status.label())
	}
	out.WriteString("\n## Contracts\n\n")
	writeWrapped(&out, "", "", "Expand a feature for its exact stage contracts. Each table lists its source and test evidence once, even when several stages rely on the same reference.")
	out.WriteByte('\n')
	for _, feature := range Distributed {
		renderFeature(&out, feature)
	}

	out.WriteString("\n## Regenerate\n\n")
	writeWrapped(&out, "1. ", "   ", "Change `internal/featurestate/manifest.go` and cite production code or an executable test.")
	writeWrapped(&out, "2. ", "   ", "Run `go generate ./internal/featurestate`.")
	writeWrapped(&out, "3. ", "   ", "Run `go test ./internal/featurestate`. The test rejects stale output, invalid state transitions, duplicate rows, and missing evidence files.")
	out.WriteByte('\n')
	writeWrapped(&out, "", "", "See the [embedded capability matrix](capabilities.md) for local entry points and [distributed operations](operations/distributed.md) for runtime behavior.")
	return out.Bytes()
}

func renderFeature(out *bytes.Buffer, feature Feature) {
	evidenceIDs, evidence := collectFeatureEvidence(feature)

	fmt.Fprintf(out, "<details>\n<summary><strong>%s</strong></summary>\n\n", escape(feature.Name))
	out.WriteString("| Stage | Exact contract |\n")
	out.WriteString("| --- | --- |\n")
	renderStageRow(out, "Primitive", feature.Primitive, evidenceIDs)
	renderStageRow(out, "Integrated", feature.Integrated, evidenceIDs)
	renderStageRow(out, "Development command", feature.Shipped, evidenceIDs)
	renderStageRow(out, "Qualification", feature.Qualification, evidenceIDs)
	if len(evidence) != 0 {
		out.WriteString("| Evidence key | ")
		for index, reference := range evidence {
			if index != 0 {
				out.WriteString("<br>")
			}
			fmt.Fprintf(out, "**E%d** [%s](../%s) — `%s`", index+1,
				escape(reference.Path), escape(reference.Path), escape(reference.Symbol))
		}
		out.WriteString(" |\n")
	}
	out.WriteString("\n</details>\n\n")
}

func collectFeatureEvidence(feature Feature) (map[Reference]int, []Reference) {
	ids := make(map[Reference]int)
	var ordered []Reference
	stages := [...]Stage{
		feature.Primitive,
		feature.Integrated,
		feature.Shipped,
		feature.Qualification,
	}
	for _, stage := range stages {
		for _, reference := range stage.Evidence {
			if _, exists := ids[reference]; exists {
				continue
			}
			ordered = append(ordered, reference)
			ids[reference] = len(ordered)
		}
	}
	return ids, ordered
}

func renderStageRow(out *bytes.Buffer, title string, stage Stage, evidenceIDs map[Reference]int) {
	fmt.Fprintf(out, "| **%s — %s** | %s", title, stage.Status.label(), escape(stage.Contract))
	if len(stage.Evidence) != 0 {
		out.WriteString("<br><sub>Evidence: ")
		for index, reference := range stage.Evidence {
			if index != 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(out, "**E%d**", evidenceIDs[reference])
		}
		out.WriteString("</sub>")
	}
	out.WriteString(" |\n")
}

func writeWrapped(out *bytes.Buffer, prefix, continuation, value string) {
	words := strings.Fields(value)
	out.WriteString(prefix)
	column := len(prefix)
	for index, word := range words {
		separator := 0
		if index != 0 {
			separator = 1
		}
		if index != 0 && column+separator+len(word) > markdownWrapWidth {
			out.WriteByte('\n')
			out.WriteString(continuation)
			column = len(continuation)
			separator = 0
		}
		if separator != 0 {
			out.WriteByte(' ')
			column++
		}
		out.WriteString(word)
		column += len(word)
	}
	out.WriteByte('\n')
}

func escape(value string) string {
	value = strings.ReplaceAll(value, "|", "&#124;")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}
