package rf3bench

import "runtime/debug"

// Qualification scripts set these at link time after checking the source
// revision and dirty state. Go test binaries do not normally carry vcs.*
// build settings. Never recover their provenance from a runtime checkout.
var buildRevision string
var buildModified string

// BuildProvenance reports the binary's source identity, not its environment.
// Missing, malformed or contradictory evidence remains explicitly unknown.
func BuildProvenance() (revision, modified string) {
	var settings []debug.BuildSetting
	if info, ok := debug.ReadBuildInfo(); ok {
		settings = info.Settings
	}
	return resolveBuildProvenance(buildRevision, buildModified, settings)
}

func resolveBuildProvenance(revision, modified string, settings []debug.BuildSetting) (string, string) {
	var vcsRevision, vcsModified string
	var haveRevision, haveModified bool
	for _, setting := range settings {
		switch setting.Key {
		case "vcs.revision":
			if haveRevision {
				return "unknown", "unknown"
			}
			haveRevision = true
			vcsRevision = setting.Value
		case "vcs.modified":
			if haveModified {
				return "unknown", "unknown"
			}
			haveModified = true
			vcsModified = setting.Value
		}
	}
	if revision == "" && modified == "" {
		revision, modified = vcsRevision, vcsModified
	} else if haveRevision || haveModified {
		if revision != vcsRevision || modified != vcsModified {
			return "unknown", "unknown"
		}
	}
	if len(revision) != 40 && len(revision) != 64 || modified != "true" && modified != "false" {
		return "unknown", "unknown"
	}
	for _, ch := range revision {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return "unknown", "unknown"
		}
	}
	return revision, modified
}
