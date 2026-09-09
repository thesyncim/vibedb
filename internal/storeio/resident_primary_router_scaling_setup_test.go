package storeio

// residentPrimaryRouterScalingFinalize installs the candidate's persistent
// route representations. Fixture construction remains outside benchmark time.
func residentPrimaryRouterScalingFinalize(router *ResidentPrimaryRouter) {
	router.buildSearchKeys()
	router.buildPersistentTree()
	router.fences = nil
	router.rows = nil
	router.hints = nil
	router.empty = nil
	router.searchKeys = nil
	router.searchTops = nil
}
