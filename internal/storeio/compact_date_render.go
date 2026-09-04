package storeio

// A March-based four-year cycle ends with leap day. Gregorian centuries omit
// that final day except every 400 years; the era adjustment below skips those
// three missing days before indexing this 2,922-byte read-only table.
var compactDateFourYear = func() (days [1461]uint16) {
	monthDays := [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	year, month, day := 0, 3, 1
	for i := range days {
		days[i] = uint16(year<<9 | month<<5 | day)
		limit := monthDays[month-1]
		if month == 2 && year%4 == 0 {
			limit++
		}
		day++
		if day > limit {
			day = 1
			month++
			if month > 12 {
				month = 1
				year++
			}
		}
	}
	return
}()

func appendCompactDate(dst []byte, ordinal int32) []byte {
	if ordinal < 0 || ordinal >= int32(compactDaysBeforeYear(10_000)) {
		return appendCompactDateArithmetic(dst, ordinal)
	}
	z := int(ordinal) - 60
	era := z / 146097
	if z < 0 {
		era = -1
	}
	day := z - era*146097
	day += min(day/36524, 3)
	cycle := day / 1461
	date := compactDateFourYear[day-cycle*1461]
	year := era*400 + cycle*4 + int(date>>9)
	month, dom := int(date>>5&15)*2, int(date&31)*2
	century, suffix := year/100*2, year%100*2
	return append(dst,
		'"', canonicalDecimalPairs[century], canonicalDecimalPairs[century+1],
		canonicalDecimalPairs[suffix], canonicalDecimalPairs[suffix+1], '-',
		canonicalDecimalPairs[month], canonicalDecimalPairs[month+1], '-',
		canonicalDecimalPairs[dom], canonicalDecimalPairs[dom+1], '"',
	)
}
