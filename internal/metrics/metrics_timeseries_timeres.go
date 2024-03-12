package metrics

import "time"

const (
	daysPerWeek      = 7
	monthsPerQuarter = 3
)

// TimeResolutionFunc is a function that maps given point in time to a beginning and end of
// a time period, such as hour, day, week, month, quarter, or year.
type TimeResolutionFunc func(time.Time) (time.Time, time.Time)

// TimeResolutionByHour is a time resolution function that maps given time to a beginning and end of an hour.
func TimeResolutionByHour(t time.Time) (hourStart, nextHourStart time.Time) {
	t0 := t.Truncate(time.Hour)
	t1 := t0.Add(time.Hour)

	return t0, t1
}

// TimeResolutionByDay is a time resolution function that maps given time to a beginning and end of a day.
func TimeResolutionByDay(t time.Time) (dayStart, nextDayStart time.Time) {
	y, m, d := t.Date()

	d0 := time.Date(y, m, d, 0, 0, 0, 0, t.Location())
	d1 := d0.AddDate(0, 0, 1)

	return d0, d1
}

// TimeResolutionByQuarter is a time resolution function that maps given time to a beginning and end of a quarter (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec).
func TimeResolutionByQuarter(t time.Time) (quarterStart, nextQuarterStart time.Time) {
	d0 := startOfQuarter(t)
	d1 := d0.AddDate(0, monthsPerQuarter, 0)

	return d0, d1
}

// TimeResolutionByWeekStartingSunday is a time resolution function that maps given time to a beginning and end of a week (starting Sunday).
func TimeResolutionByWeekStartingSunday(t time.Time) (weekStart, nextWeekStart time.Time) {
	y, m, d := t.Date()

	d0 := startOfSundayBasedWeek(time.Date(y, m, d, 0, 0, 0, 0, t.Location()))
	d1 := d0.AddDate(0, 0, daysPerWeek)

	return d0, d1
}

// TimeResolutionByWeekStartingMonday is a time resolution function that maps given time to a beginning and end of a week (starting Sunday).
func TimeResolutionByWeekStartingMonday(t time.Time) (weekStart, nextWeekStart time.Time) {
	y, m, d := t.Date()

	d0 := startOfMondayBasedWeek(time.Date(y, m, d, 0, 0, 0, 0, t.Location()))
	d1 := d0.AddDate(0, 0, daysPerWeek)

	return d0, d1
}

// TimeResolutionByMonth is a time resolution function that maps given time to a beginning and end of a month.
func TimeResolutionByMonth(t time.Time) (monthStart, nextMonthStart time.Time) {
	y, m, _ := t.Date()

	d0 := time.Date(y, m, 1, 0, 0, 0, 0, t.Location())
	d1 := d0.AddDate(0, 1, 0)

	return d0, d1
}

// TimeResolutionByYear is a time resolution function that maps given time to a beginning and end of a year.
func TimeResolutionByYear(t time.Time) (yearStart, nextYearStart time.Time) {
	y, _, _ := t.Date()

	d0 := time.Date(y, time.January, 1, 0, 0, 0, 0, t.Location())
	d1 := d0.AddDate(1, 0, 0)

	return d0, d1
}

func startOfSundayBasedWeek(t time.Time) time.Time {
	return t.AddDate(0, 0, -int(t.Weekday()))
}

func startOfMondayBasedWeek(t time.Time) time.Time {
	switch t.Weekday() {
	case time.Sunday:
		return t.AddDate(0, 0, -6)
	case time.Monday:
		return t
	default:
		return t.AddDate(0, 0, -int(t.Weekday())+1)
	}
}

// startOfQuarter returns the start of the quarter for the given time.
func startOfQuarter(t time.Time) time.Time {
	y, m, _ := t.Date()

	m = ((m-1)/monthsPerQuarter)*monthsPerQuarter + 1

	return time.Date(y, m, 1, 0, 0, 0, 0, t.Location())
}
