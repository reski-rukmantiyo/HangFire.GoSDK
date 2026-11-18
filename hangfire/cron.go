package hangfire

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

// Schedule yields the occurrences of a recurring job.
type Schedule interface {
	// Next returns the first occurrence strictly after the given instant, or
	// false when there is none before year 2500.
	Next(after time.Time) (time.Time, bool)
}

// ParseCron parses a cron expression with the semantics Hangfire gets from
// Cronos (RecurringJobEntity.cs:285-305): five fields, or six with seconds
// first, or an @macro. Day-of-month and day-of-week must both match.
// timeZone is an IANA id or one of the common Windows ids; "" means UTC.
func ParseCron(expr, timeZone string) (Schedule, error) {
	loc, err := loadZone(timeZone)
	if err != nil {
		return nil, err
	}
	c, err := parseCronExpr(expr)
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %v", ErrInvalidCron, expr, err)
	}
	c.loc = loc
	return c, nil
}

var cronMacros = map[string]string{
	"@every_second": "* * * * * *",
	"@every_minute": "0 * * * * *",
	"@hourly":       "0 0 * * * *",
	"@daily":        "0 0 0 * * *",
	"@midnight":     "0 0 0 * * *",
	"@weekly":       "0 0 0 * * 0",
	"@monthly":      "0 0 0 1 * *",
	"@yearly":       "0 0 0 1 1 *",
	"@annually":     "0 0 0 1 1 *",
}

type domMode int

const (
	domList domMode = iota
	domLastOffset
	domLastWeekday
	domNearestWeekday
)

type dowMode int

const (
	dowList dowMode = iota
	dowLastInMonth
	dowNth
)

type cronExpr struct {
	seconds, minutes, hours uint64
	months                  uint64
	dom                     uint64
	domMode                 domMode
	domArg                  int
	dow                     uint64
	dowMode                 dowMode
	dowDay, dowNth          int
	// interval mirrors Cronos' Interval flag: such expressions also fire in
	// the repeated hour when clocks fall back.
	interval bool
	loc      *time.Location
}

type cronField struct {
	name     string
	min, max int
	names    map[string]int
}

var (
	secondField = cronField{name: "second", min: 0, max: 59}
	minuteField = cronField{name: "minute", min: 0, max: 59}
	hourField   = cronField{name: "hour", min: 0, max: 23}
	domField    = cronField{name: "day of month", min: 1, max: 31}
	monthField  = cronField{name: "month", min: 1, max: 12, names: map[string]int{
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
		"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
	}}
	dowField = cronField{name: "day of week", min: 0, max: 7, names: map[string]int{
		"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
	}}
)

func parseCronExpr(expr string) (*cronExpr, error) {
	if strings.HasPrefix(expr, "@") {
		macro, ok := cronMacros[strings.ToLower(strings.TrimRight(expr, " \t"))]
		if !ok {
			return nil, fmt.Errorf("unknown macro")
		}
		expr = macro
	}
	parts := strings.FieldsFunc(expr, func(r rune) bool { return r == ' ' || r == '\t' })
	switch len(parts) {
	case 5:
		parts = append([]string{"0"}, parts...)
	case 6:
	default:
		return nil, fmt.Errorf("%d fields, want 5 or 6", len(parts))
	}

	c := &cronExpr{}
	var err error
	for i, target := range []*uint64{&c.seconds, &c.minutes, &c.hours} {
		f := []cronField{secondField, minuteField, hourField}[i]
		if *target, err = parseList(parts[i], f); err != nil {
			return nil, err
		}
		if strings.ContainsAny(parts[i], "*?-/") {
			c.interval = true
		}
	}
	if err = c.parseDom(parts[3]); err != nil {
		return nil, err
	}
	if c.months, err = parseList(parts[4], monthField); err != nil {
		return nil, err
	}
	if err = c.parseDow(parts[5]); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *cronExpr) parseDom(s string) error {
	u := strings.ToUpper(s)
	switch {
	case u == "L":
		c.domMode = domLastOffset
	case u == "LW":
		c.domMode = domLastWeekday
	case strings.HasPrefix(u, "L-"):
		n, err := strconv.Atoi(u[2:])
		if err != nil || n < 0 || n > 30 {
			return fmt.Errorf("bad L offset %q", s)
		}
		c.domMode, c.domArg = domLastOffset, n
	case strings.HasSuffix(u, "W"):
		n, err := strconv.Atoi(u[:len(u)-1])
		if err != nil || n < 1 || n > 31 {
			return fmt.Errorf("bad W day %q", s)
		}
		c.domMode, c.domArg = domNearestWeekday, n
	default:
		var err error
		c.dom, err = parseList(s, domField)
		return err
	}
	return nil
}

func (c *cronExpr) parseDow(s string) error {
	u := strings.ToUpper(s)
	if before, after, ok := strings.Cut(u, "#"); ok {
		day, err := parseValue(before, dowField)
		if err != nil {
			return err
		}
		n, err := strconv.Atoi(after)
		if err != nil || n < 1 || n > 5 {
			return fmt.Errorf("bad # ordinal %q", s)
		}
		c.dowMode, c.dowDay, c.dowNth = dowNth, day%7, n
		return nil
	}
	if len(u) > 1 && strings.HasSuffix(u, "L") {
		day, err := parseValue(u[:len(u)-1], dowField)
		if err != nil {
			return err
		}
		c.dowMode, c.dowDay = dowLastInMonth, day%7
		return nil
	}
	set, err := parseList(s, dowField)
	if err != nil {
		return err
	}
	if set&(1<<7) != 0 {
		set = set&^(1<<7) | 1
	}
	c.dow = set
	return nil
}

func parseList(s string, f cronField) (uint64, error) {
	var set uint64
	for _, item := range strings.Split(s, ",") {
		bitsOf, err := parseItem(strings.ToUpper(item), f)
		if err != nil {
			return 0, err
		}
		set |= bitsOf
	}
	return set, nil
}

func parseItem(s string, f cronField) (uint64, error) {
	rangePart, stepPart, hasStep := strings.Cut(s, "/")
	step := 1
	if hasStep {
		n, err := strconv.Atoi(stepPart)
		if err != nil || n < 1 || n > f.max {
			return 0, fmt.Errorf("bad %s step %q", f.name, s)
		}
		step = n
	}
	lo, hi := f.min, f.max
	if rangePart != "*" && rangePart != "?" {
		from, to, isRange := strings.Cut(rangePart, "-")
		var err error
		if lo, err = parseValue(from, f); err != nil {
			return 0, err
		}
		switch {
		case isRange:
			if hi, err = parseValue(to, f); err != nil {
				return 0, err
			}
		case !hasStep:
			hi = lo
		}
	}
	span := f.max - f.min + 1
	count := hi - lo
	if count < 0 {
		count += span
	}
	var set uint64
	for i := 0; i <= count; i += step {
		v := lo + i
		if v > f.max {
			v -= span
		}
		set |= 1 << v
	}
	return set, nil
}

func parseValue(s string, f cronField) (int, error) {
	if v, ok := f.names[s]; ok {
		return v, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < f.min || n > f.max || s == "" || s[0] == '+' || s[0] == '-' {
		return 0, fmt.Errorf("bad %s value %q", f.name, s)
	}
	return n, nil
}

func daysIn(y int, m time.Month) int {
	return time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func weekday(y int, m time.Month, d int) time.Weekday {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Weekday()
}

func (c *cronExpr) dayMatches(y int, m time.Month, d int) bool {
	last := daysIn(y, m)
	var domOK bool
	switch c.domMode {
	case domList:
		domOK = c.dom&(1<<d) != 0
	case domLastOffset:
		domOK = d == last-c.domArg
	case domLastWeekday:
		domOK = d == nearestWeekday(y, m, last, last)
	case domNearestWeekday:
		domOK = c.domArg <= last && d == nearestWeekday(y, m, c.domArg, last)
	}
	if !domOK {
		return false
	}
	wd := int(weekday(y, m, d))
	switch c.dowMode {
	case dowLastInMonth:
		return wd == c.dowDay && d+7 > last
	case dowNth:
		return wd == c.dowDay && (d-1)/7+1 == c.dowNth
	}
	return c.dow&(1<<wd) != 0
}

// nearestWeekday finds the weekday closest to day n without leaving the month.
func nearestWeekday(y int, m time.Month, n, last int) int {
	switch weekday(y, m, n) {
	case time.Saturday:
		if n == 1 {
			return n + 2
		}
		return n - 1
	case time.Sunday:
		if n == last {
			return n - 2
		}
		return n + 1
	}
	return n
}

func setBits(set uint64) []int {
	out := make([]int, 0, bits.OnesCount64(set))
	for set != 0 {
		b := bits.TrailingZeros64(set)
		out = append(out, b)
		set &^= 1 << b
	}
	return out
}

const cronMaxYear = 2499

func (c *cronExpr) Next(after time.Time) (time.Time, bool) {
	la := after.In(c.loc)
	hours, minutes, seconds := setBits(c.hours), setBits(c.minutes), setBits(c.seconds)
	day := time.Date(la.Year(), la.Month(), la.Day()-1, 0, 0, 0, 0, time.UTC)
	afterDay := time.Date(la.Year(), la.Month(), la.Day(), 0, 0, 0, 0, time.UTC)

	for ; day.Year() <= cronMaxYear; day = day.AddDate(0, 0, 1) {
		if c.months&(1<<int(day.Month())) == 0 {
			day = time.Date(day.Year(), day.Month()+1, 0, 0, 0, 0, 0, time.UTC)
			continue
		}
		if !c.dayMatches(day.Year(), day.Month(), day.Day()) {
			continue
		}
		transition := c.hasTransition(day)
		if !transition && day.Before(afterDay) {
			continue
		}
		dayOffset := offsetAt(c.loc, day.Add(12*time.Hour))
		var best time.Time
		for _, h := range hours {
			for _, mi := range minutes {
				for _, s := range seconds {
					wall := day.Add(time.Duration(h)*time.Hour + time.Duration(mi)*time.Minute + time.Duration(s)*time.Second)
					if !transition {
						if t := wall.Add(-dayOffset); t.After(after) {
							return t, true
						}
						continue
					}
					if t, ok := c.resolve(wall, after); ok && (best.IsZero() || t.Before(best)) {
						best = t
					}
				}
			}
		}
		if !best.IsZero() {
			return best, true
		}
	}
	return time.Time{}, false
}

// offsetAt returns the zone offset in effect at a wall time on a day without transitions.
func offsetAt(loc *time.Location, wall time.Time) time.Duration {
	_, off := wall.In(loc).Zone()
	t := wall.Add(-time.Duration(off) * time.Second)
	_, off = t.In(loc).Zone()
	return time.Duration(off) * time.Second
}

// hasTransition reports whether the zone changes offset anywhere near the
// local day; erring towards true only costs a slower scan.
func (c *cronExpr) hasTransition(day time.Time) bool {
	start := day.Add(-14 * time.Hour)
	_, end := start.In(c.loc).ZoneBounds()
	return !end.IsZero() && end.Before(day.Add(38*time.Hour))
}

// resolve maps a wall time to the instant Cronos would fire for it, if that
// instant is after `after`. Skipped wall times fire at the transition; in a
// repeated hour the first instance fires, and the second only for interval
// expressions.
func (c *cronExpr) resolve(wall, after time.Time) (time.Time, bool) {
	var valid []time.Time
	for _, probe := range []time.Time{wall.Add(-26 * time.Hour), wall.Add(26 * time.Hour)} {
		_, off := probe.In(c.loc).Zone()
		t := wall.Add(-time.Duration(off) * time.Second)
		if sameWall(t.In(c.loc), wall) && (len(valid) == 0 || !valid[0].Equal(t)) {
			valid = append(valid, t)
		}
	}
	switch len(valid) {
	case 0:
		_, off := wall.Add(-26 * time.Hour).In(c.loc).Zone()
		start, _ := wall.Add(-time.Duration(off) * time.Second).In(c.loc).ZoneBounds()
		return start, start.After(after)
	case 2:
		first, second := valid[0], valid[1]
		if second.Before(first) {
			first, second = second, first
		}
		if first.After(after) {
			return first, true
		}
		return second, c.interval && second.After(after)
	}
	return valid[0], valid[0].After(after)
}

func sameWall(local, wall time.Time) bool {
	y, m, d := local.Date()
	h, mi, s := local.Clock()
	wy, wm, wd := wall.Date()
	wh, wmi, ws := wall.Clock()
	return y == wy && m == wm && d == wd && h == wh && mi == wmi && s == ws
}

// windowsZones maps the Windows time zone ids most likely to appear in
// RecurringOptions.TimeZone to IANA ids, so Go can compute NextExecution for
// hashes written by .NET servers on Windows. The stored id is never changed.
var windowsZones = map[string]string{
	"UTC":                            "UTC",
	"Coordinated Universal Time":     "UTC",
	"GMT Standard Time":              "Europe/London",
	"W. Europe Standard Time":        "Europe/Berlin",
	"Central Europe Standard Time":   "Europe/Budapest",
	"Romance Standard Time":          "Europe/Paris",
	"Central European Standard Time": "Europe/Warsaw",
	"E. Europe Standard Time":        "Europe/Chisinau",
	"FLE Standard Time":              "Europe/Kiev",
	"Russian Standard Time":          "Europe/Moscow",
	"Eastern Standard Time":          "America/New_York",
	"Central Standard Time":          "America/Chicago",
	"Mountain Standard Time":         "America/Denver",
	"US Mountain Standard Time":      "America/Phoenix",
	"Pacific Standard Time":          "America/Los_Angeles",
	"Alaskan Standard Time":          "America/Anchorage",
	"Hawaiian Standard Time":         "Pacific/Honolulu",
	"Atlantic Standard Time":         "America/Halifax",
	"E. South America Standard Time": "America/Sao_Paulo",
	"India Standard Time":            "Asia/Kolkata",
	"SE Asia Standard Time":          "Asia/Bangkok",
	"Singapore Standard Time":        "Asia/Singapore",
	"China Standard Time":            "Asia/Shanghai",
	"Tokyo Standard Time":            "Asia/Tokyo",
	"Korea Standard Time":            "Asia/Seoul",
	"AUS Eastern Standard Time":      "Australia/Sydney",
	"New Zealand Standard Time":      "Pacific/Auckland",
	"Arabian Standard Time":          "Asia/Dubai",
	"South Africa Standard Time":     "Africa/Johannesburg",
}

func loadZone(id string) (*time.Location, error) {
	if id == "" {
		return time.UTC, nil
	}
	if iana, ok := windowsZones[id]; ok {
		id = iana
	}
	loc, err := time.LoadLocation(id)
	if err != nil {
		return nil, fmt.Errorf("hangfire: time zone %q: %w", id, err)
	}
	return loc, nil
}
