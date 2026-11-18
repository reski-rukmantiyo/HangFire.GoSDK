package hangfire

import (
	"math"
	"math/rand"
	"strconv"
	"time"
	"unicode/utf16"
)

// RetryDelay is AutomaticRetryAttribute's default back-off for the given
// 1-based attempt: round((n-1)^4 + 15 + rand(30)*n) seconds (AutomaticRetryAttribute.cs:88-93).
func RetryDelay(attempt int, rnd *rand.Rand) time.Duration {
	n := float64(attempt)
	secs := math.Round(math.Pow(n-1, 4) + 15 + float64(rnd.Intn(30))*n)
	return time.Duration(secs) * time.Second
}

// RetryReason is the State.Reason AutomaticRetryAttribute writes for a retry.
// Messages longer than 50 UTF-16 units are cut to 49 plus "…" (AutomaticRetryAttribute.cs:342-350).
func RetryReason(attempt, max int, msg string) string {
	const maxMessageLength = 50
	if units := utf16.Encode([]rune(msg)); len(units) > maxMessageLength {
		msg = string(utf16.Decode(units[:maxMessageLength-1])) + "…"
	}
	return "Retry attempt " + strconv.Itoa(attempt) + " of " + strconv.Itoa(max) + ": " + msg
}
