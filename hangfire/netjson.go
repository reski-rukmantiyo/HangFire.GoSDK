package hangfire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// quoteNet writes s as a JSON string the way Newtonsoft.Json's default
// StringEscapeHandling does: only quotes, backslashes, control characters,
// U+0085, U+2028 and U+2029 are escaped; everything else stays raw UTF-8.
func quoteNet(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\f':
			b.WriteString(`\f`)
		case '\r':
			b.WriteString(`\r`)
		case 0x85, 0x2028, 0x2029:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

// marshalNet encodes a user value the way Newtonsoft.Json with default
// settings would for the same shape: no HTML escaping, and U+0085 escaped.
func marshalNet(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	out := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	// A raw U+0085 can only occur inside a JSON string, so replacing its bytes is safe.
	out = bytes.ReplaceAll(out, []byte("\u0085"), []byte(`\u0085`))
	return string(out), nil
}

// jsonObject builds a compact JSON object with keys in the given order.
type jsonObject struct {
	b strings.Builder
}

func (o *jsonObject) raw(key, rawValue string) {
	if o.b.Len() == 0 {
		o.b.WriteByte('{')
	} else {
		o.b.WriteByte(',')
	}
	o.b.WriteString(quoteNet(key))
	o.b.WriteByte(':')
	o.b.WriteString(rawValue)
}

func (o *jsonObject) str(key, value string) { o.raw(key, quoteNet(value)) }

func (o *jsonObject) String() string {
	if o.b.Len() == 0 {
		return "{}"
	}
	return o.b.String() + "}"
}

func jsonStringArray(items []string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, s := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(quoteNet(s))
	}
	b.WriteByte(']')
	return b.String()
}

type dotNetTyped struct {
	value    any
	typeName string
}

// WithDotNetType wraps v so it marshals as a JSON object whose first member is
// "$type": typeName, which Newtonsoft needs for polymorphic arguments when the
// fleet uses TypeNameHandling (SRS DR-06). v must marshal to a JSON object.
func WithDotNetType(v any, typeName string) any {
	return dotNetTyped{value: v, typeName: typeName}
}

func (d dotNetTyped) MarshalJSON() ([]byte, error) {
	inner, err := marshalNet(d.value)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(inner, "{") {
		return nil, fmt.Errorf("hangfire: WithDotNetType needs a value that encodes as a JSON object, got %.20s", inner)
	}
	head := `{"$type":` + quoteNet(d.typeName)
	if inner == "{}" {
		return []byte(head + "}"), nil
	}
	return []byte(head + "," + inner[1:]), nil
}

// netEnumOrInt reads an enum value that Newtonsoft may have written either as
// a number or, with a StringEnumConverter, as its member name.
func netEnumOrInt(raw json.RawMessage, names map[string]int) (int, error) {
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("hangfire: enum value %s is neither a number nor a string", raw)
	}
	return parseEnumText(s, names)
}

func parseEnumText(s string, names map[string]int) (int, error) {
	if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return n, nil
	}
	total := 0
	for _, part := range strings.Split(s, ",") {
		v, ok := names[strings.ToLower(strings.TrimSpace(part))]
		if !ok {
			return 0, fmt.Errorf("hangfire: unknown enum member %q", part)
		}
		total |= v
	}
	return total, nil
}
