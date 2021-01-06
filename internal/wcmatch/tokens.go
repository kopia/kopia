package wcmatch

import (
	"fmt"
	"strings"
)

type token interface{}

// Represents a '?' in a pattern.
type tokenAnyChar struct{}

func (tokenAnyChar) String() string {
	return "?"
}

// Represents a directory separator.
type tokenDirSep struct{}

func (tokenDirSep) String() string {
	return "/"
}

// Represents a '*' or '**' in a pattern.
type tokenStar struct{ doubleStar bool }

func (tkn tokenStar) String() string {
	if tkn.doubleStar {
		return "**"
	}

	return "*"
}

// Represents a single character to match in a pattern.
type tokenRune struct{ Ch rune }

func (t tokenRune) String() string {
	return string(t.Ch)
}

// Represents a sequence in a pattern, eg. '[a-zA-z]' etc.
type tokenSeq struct {
	items   []seqToken
	negated bool
}

func (t tokenSeq) String() string {
	var b strings.Builder

	b.WriteRune('[')

	if t.negated {
		b.WriteRune('!')
	}

	for _, token := range t.items {
		b.WriteString(fmt.Sprintf("%v", token))
	}

	b.WriteRune(']')

	return b.String()
}

// Represents a token inside a tokenSeq.
type seqToken interface {
	match(ch rune) bool
}

// Represents a range in a sequence, eg. a-z or A-F.
type seqTokenRuneRange struct {
	Start, End rune
	negated    bool
}

func (t seqTokenRuneRange) match(ch rune) bool {
	return ch >= t.Start && ch <= t.End
}

func (t seqTokenRuneRange) String() string {
	return string(t.Start) + "-" + string(t.End)
}

// Represents a single character in a sequence.
type seqTokenRune struct {
	Ch rune
}

func (t seqTokenRune) match(ch rune) bool {
	return ch == t.Ch
}

func (t seqTokenRune) String() string {
	return string(t.Ch)
}

type seqTokenClass struct {
	class   string
	matcher func(ch rune) bool
}

func (t seqTokenClass) String() string {
	return "[:" + t.class + ":]"
}

func (t seqTokenClass) match(ch rune) bool {
	return t.matcher(ch)
}

// Utility functions

func isStar(token token) bool {
	_, ok := token.(tokenStar)
	return ok
}

func isDirSep(token token) bool {
	_, ok := token.(tokenDirSep)
	return ok
}
