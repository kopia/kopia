package wcmatch

import "unicode"

type runeScanner struct {
	pos        int
	text       []rune
	ignoreCase bool
}

func newRuneScanner(text string, ignoreCase bool) *runeScanner {
	return &runeScanner{0, []rune(text), ignoreCase}
}

//		peek returns the character at the current position plus the specified index.
//	 If ignoreCase was specified when creating the reader, any uppercase character is
//	 converted to lowercase. If outside the bounds of the text, 0 is returned.
func (s *runeScanner) peek(index int) rune {
	if s.pos+index < len(s.text) && s.pos+index >= 0 {
		ch := s.text[s.pos+index]
		if s.ignoreCase && unicode.IsUpper(ch) {
			ch = unicode.ToLower(ch)
		}

		return ch
	}

	return 0
}

// read performs the same function as peek, but also advances the position by 1
// unless we have already reached the end of the source text.
func (s *runeScanner) read() rune {
	if c := s.peek(0); c != 0 {
		s.pos++
		return c
	}

	return 0
}

// eos returns true if we have reached the end of the string.
func (s *runeScanner) eos() bool {
	return s.pos >= len(s.text)
}

// indexOf returns the index of the specified character starting at the current position. (i.e. a
// subsequent call to peek() with this index returns the given character.)
func (s *runeScanner) indexOf(ch rune) int {
	if s.eos() {
		return -1
	}

	return indexOf(s.text[s.pos:], ch)
}
