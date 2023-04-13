// Package wcmatch implements wildcard matching files using .gitignore syntax.
package wcmatch

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/pkg/errors"
)

// WildcardMatcher represents a wildcard-pattern (in .gitignore syntax) and options used to match against file paths.
type WildcardMatcher struct {
	pattern string
	tokens  []token
	dirOnly bool
	negated bool
	options Options
}

// Options defines flags that controls a WildcardMatcher.
type Options struct {
	IgnoreCase bool
	BaseDir    string
}

// Option supports the functional option pattern for NewWildcardMatcher.
type Option func(*Options)

// IgnoreCase is used to enable/disable case-insensitive operation when creating a WildcardMatcher.
func IgnoreCase(enabled bool) Option {
	return func(args *Options) {
		args.IgnoreCase = enabled
	}
}

// BaseDir is used to set the base directory to use when creating a WildcardMatcher.
// Providing a base dir here means the matcher will not match anything outside of that
// directory.
//
// Examples:
// The pattern 'a.txt' with the base dir '/my/base' is equivalent to
// the pattern '/my/base/**/a.txt' without a base dir.
//
// The pattern '/a.txt' with the base dir '/my/base/ is equivalent to
// the pattern '/my/base/a.txt' without a base dir.
func BaseDir(dir string) Option {
	return func(args *Options) {
		args.BaseDir = dir
	}
}

// Pattern returns the original pattern from which this matcher was created.
func (matcher *WildcardMatcher) Pattern() string {
	return matcher.pattern
}

// Negated indicates whether the pattern used by this matcher is a negated pattern, i.e. starts with a '!'.
func (matcher *WildcardMatcher) Negated() bool {
	return matcher.negated
}

// Options gets the options used when constructing the WildcardMatcher.
func (matcher *WildcardMatcher) Options() Options {
	return matcher.options
}

// NewWildcardMatcher creates a new WildcardMatcher with the specified pattern and options.
// The default option is for the matcher to be case-sensitive without a base dir.
//
//nolint:funlen,gocognit,gocyclo,cyclop,maintidx
func NewWildcardMatcher(pattern string, options ...Option) (matcher *WildcardMatcher, err error) {
	var result []token

	if len(pattern) > 2 && pattern[len(pattern)-2] != '\\' {
		pattern = strings.TrimSpace(pattern)
	}

	args := &Options{
		IgnoreCase: false,
		BaseDir:    "",
	}

	for _, option := range options {
		option(args)
	}

	// Ensure that BaseDir does not have a trailing directory separator if it is non-empty.
	if len(args.BaseDir) > 1 && strings.HasSuffix(args.BaseDir, "/") {
		args.BaseDir = args.BaseDir[:len(args.BaseDir)-1]
	}

	p := newRuneScanner(pattern, args.IgnoreCase)

	// Skip leading whitespace
	for unicode.IsSpace(p.peek(0)) {
		p.read()
	}

	negated := false
	if p.peek(0) == '!' {
		negated = true

		p.read()
	}

	isPatternRooted := p.peek(0) == '/'

	// Prepend the base directory to the pattern if we have one.
	if args.BaseDir != "" && pattern != "" {
		for _, ch := range args.BaseDir {
			if ch == '/' {
				result = append(result, tokenDirSep{})
			} else {
				result = append(result, tokenRune{ch})
			}
		}

		if !isPatternRooted {
			result = append(result, tokenDirSep{})
		}
	}

	// If the pattern isn't rooted, i.e. doesn't start with a '/', nor contain a '/' in the middle somewhere, then we want it to match
	// anywhere, so we add an implicit '**/' to the start of the pattern.
	firstSlashIndex := p.indexOf('/')
	if !isPatternRooted && pattern != "**" && pattern != "" {
		if firstSlashIndex == -1 || firstSlashIndex == len(p.text)-1-p.pos {
			result = append(result, tokenStar{true}, tokenDirSep{})
		} else if args.BaseDir == "" {
			// An unrooted pattern that contains a slash in the middle should be considered rooted, so
			// prepend the pattern with a '/', unless we have a baseDir, in which case we already have rooted
			// the pattern above.
			result = append(result, tokenDirSep{})
		}
	}

	dirOnly := false
	if len(p.text) > 0 && p.text[len(p.text)-1] == '/' {
		dirOnly = true
		p.text = p.text[:len(p.text)-1]
	}

	for ch := p.read(); ch != 0; ch = p.read() {
		switch ch {
		case '\\':
			if p.eos() {
				return nil, errors.Errorf("invalid pattern \"%v\": end of line found after '\\' character. Use '\\\\' to indicate a literal backslash in a pattern", pattern)
			}

			ch = p.read()
			result = append(result, tokenRune{ch})

		case '?':
			result = append(result, tokenAnyChar{})

		case '/':
			result = append(result, tokenDirSep{})

		case '*':
			if p.peek(0) == '*' {
				// We have a double star
				prevCh := p.peek(-2)

				for p.peek(0) == '*' {
					// Just consume contiguous stars
					p.read()
				}

				if prevCh == 0 || prevCh == '/' && (p.peek(0) == '/' || p.eos()) {
					result = append(result, tokenStar{doubleStar: true})
					continue
				}
			}

			result = append(result, tokenStar{doubleStar: false})

		case '[':
			ch = p.read()
			negatedSeq := ch == '!'

			if negatedSeq {
				ch = p.read()
			}

			var seq []seqToken

			for ; ch != ']' && ch != 0; ch = p.read() {
				if ch == 0 {
					return nil, errors.Errorf("invalid pattern \"%v\": end of line found, expected ']'", pattern)
				}

				if ch == '\\' {
					ch = p.read()
					if ch == 0 {
						return nil, errors.Errorf("invalid pattern \"%v\": end of line found after '\\' character. Use '\\\\' to indicate a literal backslash in a pattern", pattern)
					}
				}

				switch {
				case p.peek(0) == '-' && p.peek(1) != ']' && p.peek(1) != 0:
					// we have a range
					p.read() // consume the '-'
					endCh := p.read()

					if endCh == '\\' {
						endCh = p.read()
						if endCh == 0 {
							return nil, errors.Errorf("invalid pattern \"%v\": end of line found after '\\' character. Use '\\\\' to indicate a literal backslash in a pattern", pattern)
						}
					}

					seq = append(seq, seqTokenRuneRange{ch, endCh, negatedSeq})

				case ch == '[' && p.peek(0) == ':':
					closingBracketIndex := p.indexOf(']')
					if closingBracketIndex == -1 {
						return nil, errors.Errorf("invalid pattern \"%v\": unterminated sequence, expected ']'", pattern)
					}

					if closingBracketIndex-1 <= p.pos || p.peek(closingBracketIndex-1) != ':' {
						// treat as normal
						seq = append(seq, seqTokenRune{ch})
						continue
					}

					class := strings.ToLower(string(p.text[p.pos+1 : closingBracketIndex+p.pos-1]))
					p.pos += closingBracketIndex + 1

					var m func(ch rune) bool

					switch class {
					case "alnum":
						m = func(ch rune) bool {
							return unicode.IsLetter(ch) || unicode.IsDigit(ch)
						}
					case "alpha":
						m = unicode.IsLetter

					case "ascii":
						m = func(ch rune) bool {
							return ch >= 0 && ch <= 127
						}
					case "blank":
						m = func(ch rune) bool {
							return unicode.Is(unicode.Zs, ch) || ch == '\t'
						}
					case "cntrl":
						m = unicode.IsControl

					case "digit":
						m = unicode.IsDigit

					case "graph":
						m = unicode.IsGraphic

					case "lower":
						m = unicode.IsLower

					case "print":
						m = unicode.IsPrint

					case "punct":
						m = func(ch rune) bool {
							return unicode.IsPunct(ch) || unicode.IsSymbol(ch)
						}
					case "space":
						m = unicode.IsSpace

					case "upper":
						if args.IgnoreCase {
							m = func(ch rune) bool {
								return unicode.IsUpper(ch) || unicode.IsLower(ch)
							}
						} else {
							m = unicode.IsUpper
						}
					case "xdigit":
						m = func(ch rune) bool {
							return ch >= 'A' && ch <= 'F' ||
								ch >= 'a' && ch <= 'f' ||
								unicode.IsDigit(ch)
						}

					default:
						return nil, errors.Errorf("invalid pattern %#v: unrecognized character class [:%v:]", pattern, class)
					}

					seq = append(seq, seqTokenClass{class, m})
				default:
					seq = append(seq, seqTokenRune{ch})
				}
			}

			if ch != ']' {
				return nil, errors.Errorf("invalid pattern %#v: unterminated sequence, expected ']'", pattern)
			}

			result = append(result, tokenSeq{seq, negatedSeq})

		default:
			result = append(result, tokenRune{ch})
		}
	}

	return &WildcardMatcher{
		pattern: pattern,
		tokens:  result,
		options: *args,
		dirOnly: dirOnly,
		negated: negated,
	}, nil
}

type matchResult int

const (
	wcMatch matchResult = iota
	wcNoMatch
	wcAbortAll
	wcAbortToDoubleStar
)

// Match matches the specified text against the pattern of this WildcardMatcher. Returns true if it is a match,
// and false if it is not.  isDir should be set to true to indicate that the specified text is a directory, and
// false if it is a file.
func (matcher *WildcardMatcher) Match(text string, isDir bool) bool {
	if matcher.dirOnly && !isDir {
		return matcher.negated
	}

	return (doMatch(matcher.tokens, []rune(text), matcher.options.IgnoreCase) == wcMatch) != matcher.negated
}

//nolint:gocognit,gocyclo,cyclop
func doMatch(tokens []token, text []rune, ignoreCase bool) matchResult {
	t := runeScanner{0, text, ignoreCase}

	var tch rune
	for pi := 0; pi < len(tokens); pi, _ = pi+1, t.read() {
		tch = t.peek(0)

		if t.eos() && !isStar(tokens[pi]) {
			// We have reached the end of the text but with pattern still remaining without an '*'. This is not a match.
			return wcAbortAll
		}

		switch token := tokens[pi].(type) {
		case tokenRune:
			if tch != token.Ch {
				return wcNoMatch
			}

		case tokenDirSep:
			if tch != '/' {
				return wcNoMatch
			}
		case tokenAnyChar:
			if tch == '/' {
				return wcNoMatch
			}

			continue

		case tokenStar:
			if pi == len(tokens)-1 {
				// Trailing ** matches everything. Trailing '*' matches only if there are no more directory separators.
				if !token.doubleStar && indexOf(text[t.pos:], '/') != -1 {
					return wcNoMatch
				}

				return wcMatch
			}

			if token.doubleStar && pi+2 < len(tokens) && isDirSep(tokens[pi+1]) && doMatch(tokens[pi+2:], text[t.pos:], ignoreCase) == wcMatch {
				return wcMatch
			}

			if !token.doubleStar && isDirSep(tokens[pi+1]) {
				// One asterisk followed by a slash
				slashIndex := indexOf(text[t.pos:], '/')
				if slashIndex == -1 {
					return wcNoMatch
				}

				// Skip all characters up to the upcoming directory sep.
				t.pos += slashIndex - 1

				break
			}

			for {
				if t.eos() {
					break
				}

				matchResult := doMatch(tokens[pi+1:], text[t.pos:], ignoreCase)
				if matchResult != wcNoMatch {
					if !token.doubleStar || matchResult != wcAbortToDoubleStar {
						return matchResult
					}
				} else if !token.doubleStar && tch == '/' {
					// We are working on a single asterisk matching and encountered a '/', so return AbortToStarStar, meaning any
					// recursive calls will abort until we reach a '**' matching loop where we will then continue.
					return wcAbortToDoubleStar
				}

				tch = t.read()
			}

			return wcAbortAll

		case tokenSeq:
			match := false

			for _, r := range token.items {
				if r.match(tch) {
					match = true
					break
				}
			}

			if match == token.negated {
				return wcNoMatch
			}

		default:
			panic(fmt.Sprintf("internal error, unsupported token %T", token))
		}
	}

	if t.eos() {
		return wcMatch
	}

	return wcNoMatch
}

func indexOf(slice []rune, ch rune) int {
	for i, n := range slice {
		if n == ch {
			return i
		}
	}

	return -1
}
