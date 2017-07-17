package cli

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	genPasswordCommand          = app.Command("genpasswd", "Generate memorable password - inspired by http://xkcd.com/936/")
	genPasswordCount            = genPasswordCommand.Flag("num-passwords", "Number of passwords to generate.").Short('n').Default("20").Int()
	genPasswordWordsPerPassword = genPasswordCommand.Flag("words-per-password", "Number of words per password.").Short('w').Default("6").Int()

	genPasswordWordsSource   = genPasswordCommand.Flag("word-list", "Path or URL to the word list.").Default("http://kopia.github.io/words/en.txt").String()
	genPasswordWordSeparator = genPasswordCommand.Flag("words-separator", "Word separator.").Default("-").Short('s').String()
	genPasswordUppercase     = genPasswordCommand.Flag("uppercase", "Use upper-case versions of words.").Short('u').Bool()
	genPasswordCapitalize    = genPasswordCommand.Flag("capitalize", "Use capitalized versions of words.").Short('c').Bool()
	genPasswordL33t          = genPasswordCommand.Flag("l33t", "Use common substitutions o->0, s->$, etc.").Short('l').Bool()
	genPasswordMinWordLength = genPasswordCommand.Flag("min-word-length", "Minimum dictionary word length.").Default("4").Int()
	genPasswordMaxWordLength = genPasswordCommand.Flag("max-word-length", "Maximum dictionary word length.").Default("8").Int()

	l33tSubstTable = map[string]string{
		"a": "4",
		"e": "3",
		"l": "1",
		"s": "$",
		"o": "0",
	}
)

func init() {
	genPasswordCommand.Action(runGenPassword)
}

func genleet(result *[]string, prefix string, suffix string) {
	if len(suffix) == 0 {
		return
	}

	*result = append(*result, prefix+suffix)

	ch := suffix[0:1]
	genleet(result, prefix+ch, suffix[1:])
	if subst, ok := l33tSubstTable[ch]; ok {
		genleet(result, prefix+subst, suffix[1:])
	}
}

func l33t(w string) []string {
	var result []string

	genleet(&result, "", w)

	return result
}

func getWordList() ([]string, error) {
	var allWords []byte
	var err error

	// Read the words file, that is typically 2-5MB, not a big deal.
	if strings.HasPrefix(*genPasswordWordsSource, "http") {
		// File not found, try URL.
		fmt.Printf("Downloading word list from %v ...\n", *genPasswordWordsSource)
		resp, err := http.Get(*genPasswordWordsSource)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		allWords, err = ioutil.ReadAll(resp.Body)
	} else {
		fmt.Printf("Using word list from file: %v\n", *genPasswordWordsSource)
		allWords, err = ioutil.ReadFile(*genPasswordWordsSource)
	}

	if err != nil {
		return nil, err
	}

	words := bytes.Split(allWords, []byte("\n"))
	var usableWords []string

	for _, w := range words {
		word := strings.TrimSpace(string(w))
		if len(word) >= *genPasswordMinWordLength && len(word) <= *genPasswordMaxWordLength {
			usableWords = append(usableWords, word)
			if *genPasswordUppercase {
				usableWords = append(usableWords, strings.ToUpper(word))
			}
			if *genPasswordCapitalize {
				usableWords = append(usableWords, strings.ToUpper(word[0:1])+word[1:])
			}
			if *genPasswordL33t {
				usableWords = append(usableWords, l33t(word)...)
			}
		}
	}

	if len(usableWords) < 500 {
		return nil, fmt.Errorf("word list too short: %v entries", len(usableWords))
	}
	fmt.Printf("Got %v usable words.\n", len(usableWords))
	fmt.Printf("\n")

	return usableWords, nil
}

func runGenPassword(context *kingpin.ParseContext) error {
	if *genPasswordWordsPerPassword < 1 || *genPasswordWordsPerPassword > 8 {
		return fmt.Errorf("--words-per-password must be between 1 and 8")
	}
	usableWords, err := getWordList()
	if err != nil {
		return fmt.Errorf("unable to read word list: %v", err)
	}

	randomBitsPerWord := math.Log2(float64(len(usableWords)))
	randomBitsPerSeparator := math.Log2(float64(len(*genPasswordWordSeparator)))
	wordCharSetSize := 26
	if *genPasswordUppercase || *genPasswordCapitalize {
		wordCharSetSize *= 2
	}

	fmt.Printf("Memorable passwords, inspired by http://xkcd.com/936/ :\n")
	fmt.Printf("\n")

	for i := 0; i < *genPasswordCount; i++ {
		pass := generatePasswordFromWords(usableWords)

		fmt.Printf("%2d. %-60v blind entropy %.2f bits\n", i+1, pass, math.Log2(math.Pow(float64(wordCharSetSize), float64(len(pass)))))
	}

	fmt.Printf("\n")
	fmt.Printf("Password entropy with full knowledge of the algorithm: %.2f bits.\n",
		randomBitsPerWord*float64(*genPasswordWordsPerPassword)+
			randomBitsPerSeparator*float64(len(*genPasswordWordSeparator)))

	return nil
}

func secureRandomInt() int {
	var b [4]byte
	io.ReadFull(rand.Reader, b[:])
	return ((int(b[0]) & 0x7F) << 24) | (int(b[1]) << 16) | (int(b[2]) << 8) | int(b[3])
}

func generatePasswordFromWords(words []string) string {
	var result string
	separators := *genPasswordWordSeparator

	for i := 0; i < *genPasswordWordsPerPassword; i++ {
		if i > 0 {
			x := secureRandomInt() % len(separators)
			result += separators[x : x+1]
		}
		result += words[secureRandomInt()%len(words)]
	}

	return result
}
