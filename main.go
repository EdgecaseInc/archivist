package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

var runStage = flag.String("stage", "", "specify the stage to run.  Can be 'mapper' or 'reducer'")
var expectedDelims = flag.Uint("numDelims", 0, "specify the number of times the delimiter is expected to appear in each line")
var badFile = flag.String("badFile", "", "specify the file where bad rows should be written")

type reader struct {
	*bufio.Reader // 'reader' inherits all bufio.Reader methods
}

func (r *reader) PeekLine() (string, error) {
	var peeked []byte
	bufferSize := 1

	for ; !strings.Contains(string(peeked), "\n"); bufferSize++ {
		peeked, _ = r.Peek(bufferSize)
	}
	line, err := r.Peek(bufferSize)
	if err != nil {
		return string(line), err
	}

	return string(line), nil
}

func (r *reader) PeekLines(num int) (string, error) {
	var peeked []byte
	bufferSize := 1

	for ; strings.Count(string(peeked), "\n") < num; bufferSize++ {
		peeked, _ = r.Peek(bufferSize)
	}
	line, err := r.Peek(bufferSize - 1)
	if err != nil {
		return string(line), err
	}

	return string(line), nil
}

func main() {
	// validate & parse the flags sent into the command
	flag.Parse()
	if *expectedDelims == 0 || *runStage == "" || *badFile == "" {
		flag.PrintDefaults()
		return
	}

	switch *runStage {
	case "mapper":
		runMapper()
	case "reducer":
		runReducer()
	default:
		log.Fatalln("stage must be either 'mapper' or 'reducer'")
	}
}

func runMapper() {
	trimmed := 0
	bad := 0
	correct := 0
	lines := 0
	in := reader{bufio.NewReader(os.Stdin)}

	for {
		line, err := in.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		check(err)

		increment("wc_mapper", "lines")
		lines++
		words := strings.Split(line, "|")

		delimCount := uint(strings.Count(line, "|"))

		if delimCount > *expectedDelims {
			increment("wc_mapper", "too_many_delims")
			bad++
			// write the bad row to the specifed bad file
			fmt.Fprintf(os.Stdout, "bad\t%s", line)
		} else if delimCount < *expectedDelims {
			increment("wc_mapper", "split_line")
			trimmed++

			count := delimCount
			numLines := 1

			for ; count < *expectedDelims; numLines++ {
				peeked, err := in.PeekLines(numLines)
				check(err)
				count += uint(strings.Count(peeked, "|"))
			}

			for i := 0; i < numLines-1; i++ {
				next, _ := in.ReadString('\n')
				nextWords := strings.Split(next, "|")

				for _, word := range nextWords {
					words = append(words, word)
				}
			}

			fmt.Fprintf(os.Stdout, "split\t%s", writeFixedLine(words))
		} else {
			increment("wc_mapper", "correct")
			correct++
			fmt.Fprintf(os.Stdout, "good\t%s", writeFixedLine(words))
		}
	}

	fmt.Fprintf(os.Stderr, "Summary:\n\tLines Processed:\t%d\n\tCorrect Rows:\t\t%d\n\tSplit Rows:\t\t%d\n\tBad Rows:\t\t%d\n", lines, correct, trimmed, bad)
}

func writeFixedLine(words []string) string {
	var buf bytes.Buffer

	for i, word := range words {
		trimmedWord := strings.TrimSpace(word)

		buf.WriteString(trimmedWord)

		if i < len(words)-1 {
			if !strings.Contains(word, "\n") {
				buf.WriteString("\\|")
			}
		} else {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

func runReducer() {
	// This should do nothing
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func increment(group string, counter string) {
	fmt.Fprintf(os.Stderr, "reporter:counter:%s,%s,1\n", group, counter)
}
