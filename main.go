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
var bufferSize = flag.Uint("bufferSize", 8196, "specify the buffer size to use why scanning through files")
var badFile = flag.String("badFile", "", "specify the file where bad rows should be written")

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
	in := bufio.NewReader(os.Stdin)

	for {
		line, err := in.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		check(err)

		increment("wc_mapper", "lines")
		words := strings.Split(line, "|")

		delimCount := uint(strings.Count(line, "|"))

		if delimCount > *expectedDelims {
			increment("wc_mapper", "too_many_delims")
			// write the bad row to the specifed bad file

		} else if delimCount < *expectedDelims {
			increment("wc_mapper", "split_line")

			fmt.Fprintf(os.Stderr, "num words: %d\n", len(words))

			next, _ := in.ReadString('\n')
			nextWords := strings.Split(next, "|")

			for _, word := range nextWords {
				//if !strings.Contains(word, "\n") {
				words = append(words, word)
				//}
			}

			fmt.Fprintf(os.Stderr, "num next words: %d\n", len(nextWords))
			fmt.Fprintf(os.Stderr, "now num words: %d\n", len(words))

			last, _ := in.ReadString('\n')
			lastWords := strings.Split(last, "|")

			for _, word := range lastWords {
				//if !strings.Contains(word, "\n") {
				words = append(words, word)
				//}
			}

			fmt.Fprintf(os.Stderr, "num last words: %d\n", len(lastWords))
			fmt.Fprintf(os.Stderr, "finally num words: %d\n", len(words))

			fmt.Fprintf(os.Stdout, "%s", writeFixedLine(words))
		} else {
			increment("wc_mapper", "correct")
			fmt.Fprintf(os.Stdout, "%s", writeFixedLine(words))
		}
	}
}

func unsplitLines(words []string) {

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
