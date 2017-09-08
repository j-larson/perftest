package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
)

func main() {
	target := flag.String("target", "http://localhost:9200/testfiles/data", "query service URL")
	num := flag.Int("num", 1, "number of requests to send")
	show := flag.Bool("show", false, "show generated contents")

	flag.Parse()

	fmt.Printf("target: %s\n", *target)
	fmt.Printf("num: %d\n", *num)
	fmt.Printf("show: %v\n", *show)

	client := &http.Client{}
	for i := 0; i < *num; i++ {
		contents := generateTestFile()
		url := fmt.Sprintf("%s/%d", *target, i)
		fmt.Printf("Creating object %s\n", url)
		if *show {
			fmt.Printf("%s\n", contents)
		}
		createObject(client, url, contents)
	}
}

func createObject(client *http.Client, url string, body string) {
	req, err := http.NewRequest("PUT", url, strings.NewReader(body))
	if err != nil {
		fmt.Printf("  Unable to create request: %v\n", err)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("  Unable to send request: %v\n", err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("  %s\n", resp.Status)
}

func generateTestFile() string {
	lines := make([]string, 0, 50)
	// Document format:
	//   - header: 5 fields, cardinality 100
	//   - data: 30 fields
	//	- 1 cardinality 2
	//	- 1 cardinality 10
	//	- 25 cardinality 100
	//	- 1 cardinality 1000
	//	- 1 cardinality 10000
	//	- 1 cardinality 100000
	// Fields are named like "field10"
	// Values are like "field5val39"

	lines = append(lines, "{ \"header\": {\n")
	for fieldNum := 0; fieldNum < 5; fieldNum++ {
		fieldVal := rand.Intn(100)
		separator := ","
		if fieldNum == 4 {
			separator = ""
		}
		lines = append(lines, fmt.Sprintf("\"field%d\" : \"field%dval%d\"%s\n", fieldNum, fieldNum, fieldVal, separator))
	}
	lines = append(lines, "}, \"data\": {\n")
	for fieldNum := 0; fieldNum < 25; fieldNum++ {
		cardinality := 100
		switch fieldNum {
		case 0:
			cardinality = 2
		case 1:
			cardinality = 10
		case 2:
			cardinality = 1000
		case 3:
			cardinality = 10000
		case 4:
			cardinality = 100000
		}
		fieldVal := rand.Intn(cardinality)
		separator := ","
		if fieldNum == 24 {
			separator = ""
		}
		lines = append(lines, fmt.Sprintf("\"field%d\" : \"field%dval%d\"%s\n", fieldNum, fieldNum, fieldVal, separator))
	}
	lines = append(lines, "}}\n")

	return strings.Join(lines, "")
}
