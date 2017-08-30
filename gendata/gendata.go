package main

import (
	"fmt"
	"math/rand"
	"os"
)

func main() {
	numDocs := 1000
	filePrefix := "data/file"
	
	for i := 0; i < numDocs; i++ {
		name := fmt.Sprintf("%s%d", filePrefix, i)
		generateTestFile(name)
	}
}

func write(f *os.File, s string) {
	_, err := f.WriteString(s)
	if err != nil {
		fmt.Println("Unable to write to file %s: %v", f.Name(), err)
		os.Exit(1)
	}
}

func generateTestFile(name string) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Println("Unable to open file %s: %v", name, err)
		os.Exit(1)
	}
	defer f.Close()

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

	write(f, "{ \"header\": {\n")
	for fieldNum := 0; fieldNum < 5; fieldNum++ {
		fieldVal := rand.Intn(100)
		separator := ","
		if fieldNum == 4 {
			separator = ""
		}
		write(f, fmt.Sprintf("\"field%d\" : \"field%dval%d\"%s\n", fieldNum, fieldNum, fieldVal, separator))
	}
	write(f, "}, \"data\": {\n")
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
		write(f, fmt.Sprintf("\"field%d\" : \"field%dval%d\"%s\n", fieldNum, fieldNum, fieldVal, separator))
	}
	write(f, "}}\n")
}
