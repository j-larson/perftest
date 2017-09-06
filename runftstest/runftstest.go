package main

import (
	"fmt"
	"net/http"
	"io/ioutil"
	"flag"
	"strings"
	_ "math/rand"
	"time"
)

func buildQuery(numPredicates int, equality bool) string {
	template := `{"size":100,"query":{"must":{"conjuncts":[%s]}}}`
	predicatesList := make([]string, numPredicates)
	/*
	for i := 0; i < numPredicates; i++ {
		fieldNum := rand.Intn(20) + 5
		fieldVal := rand.Intn(100)
		symbol := "="
		predicate := fmt.Sprintf("+data.field%d: field%dval%v", fieldNum, symbol, fieldNum, fieldVal)
		predicatesList[i] = predicate
	}
	*/
	if equality {
		predicatesList[0] = `{"term":"field19val69", "field":"data.field19"}`
		predicatesList[1] = `{"term":"field0val0", "field":"data.field0"}`
	} else {
		predicatesList[0] = `{"min":"field19val98", "field":"data.field19"}`
		predicatesList[1] = `{"min":"field0val1", "field":"data.field0"}`
	}

	joinedPredicates := strings.Join(predicatesList, ",")
	return fmt.Sprintf(template, joinedPredicates)
}

func main() {
	target := flag.String("target", "http://localhost:8094/api/index/smalltestdataFTSidx/query", "query service URL")
	num := flag.Int("num", 1, "number of requests to send")
	predicates := flag.Int("predicates", 2, "number of predicates per request")
	bucket := flag.String("bucket", "default", "bucket to query")
	equality := flag.Bool("equality", true, "whether to use = predicates rather than >")
	showQueries := flag.Bool("showQueries", false, "show queries")
	sendQueries := flag.Bool("sendQueries", true, "send queries")
	showResults := flag.Bool("showResults", false, "show results")

	flag.Parse()

	fmt.Printf("target: %s\n", *target)
	fmt.Printf("num: %d\n", *num)
	fmt.Printf("predicates: %d\n", *predicates)
	fmt.Printf("bucket: %s\n", *bucket)
	fmt.Printf("equality: %v\n", *equality)
	fmt.Printf("showQueries: %v\n", *showQueries)
	fmt.Printf("sendQueries: %v\n", *sendQueries)
	fmt.Printf("showResults: %v\n", *showResults)

	client := &http.Client{}
	totalElapsed := time.Duration(0)
	totalErrors := 0
	for i := 0; i < *num; i++ {
		query := buildQuery(*predicates, *equality)
		if *showQueries {
			fmt.Printf("Query %d: %s\n", i, query)
		}
		req, err := http.NewRequest("POST", *target, strings.NewReader(query))
		if err != nil {
			fmt.Printf("Unable to create request: %v", err)
			totalErrors++
			continue
		}
		req.Header.Add("Content-Type", "application/json")
		req.SetBasicAuth("Administrator", "password")
		
		startTime := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Request failed: %v\n", err)
			totalErrors++
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Printf("Request failed with code %d", resp.StatusCode)
			totalErrors++
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("Unable to read response body: %v", err)
			totalErrors++
			continue
		}
		totalElapsed = totalElapsed + time.Since(startTime)
		if *showResults {
			fmt.Printf("%s\n", body)
		}
	}
	
	fmt.Printf("Total elapsed (ms): %v\n", totalElapsed.Seconds()*1000.0)
	fmt.Printf("Total errors: %v\n", totalErrors)

}
