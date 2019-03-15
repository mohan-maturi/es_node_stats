package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/golang/glog"
	"github.com/hokaccha/go-prettyjson"
	gjson "github.com/tidwall/gjson"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var fVar string
var optionVar string
var nodeVar string
var metricVar string
var aggVar string
var timeFilter string

// Map of [nodeName][timeStamp][metricName]value
var statsMap map[string]interface{}
var nodeFilter []string
var metricFilter []string
var aggs []string
var startTime int64
var endTime int64
var nodeNames map[string]string

func init() {
	flag.StringVar(&fVar, "f", "/tmp/ingest_stats",
		"file to read data from")
	flag.StringVar(&optionVar, "option", "pretty2",
		"option: pretty, nodelist, metricname, metricvalue")
	flag.StringVar(&nodeVar, "nodes", "",
		"limit results to only these comma seperated list of nodes")
	flag.StringVar(&metricVar, "metric", "",
		"limit results to only these comma seperated list of metrics")
	flag.StringVar(&aggVar, "a", "",
		"agg: diff, sum")
	flag.StringVar(&timeFilter, "timeFilter", "",
		"start_time and end_time seperated by comma")

	flag.Parse()

	statsMap = make(map[string]interface{})
	nodeNames = make(map[string]string)

	if nodeVar != "" {
		nodeFilter = strings.Split(nodeVar, ",")
		for i := range nodeFilter {
			log.Infof("Filering node %s\n", nodeFilter[i])
		}
	}

	if metricVar != "" {
		metricFilter = strings.Split(metricVar, ",")
		for i := range metricFilter {
			log.Infof("Filtering metric %s\n", metricFilter[i])
		}
	}

	if aggVar != "" {
		aggs = strings.Split(aggVar, ",")
		for i := range aggs {
			log.Infof("Aggregate %s\n", aggs[i])
		}
	}

	if timeFilter != "" {
		times := strings.Split(timeFilter, ",")
		startTime, _ = strconv.ParseInt(times[0], 10, 64)
		endTime, _ = strconv.ParseInt(times[1], 10, 64)
		log.Infof("startTime %d endTime %d\n", startTime, endTime)
	}
}

type input struct {
	ts      string
	nname   string
	mfilter string
}

func main() {
	// Option
	option := optionVar

	// Open our jsonFile
	file, err := os.Open(fVar)
	// if we os.Open returns an error then handle it
	if err != nil {
		log.Fatal(err)
	}
	log.Infoln("Successfully Opened ", fVar)
	// defer the closing of our jsonFile so that we can parse it later on
	defer file.Close()

	// Map of node and first seen timestamp
	nodeMap := make(map[string]string)

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 50*1024*1024)

	i := 0
	timeStr := ""
	gotNodeNames := false
	for scanner.Scan() {
		if i > 2 {
			i = i + 1
			break
		}
		str := scanner.Text()
		t, err := time.Parse(time.UnixDate, str)
		if err == nil {
			timeStr = t.String()
		} else {
			var result map[string]interface{}
			json.Unmarshal([]byte(str), &result)
			if option == "pretty" {
				fmt.Println(timeStr)
				s, _ := prettyjson.Marshal(result)
				fmt.Println(string(s))
			} else if option == "nodelist" {
				nodes := result["nodes"].(map[string]interface{})
				for nname, _ := range nodes {
					if _, ok := nodeMap[nname]; !ok {
						nodeMap[nname] = timeStr
					}
				}
			} else if option == "metrics" {
				/*
					nodes := result["nodes"].(map[string]interface{})
					for nname, nvalue := range nodes {
						nname := "PDnj0rrbRVKwe2gM6Z_aqQ"
						key := "nodes." + nname + "." + keyVar
						fmt.Println(key)
						byteArray, err := json.Marshal(nvalue)
						if err == nil {
							len := len(byteArray)
							s := string(byteArray[:len])
							value := gjson.Get(s, key)
							fmt.Println(timeStr + " " + key + " " + value.String())
						}
					}
				*/
				nname := "PDnj0rrbRVKwe2gM6Z_aqQ"
				key := "nodes." + nname + "." + metricVar
				value := gjson.Get(str, key)
				fmt.Println(timeStr + " " + key + " " + value.String())

			} else if option == "metricvalue" || option == "metriclist" {

				nodes := result["nodes"].(map[string]interface{})
				for nname, v := range nodes {
					if skip(nodeFilter, nname) {
						continue
					}
					if gotNodeNames == false {
						nkey := "nodes." + nname + "." + "name"
						nvalue := gjson.Get(str, nkey)
						nodeNames[nname] = nvalue.String()
					}

					log.V(1).Infof("%s, %s\n", timeStr, nname)
					i := input{ts: timeStr, nname: nname, mfilter: metricVar}
					parse(v, i, "")
				}
				gotNodeNames = true
			}
		}
	}

	// print the output
	if option == "nodelist" {
		i := 0
		for k, _ := range nodeMap {
			fmt.Print(k, "\t")
			i = i + 1
			if (i % 5) == 0 {
				fmt.Println("\n")
			}
		}
		fmt.Println("\n")
	} else if option == "metricvalue" {
		dataMap := statsMap
		// If number of nodes is more than 1, then dump graph for each metric across nodes
		if len(dataMap) > 1 {
			dataMap = transpose(dataMap)
		}
		createTable(dataMap)
	} else if option == "metriclist" {
		for _, oneval := range statsMap {
			for _, twoval := range oneval.(map[string]interface{}) {
				for three, threeval := range twoval.(map[string]interface{}) {
					threeint, err := strconv.ParseInt(threeval.(string), 10, 64)
					if err != nil && threeint == 0 {
						continue
					}
					fmt.Println(three)
				}
				break
			}
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func createTable(v map[string]interface{}) {

	rowNameMap := make(map[string]bool)
	columnNameMap := make(map[string]bool)

	numTables := len(v)
	// Find number of rows - timestamp
	// Find number of columns - can be node or metric name column
	for _, oneval := range v {
		for two, twoval := range oneval.(map[string]interface{}) {
			rowNameMap[two] = true
			for three, _ := range twoval.(map[string]interface{}) {
				columnNameMap[three] = true
			}
		}
	}

	rows := []string{}
	for row := range rowNameMap {
		rows = append(rows, row)
	}
	sort.Strings(rows)
	numRows := len(rows)

	columns := []string{}
	for column := range columnNameMap {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	numColumns := len(columns)

	log.Infof("%d tables, %d rows, %d columns\n", numTables, numRows, numColumns)
	for i, row := range rows {
		log.V(1).Infof("%d: %s ", i, row)
	}
	log.V(1).Infof("\n")
	for i, column := range columns {
		log.V(1).Infof("%d: %s ", i, column)
	}
	log.V(1).Infof("\n")

	for one, _ := range v {
		fmt.Printf("# %s\n", one)
		oneval := v[one].(map[string]interface{})

		// Create a table
		table := make([][]interface{}, numRows)
		for r := range table {
			table[r] = make([]interface{}, numColumns+1)
		}
		// Populate rows and columns
		for r, rname := range rows {
			// 0th column is row identifier - timestamp
			var yr, mn, day, hr, min, sec int
			fmt.Sscanf(rname, "%04d-%02d-%02d %02d:%02d:%02d", &yr, &mn, &day, &hr, &min, &sec)
			gotime := fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d-08:00", yr, mn, day, hr, min, sec)
			ptime, _ := time.Parse(time.RFC3339, gotime)
			table[r][0] = strconv.FormatInt(ptime.Local().Unix(), 10)

			twoval, ok := oneval[rname]
			if ok {
				twovalMap := twoval.(map[string]interface{})
				// columns 1 to n+1
				for c, cname := range columns {
					table[r][c+1] = ""
					threeval, ok := twovalMap[cname]
					if ok {
						table[r][c+1] = threeval
					}
				}
			}
		}

		// print the table
		rowHeader := "Timestamp"
		for _, cname := range columns {
			if nname, ok := nodeNames[cname]; ok {
				cname = nname
			}
			rowHeader += ", " + cname
		}
		fmt.Println(rowHeader)
		for r := range table {
			if inlist(aggs, "diff") && r == 0 {
				continue
			}

			dataTime, _ := strconv.ParseInt(table[r][0].(string), 10, 64)
			if dataTime < startTime || dataTime > endTime {
				log.Infof("skipping %d, %d, %d\n", startTime, dataTime, endTime)
				continue
			}
			sum := int64(0)
			for c := range table[r] {
				// First column is always time, simply print it
				if c == 0 {
					fmt.Printf("%s,", table[r][c])
					continue
				}

				// coumn 1 to n+1 are metrics
				value, _ := strconv.ParseInt(table[r][c].(string), 10, 64)
				if inlist(aggs, "diff") {
					prevalue, _ := strconv.ParseInt(table[r-1][c].(string), 10, 64)
					value -= prevalue
				}
				sum += value
				// if no aggregate list or only asked to print sum
				if !inlist(aggs, "only_sum") {
					if value > 0 {
						fmt.Printf("%d,", value)
					} else {
						fmt.Printf(",")
					}
				}
			}
			if inlist(aggs, "also_sum") || inlist(aggs, "only_sum") {
				fmt.Printf("%d,", sum)
			}
			fmt.Printf("\n")
		}
	}
}

// change from [nodeName][timeStamp][metricName]value
// to [metricName][timeStamp][nodeName]value
func transpose(v map[string]interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	for one, oneval := range v {
		for two, twoval := range oneval.(map[string]interface{}) {
			for three, threeval := range twoval.(map[string]interface{}) {
				addValue(ret, three, two, one, threeval.(string))
			}
		}
	}
	return ret
}

func inlist(list []string, elem string) bool {
	if len(list) == 0 {
		return false
	}

	for i := range list {
		if list[i] == elem {
			return true
		}
	}
	return false
}

func skip(list []string, elem string) bool {
	if len(list) == 0 {
		return false
	}

	for i := range list {
		if list[i] == elem {
			return false
		}
	}
	return true
}

func addValue(m map[string]interface{}, one string, two string, three string, value string) {
	if _, ok := m[one]; !ok {
		m[one] = make(map[string]interface{})
	}
	twoMap := m[one].(map[string]interface{})
	if _, ok := twoMap[two]; !ok {
		twoMap[two] = make(map[string]interface{})
	}
	threeMap := twoMap[two].(map[string]interface{})
	if _, ok := threeMap[three]; !ok {
		threeMap[three] = value
	}
	log.V(2).Infof("%s, %s, %s, %s\n", one, two, three, value)
}

func addValueToMap(i input, mname string, value string) {
	if skip(metricFilter, mname) {
		return
	}

	addValue(statsMap, i.nname, i.ts, mname, value)
}

func parse(v interface{}, i input, mname string) {
	switch val := v.(type) {
	case string:
		addValueToMap(i, mname, val)
	case float64:
		addValueToMap(i, mname, strconv.FormatFloat(val, 'f', -1, 64))
	case bool:
		addValueToMap(i, mname, strconv.FormatBool(val))
	case nil:
	case map[string]interface{}:
		processMap(val, i, mname)
	case []interface{}:
		processArray(val, i, mname)
	}
}

func processMap(m map[string]interface{}, i input, mname string) {
	if len(m) == 0 {
		return
	}

	keys := []string{}
	for key := range m {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	for _, key := range keys {
		val := m[key]
		dottedKey := mname
		if mname != "" {
			dottedKey += "."
		}
		dottedKey += key
		log.V(2).Infof("%s %s %s\n", i.ts, i.nname, dottedKey)
		parse(val, i, dottedKey)
	}
}

func processArray(m []interface{}, i input, mname string) {
	if len(m) == 0 {
		return
	}
}
