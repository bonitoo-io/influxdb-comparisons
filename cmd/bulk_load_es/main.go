// bulk_load_es loads an ElasticSearch daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/valyala/fasthttp"
	"strconv"
)

// Program option vars:
var (
	csvDaemonUrls      string
	daemonUrls         []string
	refreshEachBatch   bool
	workers            int
	batchSize          int
	itemLimit          int64
	indexTemplateName  string
	useGzip            bool
	doLoad             bool
	doDBCreate         bool
	numberOfReplicas   uint
	numberOfShards     uint
	telemetryHost      string
	telemetryStderr    bool
	telemetryBatchSize uint64
	telemetryTagsCSV   string
	telemetryBasicAuth string
	reportDatabase     string
	reportHost         string
	reportUser         string
	reportPassword     string
	reportTagsCSV      string
)

// Global vars
var (
	bufPool             sync.Pool
	batchChan           chan *bytes.Buffer
	inputDone           chan struct{}
	workersGroup        sync.WaitGroup
	telemetryChanPoints chan *report.Point
	telemetryChanDone   chan struct{}
	telemetryHostname   string
	telemetryTags       [][2]string
	reportTags          [][2]string
	reportHostname      string
)

// Args parsing vars
var (
	indexTemplateChoices = map[string]map[string][]byte{
		"default": {
			"5": defaultTemplate,
			"6": defaultTemplate6x,
		},
		"aggregation": {
			"5": aggregationTemplate,
			"6": aggregationTemplate6x,
		},
	}
)

var defaultTemplate = []byte(`
{
  "template": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}}
    }
  },
  "mappings": {
    "point": {
      "_all":            { "enabled": false },
      "_source":         { "enabled": true },
      "properties": {
        "timestamp":    { "type": "date", "doc_values": true }
      }
    }
  }
}
`)

var aggregationTemplate = []byte(`
{
  "template": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}}
    }
  },
  "mappings": {
    "_default_": {
      "dynamic_templates": [
        {
          "all_string_fields_can_be_used_for_filtering": {
            "match": "*",
            "match_mapping_type": "string",
            "mapping": {
              "type": "string",
              "doc_values": true,
              "index": "not_analyzed"
            }
          }
        },
        {
          "all_nonstring_fields_are_just_stored_in_column_index": {
            "match": "*",
            "match_mapping_type": "*",
            "mapping": {
              "doc_values": true,
              "index": "no"
            }
          }
        }
      ],
      "_all": { "enabled": false },
      "_source": { "enabled": false },
      "properties": {
        "timestamp": {
          "type": "date",
          "doc_values": true,
          "index": "not_analyzed"
        }
      }
    }
  }
}

`)

var defaultTemplate6x = []byte(`
{
  "index_patterns": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}}
    }
  },
  "mappings": {
    "_doc": {
      "_all":            { "enabled": false },
      "_source":         { "enabled": true },
      "properties": {
        "timestamp":    { "type": "date", "doc_values": true }
      }
    }
  }
}
`)

var aggregationTemplate6x = []byte(`
{
  "index_patterns": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}}
    }
  },
  "mappings": {
    "_doc": {
      "dynamic_templates": [
        {
          "all_string_fields_can_be_used_for_filtering": {
            "match": "*",
            "match_mapping_type": "string",
            "mapping": {
              "type": "keyword",
              "doc_values": true
            }
          }
        },
        {
          "all_nonstring_fields_are_just_stored_in_column_index": {
            "match": "*",
            "match_mapping_type": "*",
            "mapping": {
              "doc_values": true,
              "index": false
            }
          }
        }
      ],
      "_all": { "enabled": false },
      "_source": { "enabled": false },
      "properties": {
        "timestamp": {
          "type": "date",
          "doc_values": true,
          "index": true
        }
      }
    }
  }
}
`)

// Parse args:
func init() {
	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:9200", "ElasticSearch URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.BoolVar(&refreshEachBatch, "refresh", true, "Whether each batch is immediately indexed.")
	flag.Int64Var(&itemLimit, "item-limit", -1, "Number of items to read from stdin before quitting. (2 lines of input = 1 item)")

	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")

	flag.StringVar(&indexTemplateName, "index-template", "default", "ElasticSearch index template to use (choices: default, aggregation).")

	flag.BoolVar(&useGzip, "gzip", true, "Whether to gzip encode requests (default true).")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&doDBCreate, "do-db-create", true, "Whether to create the database.")

	flag.UintVar(&numberOfReplicas, "number-of-replicas", 0, "Number of ES replicas (note: replicas == replication_factor - 1). Zero replicas means RF of 1.")
	flag.UintVar(&numberOfShards, "number-of-shards", 1, "Number of ES shards. Typically you will set this to the number of nodes in the cluster.")

	flag.StringVar(&telemetryHost, "telemetry-host", "", "InfluxDB host to write telegraf telemetry to (optional).")
	flag.BoolVar(&telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&telemetryBatchSize, "telemetry-batch-size", 100, "Telemetry batch size (lines).")
	flag.StringVar(&telemetryBasicAuth, "telemetry-basic-auth", "", "basic auth (username:password) for telemetry.")
	flag.StringVar(&telemetryTagsCSV, "telemetry-tags", "", "Tag(s) for telemetry. Format: key0:val0,key1:val1,...")

	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")

	flag.Parse()

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", daemonUrls)

	if telemetryHost != "" {
		fmt.Printf("telemetry destination: %v\n", telemetryHost)
		if telemetryBatchSize == 0 {
			panic("invalid telemetryBatchSize")
		}
		var err error
		telemetryHostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
		fmt.Printf("src addr for telemetry: %v\n", telemetryHostname)

		if telemetryTagsCSV != "" {
			pairs := strings.Split(telemetryTagsCSV, ",")
			for _, pair := range pairs {
				fields := strings.SplitN(pair, ":", 2)
				tagpair := [2]string{fields[0], fields[1]}
				telemetryTags = append(telemetryTags, tagpair)
			}
		}
		fmt.Printf("telemetry tags: %v\n", telemetryTags)
	}

	if reportHost != "" {
		fmt.Printf("results report destination: %v\n", reportHost)
		fmt.Printf("results report database: %v\n", reportDatabase)

		var err error
		reportHostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
		fmt.Printf("hostname for results report: %v\n", reportHostname)

		if reportTagsCSV != "" {
			pairs := strings.Split(reportTagsCSV, ",")
			for _, pair := range pairs {
				fields := strings.SplitN(pair, ":", 2)
				tagpair := [2]string{fields[0], fields[1]}
				reportTags = append(reportTags, tagpair)
			}
		}
		fmt.Printf("results report tags: %v\n", reportTags)
	}

	if _, ok := indexTemplateChoices[indexTemplateName]; !ok {
		log.Fatalf("invalid index template type")
	}
}

func main() {
	if doLoad {
		v, err := checkServer(daemonUrls[0])
		if err != nil {
			log.Fatal(err)
		}
		if doDBCreate {
			// check that there are no pre-existing index templates:
			existingIndexTemplates, err := listIndexTemplates(daemonUrls[0])
			if err != nil {
				log.Fatal(err)
			}

			if len(existingIndexTemplates) > 0 {
				log.Println("There are index templates already in the data store. If you know what you are doing, clear them first with a command like:\ncurl -XDELETE 'http://localhost:9200/_template/*'")
			}

			// check that there are no pre-existing indices:
			existingIndices, err := listIndices(daemonUrls[0])
			if err != nil {
				log.Fatal(err)
			}

			if len(existingIndices) > 0 {
				log.Println("There are indices already in the data store. If you know what you are doing, clear them first with a command like:\ncurl -XDELETE 'http://localhost:9200/_all'")
			}

			// create the index template:
			indexTemplate := indexTemplateChoices[indexTemplateName]
			err = createESTemplate(daemonUrls[0], "measurements_template", indexTemplate[v], numberOfReplicas, numberOfShards)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	batchChan = make(chan *bytes.Buffer, workers)
	inputDone = make(chan struct{})

	if telemetryHost != "" {
		telemetryCollector := report.NewCollector(telemetryHost, "telegraf", reportUser, reportPassword)
		telemetryChanPoints, telemetryChanDone = report.TelemetryRunAsync(telemetryCollector, telemetryBatchSize, telemetryStderr, 0)
	}

	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[i%len(daemonUrls)]
		workersGroup.Add(1)
		cfg := HTTPWriterConfig{
			Host: daemonUrl,
		}
		go processBatches(NewHTTPWriter(cfg, refreshEachBatch), telemetryChanPoints, fmt.Sprintf("%d", i))
	}

	start := time.Now()
	itemsRead, bytesRead, valuesRead := scan(batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	if telemetryHost != "" {
		close(telemetryChanPoints)
		<-telemetryChanDone
	}

	fmt.Printf("loaded %d items in %fsec with %d workers (mean point rate %f items/sec, mean value rate %f/s, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, valuesRate, bytesRate/(1<<20))

	if reportHost != "" {
		//append db specific tags to custom tags
		reportTags = append(reportTags, [2]string{"replicas", strconv.Itoa(int(numberOfReplicas))})
		reportTags = append(reportTags, [2]string{"shards", strconv.Itoa(int(numberOfShards))})
		reportTags = append(reportTags, [2]string{"index-template", indexTemplateName})

		reportParams := &report.LoadReportParams{
			ReportParams: report.ReportParams{
				DBType:             "ElasticSearch",
				ReportDatabaseName: reportDatabase,
				ReportHost:         reportHost,
				ReportUser:         reportUser,
				ReportPassword:     reportPassword,
				ReportTags:         reportTags,
				Hostname:           reportHostname,
				DestinationUrl:     csvDaemonUrls,
				Workers:            workers,
				ItemLimit:          int(itemLimit),
			},
			IsGzip:    useGzip,
			BatchSize: batchSize,
		}
		err := report.ReportLoadResult(reportParams, itemsRead, valuesRate, bytesRate, took)

		if err != nil {
			log.Fatal(err)
		}
	}
}

// scan reads items from stdin. It expects input in the ElasticSearch bulk
// format: two line pairs, the first line being an 'action' and the second line
// being the payload. (2 lines = 1 item)
func scan(itemsPerBatch int) (int64, int64, int64) {
	buf := bufPool.Get().(*bytes.Buffer)

	var linesRead int64
	var err error
	var itemsRead, bytesRead int64
	var totalPoints, totalValues int64

	var itemsThisBatch int
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {

		totalPoints, totalValues, err = common.CheckTotalValues(scanner.Text())
		if totalPoints > 0 || totalValues > 0 {
			continue
		}
		if err != nil {
			log.Fatal(err)
		}

		linesRead++

		buf.Write(scanner.Bytes())
		buf.Write([]byte("\n"))

		//n++
		if linesRead%2 == 0 {
			itemsRead++
			itemsThisBatch++
		}

		hitLimit := itemLimit >= 0 && itemsRead >= itemLimit

		if itemsThisBatch == itemsPerBatch || hitLimit {
			bytesRead += int64(buf.Len())
			batchChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			itemsThisBatch = 0
		}

		if hitLimit {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if itemsThisBatch > 0 {
		batchChan <- buf
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	// The ES bulk format uses 2 lines per item:
	if linesRead%2 != 0 {
		log.Fatalf("the number of lines read was not a multiple of 2, which indicates a bad bulk format for Elastic")
	}
	if itemsRead != totalPoints { // totalPoints is unknown (0) when exiting prematurely due to time limit
		log.Fatalf("Incorrent number of read points: %d, expected: %d:", itemsRead, totalPoints)
	}

	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w *HTTPWriter, telemetrySink chan *report.Point, telemetryWorkerLabel string) {
	var batchesSeen int64
	for batch := range batchChan {
		batchesSeen++
		if !doLoad {
			continue
		}

		var err error
		var bodySize int

		// Write the batch.
		if useGzip {
			compressedBatch := bufPool.Get().(*bytes.Buffer)
			fasthttp.WriteGzip(compressedBatch, batch.Bytes())
			bodySize = len(compressedBatch.Bytes())
			_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
			// Return the compressed batch buffer to the pool.
			compressedBatch.Reset()
			bufPool.Put(compressedBatch)
		} else {
			bodySize = len(batch.Bytes())
			_, err = w.WriteLineProtocol(batch.Bytes(), false)
		}

		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)

		// Report telemetry, if applicable:
		if telemetrySink != nil {
			p := report.GetPointFromGlobalPool()
			p.Init("benchmark_write", time.Now().UnixNano())
			p.AddTag("src_addr", telemetryHostname)
			p.AddTag("dst_addr", w.c.Host)
			p.AddTag("worker_id", telemetryWorkerLabel)
			p.AddInt64Field("worker_req_num", batchesSeen)
			p.AddBoolField("gzip", useGzip)
			p.AddInt64Field("body_bytes", int64(bodySize))
			telemetrySink <- p
		}
	}
	workersGroup.Done()
}

// createESTemplate uses a Go text/template to create an ElasticSearch index
// template. (This terminological conflict is mostly unavoidable).
func createESTemplate(daemonUrl, indexTemplateName string, indexTemplateBodyTemplate []byte, numberOfReplicas, numberOfShards uint) error {
	// set up URL:
	u, err := url.Parse(daemonUrl)
	if err != nil {
		return err
	}
	u.Path = fmt.Sprintf("_template/%s", indexTemplateName)

	// parse and execute the text/template:
	t := template.Must(template.New("index_template").Parse(string(indexTemplateBodyTemplate)))
	var body bytes.Buffer
	params := struct {
		NumberOfReplicas uint
		NumberOfShards   uint
	}{
		NumberOfReplicas: numberOfReplicas,
		NumberOfShards:   numberOfShards,
	}
	err = t.Execute(&body, params)
	if err != nil {
		return err
	}

	// do the HTTP PUT request with the body data:
	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(body.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("bad mapping create: %s", body)
	}
	return nil
}

//func createDb(daemon_url, dbname string) error {
//	u, err := url.Parse(daemon_url)
//	if err != nil {
//		return err
//	}
//
//	// serialize params the right way:
//	u.Path = "query"
//	v := u.Query()
//	v.Set("q", fmt.Sprintf("CREATE DATABASE %s", dbname))
//	u.RawQuery = v.Encode()
//
//	req, err := http.NewRequest("GET", u.String(), nil)
//	if err != nil {
//		return err
//	}
//
//	client := &http.Client{}
//	resp, err := client.Do(req)
//	if err != nil {
//		return err
//	}
//	defer resp.Body.Close()
//	// does the body need to be read into the void?
//
//	if resp.StatusCode != 200 {
//		return fmt.Errorf("bad db create")
//	}
//	return nil
//}

// listIndexTemplates lists the existing index templates in ElasticSearch.
func listIndexTemplates(daemonUrl string) (map[string]interface{}, error) {
	u := fmt.Sprintf("%s/_template", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var listing map[string]interface{}
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	return listing, nil
}

// listIndices lists the existing indices in ElasticSearch.
func listIndices(daemonUrl string) (map[string]interface{}, error) {
	u := fmt.Sprintf("%s/*", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var listing map[string]interface{}
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	return listing, nil
}

// checkServer pings  ElasticSearch and returns major version string
func checkServer(daemonUrl string) (string, error) {
	majorVer := "5"
	resp, err := http.Get(daemonUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var listing map[string]interface{}
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return "", err
	}
	if v, ok := listing["version"]; ok {
		vo := v.(map[string]interface{})
		if ver, ok := vo["number"]; ok {
			fmt.Printf("Elastic Search version %s\n", ver)
			nums := strings.Split(ver.(string), ".")
			if len(nums) > 0 {
				majorVer = nums[0]
			}
		}
	}

	return majorVer, nil
}
