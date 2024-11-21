package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

var (
	requests         int64
	period           int64
	clients          int
	url              string
	urlsFilePath     string
	keepAlive        bool
	postDataFilePath string
	writeTimeout     int
	readTimeout      int
	Authorization    string
	geolocation      string
	contentType      string
	apiUserName      string
	responseFileDir  string
	method           string
	dynamicHeaders   string
)

type ResponseData struct {
	RequestNumber int64  `json:"requestNumber"`
	StatusCode    int    `json:"statusCode"`
	ResponseData  []byte `json:"responseData"`
}

type Configuration struct {
	urls            []string
	method          string
	postData        []byte
	requests        int64
	period          int64
	keepAlive       bool
	Authorization   string
	geolocation     string
	contentType     string
	apiUserName     string
	responseFileDir string
	headers         map[string]string
	myClient        fasthttp.Client
	responseFile    *os.File
}

type Result struct {
	Requests      int64
	Success       int64
	NetworkFailed int64
	BadFailed     int64
}

var readThroughput int64
var writeThroughput int64

type MyConn struct {
	net.Conn
}

func (c *MyConn) Read(b []byte) (n int, err error) {
	len, err := c.Conn.Read(b)
	if err == nil {
		atomic.AddInt64(&readThroughput, int64(len))
	}
	return len, err
}

func (c *MyConn) Write(b []byte) (n int, err error) {
	len, err := c.Conn.Write(b)
	if err == nil {
		atomic.AddInt64(&writeThroughput, int64(len))
	}
	return len, err
}

func init() {
	flag.Int64Var(&requests, "r", -1, "Number of requests per client")
	flag.IntVar(&clients, "c", 100, "Number of concurrent clients")
	flag.StringVar(&url, "u", "", "URL")
	flag.StringVar(&urlsFilePath, "f", "", "URL's file path (line separated)")
	flag.BoolVar(&keepAlive, "k", true, "Do HTTP keep-alive")
	flag.StringVar(&postDataFilePath, "d", "", "HTTP POST data file path")
	flag.Int64Var(&period, "t", -1, "Period of time (in seconds)")
	flag.IntVar(&writeTimeout, "tw", 5000, "Write timeout (in milliseconds)")
	flag.IntVar(&readTimeout, "tr", 5000, "Read timeout (in milliseconds)")
	flag.StringVar(&Authorization, "auth", "", "Authorization header")
	flag.StringVar(&geolocation, "gl", "", "Geo Location Header")
	flag.StringVar(&contentType, "ct", "", "Content type")
	flag.StringVar(&apiUserName, "user", "", "API User Name")
	flag.StringVar(&responseFileDir, "rsp", "", "Directory path to store response json files")
	flag.StringVar(&method, "m", "GET", "HTTP method (GET, POST, PUT)")
	flag.StringVar(&dynamicHeaders, "headers", "", "Dynamic headers (key:value;key:value)")
}

func printResults(results map[int]*Result, startTime time.Time) {
	var requests, success, networkFailed, badFailed int64
	for _, result := range results {
		requests += result.Requests
		success += result.Success
		networkFailed += result.NetworkFailed
		badFailed += result.BadFailed
	}
	elapsed := int64(time.Since(startTime).Seconds())
	if elapsed == 0 {
		elapsed = 1
	}
	fmt.Println()
	fmt.Printf("Requests:                       %10d hits\n", requests)
	fmt.Printf("Successful requests:            %10d hits\n", success)
	fmt.Printf("Network failed:                 %10d hits\n", networkFailed)
	fmt.Printf("Bad requests failed (!2xx):     %10d hits\n", badFailed)
	fmt.Printf("Successful requests rate:       %10d hits/sec\n", success/elapsed)
	fmt.Printf("Read throughput:                %10d bytes/sec\n", readThroughput/elapsed)
	fmt.Printf("Write throughput:               %10d bytes/sec\n", writeThroughput/elapsed)
	fmt.Printf("Test time:                      %10d sec\n", elapsed)
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	reader := bufio.NewReader(file)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(line))
	}
	return lines, nil
}

func NewConfiguration() *Configuration {
	if urlsFilePath == "" && url == "" {
		flag.Usage()
		os.Exit(1)
	}
	if requests == -1 && period == -1 {
		fmt.Println("Requests or period must be provided")
		flag.Usage()
		os.Exit(1)
	}
	if requests != -1 && period != -1 {
		fmt.Println("Only one should be provided: [requests|period]")
		flag.Usage()
		os.Exit(1)
	}

	configuration := &Configuration{
		urls:            make([]string, 0),
		method:          method,
		postData:        nil,
		keepAlive:       keepAlive,
		requests:        requests,
		Authorization:   Authorization,
		geolocation:     geolocation,
		contentType:     contentType,
		apiUserName:     apiUserName,
		responseFileDir: responseFileDir,
		headers:         make(map[string]string),
	}

	if dynamicHeaders != "" {
		for _, header := range strings.Split(dynamicHeaders, ";") {
			parts := strings.SplitN(header, ":", 2)
			if len(parts) == 2 {
				configuration.headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			} else {
				log.Fatalf("Invalid header format: %s", header)
			}
		}
	}

	if urlsFilePath != "" {
		lines, err := readLines(urlsFilePath)
		if err != nil {
			log.Fatalf("Error reading URLs from file: %v", err)
		}
		configuration.urls = lines
	}

	if url != "" {
		configuration.urls = append(configuration.urls, url)
	}

	if postDataFilePath != "" {
		data, err := ioutil.ReadFile(postDataFilePath)
		if err != nil {
			log.Fatalf("Error reading POST data file: %v", err)
		}
		configuration.postData = data
	}

	if responseFileDir != "" {
		file, err := os.OpenFile(filepath.Join(responseFileDir, "responses.json"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Error opening response file: %v", err)
		}
		configuration.responseFile = file
	}

	configuration.myClient.ReadTimeout = time.Duration(readTimeout) * time.Millisecond
	configuration.myClient.WriteTimeout = time.Duration(writeTimeout) * time.Millisecond
	configuration.myClient.MaxConnsPerHost = clients
	configuration.myClient.Dial = func(addr string) (net.Conn, error) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		return &MyConn{Conn: conn}, nil
	}

	return configuration
}

func client(configuration *Configuration, result *Result, done *sync.WaitGroup) {
	for result.Requests < configuration.requests {
		for _, tmpUrl := range configuration.urls {
			req := fasthttp.AcquireRequest()
			req.SetRequestURI(tmpUrl)
			req.Header.SetMethod(configuration.method)

			if configuration.keepAlive {
				req.Header.Set("Connection", "keep-alive")
			} else {
				req.Header.Set("Connection", "close")
			}

			for key, value := range configuration.headers {
				req.Header.Set(key, value)
			}

			req.SetBody(configuration.postData)
			resp := fasthttp.AcquireResponse()
			err := configuration.myClient.Do(req, resp)
			statusCode := resp.StatusCode()
			result.Requests++

			if err != nil || statusCode < 200 || statusCode >= 300 {
				if err != nil {
					result.NetworkFailed++
				} else {
					result.BadFailed++
				}
				if configuration.responseFile != nil {
					data := ResponseData{
						RequestNumber: result.Requests,
						StatusCode:    statusCode,
						ResponseData:  resp.Body(),
					}
					jsonData, _ := json.Marshal(data)
					configuration.responseFile.WriteString(string(jsonData) + "\n")
				}
				continue
			}
			result.Success++
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
		}
	}
	done.Done()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	startTime := time.Now()
	results := make(map[int]*Result)
	done := &sync.WaitGroup{}
	configuration := NewConfiguration()
	done.Add(clients)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			printResults(results, startTime)
			os.Exit(0)
		}
	}()

	for i := 0; i < clients; i++ {
		result := &Result{}
		results[i] = result
		go client(configuration, result, done)
	}
	done.Wait()
	printResults(results, startTime)
}
