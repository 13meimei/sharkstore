// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command hey is an HTTP load generator.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"

	"tool/client-ds/requester"
)

var (
	c = flag.Int("c", 50, "")
	n = flag.Int("n", 200, "")
	q = flag.Int("q", 0, "")
	output = flag.String("o", "", "")
	host = flag.String("h", "", "")
	mode = flag.Int("m", 5, "")
	times = flag.Int("times", 1, "")
	cpus = flag.Int("cpus", runtime.GOMAXPROCS(-1), "")
	help = flag.Bool("help", false, "")
)

const notice = `
************************** WARNING ********************************
This project has moved to https://github.com/rakyll/hey

Use the following command to install the new binary:
$ go get github.com/rakyll/hey

Program boom might be broken in the long future, please update your
environment rather than depending on this deprecated binary.
*******************************************************************
`

var usage = `Usage: go run hey [options...] <url>

Options:
  -n  Number of requests to run.
  -c  Number of requests to run concurrently. Total number of requests cannot
      be smaller than the concurency level.
  -q  Rate limit, in seconds (QPS).
  -o  Output type. If none provided, a summary is printed.
      "csv" is the only supported alternative. Dumps the response
      metrics in comma-seperated values format.
  -h  fbase host.
  -m  mode 1: KvRawGet; 2: KvRawPut; 3: KvRawDelete 4: KvRawExecute 5: KvSelect 6: KvInsert 7: KvDelete
      8: CreateRange 9: DeleteRange 10: TransferLeader
  -times loop times
  -cpus                 Number of used cpu cores.
                        (default for current machine is %d cores)
`

func main() {
	fmt.Println(notice) // show deprecation notice
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, fmt.Sprintf(usage, runtime.NumCPU()))
	}

	flag.Parse()

	if *help {
		usageAndExit("")
	}

	runtime.GOMAXPROCS(*cpus)
	num := *n
	conc := *c
	q := *q
	host := *host

	if num <= 0 || conc <= 0 {
		usageAndExit("n and c cannot be smaller than 1.")
	}

	if num < conc {
		usageAndExit("n cannot be less than c")
	}

	if *output != "csv" && *output != "" {
		usageAndExit("Invalid output type; only csv is supported.")
	}

	w := &requester.Work{
		N:                  num,
		C:                  conc,
		Qps:                q,
		Host:               host,
		Output:             *output,
		Times:              *times,
		Mode:               *mode,
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		w.Finish()
		os.Exit(1)
	}()
	w.Run()
}


func usageAndExit(msg string) {
	if msg != "" {
		fmt.Fprintf(os.Stderr, msg)
		fmt.Fprintf(os.Stderr, "\n\n")
	}
	flag.Usage()
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(1)
}
