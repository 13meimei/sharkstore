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

package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime"

	"tool/boom/boomer"
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

var (
	user = flag.String("u", "", "")
	password =  flag.String("p", "", "")
	host = flag.String("h", "", "")
	database = flag.String("d", "", "")
	table =  flag.String("t", "split", "")
	output = flag.String("o", "", "")

	c = flag.Int("c", 50, "")
	n = flag.Int("n", 200, "")
	q = flag.Int("q", 0, "")
	l = flag.Int("l", 255, "")
	mode = flag.Int("m", 1, "")
	base = flag.Int("b", 1, "")
	times = flag.Int("times", 1, "")

	cpus = flag.Int("cpus", runtime.GOMAXPROCS(-1), "")

	help = flag.Bool("help", false, "")
	verify = flag.Bool("v", false, "")
	vStep= flag.Bool("vstep", false, "")
	vDeletePeer = flag.Uint64("vdeletepeer", 0, "")
	rangeNumber = flag.Uint64("range-number", 1, "")
	multiNumber = flag.Uint64("multi-number", 1, "")
	clusterId = flag.Uint64("clusterid", 0, "")
	clusterToken = flag.String("clustertoken", "", "")
	masterAddr = flag.String("master-addr", "", "")
)

var usage = `Usage: boom [options...] <url>

Options:
  -n  Number of requests to run.
  -c  Number of requests to run concurrently. Total number of requests cannot
      be smaller than the concurency level.
  -q  Rate limit, in seconds (QPS).
  -o  Output type. If none provided, a summary is printed.
      "csv" is the only supported alternative. Dumps the response
      metrics in comma-seperated values format.

  -u  fbase user name.
  -p  fbase password.
  -h  fbase host.
  -d  database.
  -t  table.
  -m  mode 1: random insert; 2: order insert; 3: order select 4: signal select 5: random select 6: hash random insert 7:hash random select 8:user tp table test
  -times loop times

  -cpus                 Number of used cpu cores.
                        (default for current machine is %d cores)
  -l  data length for random.
  -v			verify
  -vstep 		verify step
  -vdeletepeer 		verify with occurs peed delete duration(sec)
	-clusterid 	clusterid uint64
	-clustertoken  	clsuter token from master config
	-master-addr    	ip:port
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

	if num <= 0 || conc <= 0 {
		usageAndExit("n and c cannot be smaller than 1.")
	}

	if num < conc {
		usageAndExit("n cannot be less than c")
	}

	if *output != "csv" && *output != "" {
		usageAndExit("Invalid output type; only csv is supported.")
	}
	if len(*user) == 0 || len(*password) == 0 || len(*host) == 0 || len(*database) == 0 {
		usageAndExit("Invalid mysql dns.")
	}

	(&boomer.Boomer{
		N:                  num,
		C:                  conc,
		Qps:                q,
		DLen:               *l,
		Output:             *output,

		User:               *user,
		Password:           *password,
		Database:           *database,
		Table:              *table,
		Host:               *host,
		Mode:               *mode,
		Base:               *base,
		Times:              *times,
		Verify: *verify,
		StepVerify: *vStep,
		DeletePeerVerify: *vDeletePeer,
		ClusterId: *clusterId,
		ClusterToken: *clusterToken,
		MasterAddr: *masterAddr,
		RangeNumber: *rangeNumber,
		MultiNumber: *multiNumber,
	}).Run()
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

func parseInputWithRegexp(input, regx string) ([]string, error) {
	re := regexp.MustCompile(regx)
	matches := re.FindStringSubmatch(input)
	if len(matches) < 1 {
		return nil, fmt.Errorf("could not parse the provided input; input = %v", input)
	}
	return matches, nil
}
