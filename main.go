package main

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

const rps = 1
const format = time.StampMilli

var increment = int64(0)
var requests = int64(0)
var queue = Queue{}

type Queue struct {
	sum int64
}

func (q *Queue) Drop(delta *int64) {
	atomic.AddInt64(&q.sum, atomic.SwapInt64(delta, 0))
}

func (q *Queue) Drain() int64 {
	return atomic.SwapInt64(&q.sum, 0)
}

func main() {
	go printIncrementWorker()
	handleParallelLowWorker(5)
}

func handleParallelLowWorker(workerCount int64) {
	timeout := time.Duration(time.Second.Nanoseconds() / rps)
	localIncrements := make([]int64, workerCount)

	for i := 0; i < int(workerCount); i++ {
		localIncrements[i] = int64(0)
		go dropWorker(&localIncrements[i])
	}

	go sendWorker(timeout)

	http.HandleFunc("/inc", func(writer http.ResponseWriter, request *http.Request) {
		requestID := atomic.AddInt64(&requests, 1)
		handlerIndex := requestID % workerCount
		atomic.AddInt64(&localIncrements[handlerIndex], 1)
		writer.WriteHeader(http.StatusOK)
	})

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Error: %v", err)
	}
}

func sendWorker(timeout time.Duration) {
	for {
		if val := queue.Drain(); val != 0 {
			atomic.AddInt64(&increment, val)
			fmt.Printf("[%v] add: %d\n", time.Now().Format(format), val)
			time.Sleep(timeout)
		}
	}
}

func dropWorker(localInc *int64) {
	for {
		if *localInc != 0 {
			queue.Drop(localInc)
		}
	}
}

func printIncrementWorker() {
	for {
		fmt.Printf("[%v] Increment: %d Requests: %d\n", time.Now().Format(format), atomic.LoadInt64(&increment), atomic.LoadInt64(&requests))
		time.Sleep(1 * time.Second)
	}
}
