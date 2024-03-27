package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	lo "example.com/m/lockserver"
)

var mu1 sync.Mutex
var mu2 sync.Mutex
var n = 3
var f = 1

func judge(f int, flags ...bool) bool {
	num := 0
	for _, flag := range flags {
		if flag {
			num++
		}
	}
	return num >= f
}

func main() {
	var input string
	test, _ := strconv.Atoi(os.Args[1])
	rch1 := make(chan lo.Log)
	sch1 := make(chan lo.Log)
	rch2 := make(chan lo.Log)
	sch2 := make(chan lo.Log)
	rch3 := make(chan lo.Log)
	sch3 := make(chan lo.Log)
	switch test {
	// test1
	case 1:
		go lo.StartReplica(&rch1, &sch1, "1")
		go lo.StartReplica(&rch2, &sch2, "2")
		go lo.StartReplica(&rch3, &sch3, "3")
		{ //向replica1，2发送锁请求
			fmt.Println("client send message 1 to replica 1 and 2")
			rch1 <- lo.Log{
				Client:    "client1",
				Lock:      "lock1",
				State:     true,
				TimeStamp: "1", Next: nil, LogState: lo.Prepare}
			rch2 <- lo.Log{Client: "client1", Lock: "lock1",
				State:     true,
				TimeStamp: "1", Next: nil, LogState: lo.Prepare}
			Log1 := <-sch1
			Log2 := <-sch2

			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)

			if judge(f+1, Log1.State, Log2.State) {
				rch1 <- lo.Log{Client: "client1", Lock: "lock1",
					State:     true,
					TimeStamp: "1", Next: nil, LogState: lo.Commit}
				rch2 <- lo.Log{Client: "client1", Lock: "lock1",
					State:     true,
					TimeStamp: "1", Next: nil, LogState: lo.Commit}
				Log1 = <-sch1
				Log2 = <-sch2
				if judge(f+1, Log1.State, Log2.State) {
					mu1.Lock()
					fmt.Println("lock1 成功上锁")
				}
			}

			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)
		}

		lo.SyncReplica(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
		time.Sleep(3 * time.Second)

		lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)

	//test2
	case 2:
		go lo.StartReplica(&rch1, &sch1, "1")
		go lo.StartReplica(&rch2, &sch2, "2")
		go lo.StartReplica(&rch3, &sch3, "3")

		{ //client1向replica1，2发送锁lock1请求
			rch1 <- lo.Log{Client: "client1", Lock: "lock1",
				State:     true,
				TimeStamp: "1", Next: nil, LogState: lo.Prepare}

			rch2 <- lo.Log{Client: "client1", Lock: "lock1",
				State:     true,
				TimeStamp: "1", Next: nil, LogState: lo.Prepare}
			Log1 := <-sch1
			Log2 := <-sch2

			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)

			if judge(f, Log1.State, Log2.State) {
				rch1 <- lo.Log{Client: "client1", Lock: "lock1",
					State:     true,
					TimeStamp: "1", Next: nil, LogState: lo.Commit}
				rch2 <- lo.Log{Client: "client1", Lock: "lock1",
					State:     true,
					TimeStamp: "1", Next: nil, LogState: lo.Commit}
				Log1 = <-sch1
				Log2 = <-sch2
				if judge(f, Log1.State, Log2.State) {
					mu1.Lock()
				}
			}
			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)
		}

		{ //client2向replica1，3发送锁lock2请求
			rch1 <- lo.Log{Client: "client2", Lock: "lock2",
				State:     true,
				TimeStamp: "2", Next: nil, LogState: lo.Prepare}

			rch3 <- lo.Log{Client: "client2", Lock: "lock2",
				State:     true,
				TimeStamp: "2", Next: nil, LogState: lo.Prepare}
			Log1 := <-sch1
			Log3 := <-sch3
			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)

			if judge(f, Log1.State, Log3.State) {
				rch1 <- lo.Log{Client: "client2", Lock: "lock2",
					State:     true,
					TimeStamp: "2", Next: nil, LogState: lo.Commit}
				rch3 <- lo.Log{Client: "client2", Lock: "lock2",
					State:     true,
					TimeStamp: "2", Next: nil, LogState: lo.Commit}
				Log1 = <-sch1
				Log3 = <-sch3
				if judge(f, Log1.State, Log3.State) {
					mu2.Lock()
				}
			}
			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)
		}

		lo.SyncReplica(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
		time.Sleep(1 * time.Second)

		lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)

	//test3
	case 3:
		go lo.StartReplica(&rch1, &sch1, "1")
		go lo.StartReplica(&rch2, &sch2, "2")
		go lo.StartReplica(&rch3, &sch3, "3")
		{ //client1向replica1，2发送锁lock1请求
			rch1 <- lo.Log{Client: "client1", Lock: "lock1",
				State:     true,
				TimeStamp: "1", Next: nil, LogState: lo.Prepare}

			rch2 <- lo.Log{Client: "client1", Lock: "lock1",
				State:     true,
				TimeStamp: "1", Next: nil, LogState: lo.Prepare}
			Log1 := <-sch1
			Log2 := <-sch2

			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)
			// client2向replica2，3发送锁lock1请求
			rch2 <- lo.Log{Client: "client2", Lock: "lock1",
				State:     true,
				TimeStamp: "2", Next: nil, LogState: lo.Prepare}

			rch3 <- lo.Log{Client: "client2", Lock: "lock1",
				State:     true,
				TimeStamp: "2", Next: nil, LogState: lo.Prepare}
			Log21 := <-sch2
			Log31 := <-sch3

			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)
			if judge(f+1, Log21.State, Log31.State) {
				rch2 <- lo.Log{Client: "client2", Lock: "lock1",
					State:     true,
					TimeStamp: "2", Next: nil, LogState: lo.Commit}
				rch3 <- lo.Log{Client: "client2", Lock: "lock1",
					State:     true,
					TimeStamp: "2", Next: nil, LogState: lo.Commit}
				Log21 = <-sch2
				Log31 = <-sch3
				if judge(f, Log21.State, Log31.State) {
					mu1.Lock()
				}
			} else {
				fmt.Println("client2未收到至少f+1个正确回复,发生冲突")
				rch2 <- lo.Log{Client: "client2", Lock: "lock1",
					State:     true,
					TimeStamp: "2", Next: nil, LogState: lo.Abort}
				rch3 <- lo.Log{Client: "client2", Lock: "lock1",
					State:     true,
					TimeStamp: "2", Next: nil, LogState: lo.Abort}
				Log21 = <-sch2
				Log31 = <-sch3
			}

			if judge(f, Log1.State, Log2.State) {
				rch1 <- lo.Log{Client: "client1", Lock: "lock1",
					State:     true,
					TimeStamp: "1", Next: nil, LogState: lo.Commit}
				rch2 <- lo.Log{Client: "client1", Lock: "lock1",
					State:     true,
					TimeStamp: "1", Next: nil, LogState: lo.Commit}
				Log1 = <-sch1
				Log2 = <-sch2
				if judge(f, Log1.State, Log2.State) {
					mu1.Lock()
				}
			}
			lo.PrintLogs(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
			fmt.Scanf("%s", &input)
		}

		lo.SyncReplica(&rch1, &sch1, &rch2, &sch2, &rch3, &sch3)
		time.Sleep(1 * time.Second)

		fmt.Println("replica1 log:")
		lo.PrintLog(&rch1, &sch1)
		fmt.Println("replica2 log:")
		lo.PrintLog(&rch2, &sch2)
		fmt.Println("replica3 log:")
		lo.PrintLog(&rch3, &sch3)
	}
}
