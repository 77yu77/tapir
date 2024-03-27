package lockserver

import (
	"fmt"
	"time"
)

type LogState string

const (
	Prepare LogState = "prepare"
	Abort   LogState = "abort"
	Commit  LogState = "commit"
)

type Log struct {
	Client    string
	Lock      string
	State     bool
	TimeStamp string
	Next      *Log
	LogState  LogState
}

type Store struct {
	Mu1_state bool
	Mu2_state bool
	Logs      *Log
}

func StartReplica(rch *chan Log, sch *chan Log, id string) {
	store := Store{false, false, &Log{"", "", false, "", nil, ""}}
	for {
		log := <-*rch
		if log.Lock == "lock1" {
			var node *Log
			for node = store.Logs; node.Next != nil; node = node.Next {
				if node.TimeStamp == log.TimeStamp {
					break
				}
			}
			if node.TimeStamp == log.TimeStamp {
				node.LogState = log.LogState
			} else {
				node.Next = &log
			}
			commit := true
			if log.State == store.Mu1_state {
				log.LogState = Abort
				commit = false
			} else {
				store.Mu1_state = log.State
			}
			*sch <- Log{"", "", commit, log.TimeStamp, nil, ""}

		}
		if log.Lock == "lock2" {
			var node *Log
			for node = store.Logs; node.Next != nil; node = node.Next {
				if node.TimeStamp == log.TimeStamp {
					break
				}
			}
			if node.TimeStamp == log.TimeStamp {
				node.LogState = log.LogState
			} else {
				node.Next = &log
			}
			commit := true
			if log.State == store.Mu2_state {
				log.LogState = Abort
				commit = false
			} else {
				store.Mu2_state = log.State
			}
			*sch <- Log{"", "", commit, log.TimeStamp, nil, ""}

		}
		if log.Lock == "sync" {
			*sch <- *store.Logs
		}
		if log.Lock == "sync_return" {
			store.Logs.Next = log.Next
			//重写store状态
			for node := store.Logs.Next; node != nil; node = node.Next {
				if node.LogState != Abort {
					if node.Lock == "lock1" {
						store.Mu1_state = node.State
					} else if node.Lock == "lock2" {
						store.Mu2_state = node.State
					}

				}
			}
		}
		if log.Lock == "print" {
			for node := store.Logs.Next; node != nil; node = node.Next {
				var state string
				if node.State {
					state = "lock"
				} else {
					state = "unlock"
				}
				fmt.Printf("{%s %s %s  %s %s}\n", node.Client, node.Lock, state, node.TimeStamp, node.LogState)
			}
			*sch <- Log{"", "over", true, "", nil, ""}
		}
		time.Sleep(500 * time.Microsecond)
	}

}

func LogSync(Logs []*Log) {
	m := make(map[string]Log)
	for _, log := range Logs {
		for node := log.Next; node != nil; node = node.Next {
			if _, ok := m[node.TimeStamp]; !ok {
				m[node.TimeStamp] = *node
			}
		}
	}
	for _, log := range Logs {
		m1 := make(map[string]Log)

		for k, v := range m {
			m1[k] = v
		}
		var node *Log
		if log.Next == nil {
			for _, llog := range m1 {
				log.Next = &llog
				log = log.Next
			}
			log.Next = nil
		} else {
			for node = log.Next; node.Next != nil; node = node.Next {
				delete(m1, node.TimeStamp)
			}

			delete(m1, node.TimeStamp)
			for _, llog := range m1 {
				node.Next = &llog
				node = node.Next
			}
			node.Next = nil
		}
	}
}

func SyncReplica(rch1 *chan Log, sch1 *chan Log, rch2 *chan Log, sch2 *chan Log, rch3 *chan Log, sch3 *chan Log) {
	*rch1 <- Log{"", "sync", true, "", nil, ""}
	*rch2 <- Log{"", "sync", true, "", nil, ""}
	*rch3 <- Log{"", "sync", true, "", nil, ""}
	Log1 := <-*sch1
	Log2 := <-*sch2
	Log3 := <-*sch3
	// for node := Log2.Next; node != nil; node = node.Next {
	// 	fmt.Println(*node)
	// 	fmt.Println("ss")
	// }
	LogSync([]*Log{&Log1, &Log2, &Log3})
	// for node := Log2.Next; node != nil; node = node.Next {
	// 	fmt.Println(*node)
	// }
	*rch1 <- Log{"", "sync_return", true, "", Log1.Next, ""}
	*rch2 <- Log{"", "sync_return", true, "", Log2.Next, ""}
	*rch3 <- Log{"", "sync_return", true, "", Log3.Next, ""}
}
func PrintLog(rch1 *chan Log, sch1 *chan Log) {
	*rch1 <- Log{"", "print", true, "", nil, ""}
	Log1 := <-*sch1
	Log1.Lock = ""
}

func PrintLogs(rch1 *chan Log, sch1 *chan Log, rch2 *chan Log, sch2 *chan Log, rch3 *chan Log, sch3 *chan Log) {
	fmt.Println("replica1 log:")
	PrintLog(rch1, sch1)
	fmt.Println("replica2 log:")
	PrintLog(rch2, sch2)
	fmt.Println("replica3 log:")
	PrintLog(rch3, sch3)
}
