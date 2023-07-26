package recipe

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	once    sync.Once
	etcdCli *clientv3.Client
)

type EtcdConf struct {
	Endpoints   []string
	DialTimeout int32
	Username    string
	Password    string
}

func InitEtcd(conf *EtcdConf) (*clientv3.Client, error) {
	if etcdCli != nil {
		return etcdCli, nil
	}

	var err error
	once.Do(func() {
		var client *clientv3.Client
		client, err = clientv3.New(clientv3.Config{
			Endpoints:   conf.Endpoints,
			DialTimeout: time.Duration(conf.DialTimeout) * time.Second,
		})
		if err != nil {
			fmt.Printf("clientv3.New failed, err=%v, conf=%v", err, conf)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = client.Sync(ctx)
		if err != nil {
			fmt.Printf("client.Sync failed, err=%v, conf=%v", err, conf)
			return
		}
		etcdCli = client
	})
	return etcdCli, err
}

func TestMain(m *testing.M) {
	_, err := InitEtcd(&EtcdConf{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5,
	})
	if err != nil {
		return
	}
	m.Run()
}

func TestQueue(t *testing.T) {
	q := NewQueue(etcdCli, "test")
	Convey("TestQueue", t, func() {
		Convey("Empty", func() {
			ok, err := q.Empty()
			So(err, ShouldEqual, nil)
			So(ok, ShouldEqual, true)
		})
		Convey("Enqueue", func() {
			err := q.Enqueue("1")
			So(err, ShouldEqual, nil)
			err = q.Enqueue("2")
			So(err, ShouldEqual, nil)
		})
		Convey("Dequeue", func() {
			val, err := q.Dequeue(false)
			So(err, ShouldEqual, nil)
			So(val, ShouldEqual, "1")
			val, err = q.Dequeue(false)
			So(err, ShouldEqual, nil)
			So(val, ShouldEqual, "2")
		})
		Convey("Empty after Dequeue", func() {
			ok, err := q.Empty()
			So(err, ShouldEqual, nil)
			So(ok, ShouldEqual, true)
		})
		Convey("Clear", func() {
			err := q.Enqueue("3")
			So(err, ShouldEqual, nil)
			cnt, err := q.Delete()
			So(err, ShouldEqual, nil)
			So(cnt, ShouldEqual, 1)
			ok, err := q.Empty()
			So(err, ShouldEqual, nil)
			So(ok, ShouldEqual, true)
		})
	})
}

func TestConcurrencyReadWrite(t *testing.T) {
	go func() {
		err := http.ListenAndServe(":8081", nil)
		if err != nil {
			fmt.Printf("http.ListenAndServe failed, err:%s", err)
		}
	}()

	q := NewQueue(etcdCli, "test")
	q.Delete()
	var wg sync.WaitGroup
	concurrency := 2
	wg.Add(concurrency)
	start := time.Now()
	beginId := 0
	batch := 1000
	for i := 0; i < concurrency; i++ {
		i := i
		go func(beginId int) {
			for j := beginId; j < beginId+batch; j++ {
				q.Enqueue(strconv.Itoa(j))
			}
			fmt.Println("enqueue done:", i)
			wg.Done()
		}(beginId)
		beginId += batch
	}
	wg.Wait()
	fmt.Println("enqueue cost:", time.Since(start))

	wg.Add(concurrency + 1)
	start = time.Now()
	ch := make(chan int)
	for i := 0; i < concurrency; i++ {
		i := i
		go func() {
			for {
				val, err := q.Dequeue(false)
				if err != nil {
					fmt.Println(err)
					if err == ErrEmptyQueue {
						break
					}
					panic(err)
				}
				i, _ := strconv.Atoi(val)
				ch <- i
			}
			fmt.Println("dequeue done:", i)
			ch <- -1
			wg.Done()
		}()
	}
	tmp := make([][]int, concurrency)
	for i := 0; i < concurrency; i++ {
		tmp[i] = make([]int, 0, batch)
	}
	go func() {
		done := 0
		for i := range ch {
			if i == -1 {
				done++
				if done == concurrency {
					wg.Done()
					return
				}
			} else {
				tmp[i/batch] = append(tmp[i/batch], i)
			}
		}
	}()
	wg.Wait()
	fmt.Println("dequeue cost:", time.Since(start))
	var queueSize int
	for i := 0; i < concurrency; i++ {
		for j := 0; j < len(tmp[i]); j++ {
			if j+i*batch != tmp[i][j] {
				panic("concurrency error")
			}
		}
		queueSize += len(tmp[i])
	}
	Convey("TestConcurrencyReadWrite", t, func() {
		Convey("queue size", func() {
			So(queueSize, ShouldEqual, concurrency*batch)
		})
	})
}

func TestConcurrencyReadWriteWithFailure(t *testing.T) {
	go func() {
		err := http.ListenAndServe(":8081", nil)
		if err != nil {
			fmt.Printf("http.ListenAndServe failed, err:%s", err)
		}
	}()

	q := NewQueue(etcdCli, "test")
	q.Delete()
	var wg sync.WaitGroup
	concurrency := 2
	wg.Add(concurrency)
	start := time.Now()
	beginId := 0
	batch := 1000
	for i := 0; i < concurrency; i++ {
		i := i
		go func(beginId int) {
			for j := beginId; j < beginId+batch; j++ {
				q.Enqueue(strconv.Itoa(j))
			}
			fmt.Println("enqueue done:", i)
			wg.Done()
		}(beginId)
		beginId += batch
	}
	wg.Wait()
	fmt.Println("enqueue cost:", time.Since(start))

	wg.Add(concurrency + 1)
	start = time.Now()
	ch := make(chan int)
	var reEnqueueNum int
	for i := 0; i < concurrency; i++ {
		i := i
		go func() {
			for {
				val, err := q.Dequeue(false)
				if err != nil {
					fmt.Println(err)
					if err == ErrEmptyQueue {
						break
					}
					panic(err)
				}
				i, _ := strconv.Atoi(val)
				if i%2 == 0 || reEnqueueNum >= concurrency*batch/2 {
					ch <- i
				} else {
					reEnqueueNum++
					q.Enqueue(strconv.Itoa(i))
				}
			}
			fmt.Println("dequeue done:", i)
			ch <- -1
			wg.Done()
		}()
	}
	tmp := make([][]int, concurrency)
	for i := 0; i < concurrency; i++ {
		tmp[i] = make([]int, 0, batch)
	}
	go func() {
		done := 0
		for i := range ch {
			if i == -1 {
				done++
				if done == concurrency {
					wg.Done()
					return
				}
			} else {
				tmp[i/batch] = append(tmp[i/batch], i)
			}
		}
	}()
	wg.Wait()
	fmt.Println("dequeue cost:", time.Since(start))
	var queueSize int
	for i := 0; i < concurrency; i++ {
		sort.Slice(tmp[i], func(i2, j int) bool {
			return tmp[i][i2] < tmp[i][j]
		})
		for j := 0; j < len(tmp[i]); j++ {
			if j+i*batch != tmp[i][j] {
				panic("concurrency error")
			}
		}
		queueSize += len(tmp[i])
	}
	Convey("TestConcurrencyReadWriteWithFailure", t, func() {
		Convey("queue size", func() {
			So(queueSize, ShouldEqual, concurrency*batch)
		})
	})
}
