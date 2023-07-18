package recipe

import (
	"fmt"
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
		etcdCli = client
	})
	return etcdCli, err
}

func TestMain(m *testing.M) {
	InitEtcd(&EtcdConf{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5,
	})
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
	})
}
