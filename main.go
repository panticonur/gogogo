package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/FZambia/tarantool"
	"github.com/davecgh/go-spew/spew"
	"gopkg.in/yaml.v3"
)

type BucketForceCreateOpts struct {
	FirstBucketId int64
	Count         int64
}

type Replicaset struct {
	Weight            float64 `yaml:"weight"`
	Replicas          map[string]Replica
	EtalonBucketCount int
	IgnoreDisbalance  bool
	PinnedCount       int
}

type Replica struct {
	Master bool   `yaml:"master"`
	Uri    string `yaml:"uri"`
	Name   string `yaml:"name"`
}

type BucketRoute struct {
	BucketId   uint64
	Connection *tarantool.Connection
}

type VshardConfig struct {
	RebalancerMaxReceiving        int  `yaml:"rebalancer_max_receiving"`
	BucketCount                   int  `yaml:"bucket_count"`
	CollectLuaGarbage             bool `yaml:"collect_lua_garbage"`
	SyncTimeout                   int  `yaml:"sync_timeout"`
	ReadOnly                      bool `yaml:"read_only"`
	SchedRefQuota                 int  `yaml:"sched_ref_quota"`
	RebalancerDisbalanceThreshold int  `yaml:"rebalancer_disbalance_threshold"`
	RebalancerMaxSending          int  `yaml:"rebalancer_max_sending"`
	SchedMoveQuota                int  `yaml:"sched_move_quota"`
	Sharding                      map[string]Replicaset
}

var VshardConfigFilename = "/tmp/vshard_cfg.yaml"

type Router struct {
	Replicasets map[string]*tarantool.Connection
	Routes      sync.Map
	VshardCfg   VshardConfig
}

// console1> rm -rf tmp; cartridge start
// console2> cartridge replicasets setup --bootstrap-vshard
// console3> cartridge enter srv-2
// console3> function p1(a) local log = require('log') log.info("p1") log.info(a) end

func main() {
	router := Router{
		Replicasets: make(map[string]*tarantool.Connection),
	}
	if err := router.ReadConfig(VshardConfigFilename); err != nil {
		log.Fatalf("error reading '%s' vshard config\n%v", VshardConfigFilename, err)
	}
	if err := router.ConnectMasterInstancies(); err != nil {
		log.Fatalf("connection error\n%v", err)
	}
	defer router.CloseConnections()

	if err := router.Bootstrap(); err != nil {
		log.Fatalf("bootstap error\n%v", err)
	}

	router.DiscoveryBuckets()

	var bucketId uint64 = 1
	for ; bucketId <= uint64(router.VshardCfg.BucketCount); bucketId += 500 {
		proc := "p1"
		if _, err := router.RPC(bucketId, proc, []interface{}{101}); err != nil {
			log.Printf("could not call remote proc '%s'\n%q", proc, err)
		}
	}
}

func (r *Router) ReadConfig(configFilename string) error {
	log.Printf("read vshard cfg yaml file %s", configFilename)
	yamlFile, err := ioutil.ReadFile(configFilename)
	if err != nil {
		return fmt.Errorf("reading yaml error\n%v", err)
	}

	if err := yaml.Unmarshal(yamlFile, &r.VshardCfg); err != nil {
		return fmt.Errorf("unmarshal vshard config error\n%v", err)
	}
	return nil
}

func (r *Router) ConnectMasterInstancies() error {
	for replicasetUuid, replicaset := range r.VshardCfg.Sharding {
		for _, replica := range replicaset.Replicas {
			if replica.Master {
				u, err := url.Parse("tarantool://" + replica.Uri)
				if err != nil {
					return fmt.Errorf("could not parse URI %s\n%v", replica.Uri, err)
				}

				conn, err := connection(u.Host)
				if err != nil {
					return fmt.Errorf("could not connect to %s\n%v", u.Host, err)
				}

				r.Replicasets[replicasetUuid] = conn
				log.Printf("append replicaset %s, master %s", replicasetUuid, u.Host)
				break
			}
		}
	}
	return nil
}

func connection(host string) (conn *tarantool.Connection, err error) {
	log.Println("connecting to " + host)
	opts := tarantool.Opts{
		RequestTimeout: 500 * time.Millisecond,
		User:           "admin",
	}

	conn, err = tarantool.Connect(host, opts)
	if err != nil {
		return nil, fmt.Errorf("connection to %s refused\n%v", host, err)
	}

	_, err = conn.Exec(
		tarantool.Eval("__vshard_storage_init = require('vshard.storage.init')", []interface{}{}))
	if err != nil {
		return nil, fmt.Errorf("could not init vshard storage %s\n%v", host, err)
	}

	log.Println(host + " connected!")
	return conn, nil
}

func (r *Router) CloseConnections() {
	for replicasetUuid, conn := range r.Replicasets {
		conn.Close()
		log.Printf("close connection replicaset %s", replicasetUuid)
	}
}

func (r *Router) RPC(bucketId uint64, proc string, args []interface{}) (retVal []interface{}, err error) {
	// https://github.com/tarantool/vshard#adding-data
	conn, loaded := r.Routes.Load(bucketId)
	if !loaded {
		return nil, fmt.Errorf("could not find bucket #%d", bucketId)
	}
	retVal, err = conn.(*tarantool.Connection).Exec(
		tarantool.Call(proc, args))
	if err != nil {
		return nil, err
	}
	log.Printf("successful call '%s' remote procedure, bucket #%d", proc, bucketId)
	return retVal, nil
}

func (r *Router) DiscoveryBuckets() error {
	var wg sync.WaitGroup

	log.Println("start async tasks")
	for _, conn := range r.Replicasets {
		wg.Add(1)
		go r.ReadBuckets(conn, &wg)
	}

	wg.Wait()
	return nil
}

func (r *Router) ReadBuckets(conn *tarantool.Connection, wg *sync.WaitGroup) {
	defer wg.Done()
	var lastBucketId uint64 = 0
	for c := 0; c < 2000; c++ {
		result, err := conn.Exec(
			tarantool.Select("_bucket", "pk", 0, 1000, tarantool.IterGt, []interface{}{lastBucketId}))
		if err != nil {
			log.Printf("fail to select active buckets\n%q", err)
		}
		if len(result) == 0 {
			break
		}

		for _, bucket := range result {
			bucketId, ok := bucket.([]interface{})[0].(uint64)
			if !ok {
				_bucketId, ok := bucket.([]interface{})[0].(int64)
				if !ok {
					log.Printf("could not cast bucket[0] to int64 and uint64")
					continue
				}
				bucketId = uint64(_bucketId)
			}
			if st := bucket.([]interface{})[1].(string); st == "active" || st == "pinned" {
				r.Routes.Store(bucketId, conn)
			}
			lastBucketId = bucketId
		}
	}
}

func (r *Router) Bootstrap() error {
	for replicasetUuid, conn := range r.Replicasets {
		log.Printf("get bucket.count from %s", replicasetUuid)
		bucketCount, err := getBucketCount(conn)
		if err != nil {
			return fmt.Errorf("fail to get bucket count %s\n%v", replicasetUuid, err)
		}
		log.Printf("bucketCount = %d", bucketCount)
		if bucketCount > 0 {
			return fmt.Errorf("replicaset %s is already bootstrapped", replicasetUuid)
		}
	}

	clusterCalculateEtalonBalance(&r.VshardCfg)
	spew.Dump(r.VshardCfg)

	var firstBucketId int = 1
	for replicasetUuid, conn := range r.Replicasets {
		etalonBucketCount := r.VshardCfg.Sharding[replicasetUuid].EtalonBucketCount
		log.Printf("bootstrap replicaset %s firstBucketId=%d etalonBucketCount=%d", replicasetUuid, firstBucketId, etalonBucketCount)
		cmd := "__vshard_storage_init.bucket_force_create"
		result, err := conn.Exec(
			tarantool.Call(cmd, []interface{}{firstBucketId, etalonBucketCount}))
		if err != nil {
			return fmt.Errorf("fail to %s\n%v", cmd, err)
		}

		if !result[0].(bool) {
			return fmt.Errorf("fail to bootstrap replicaset %s", replicasetUuid)
		}
		log.Printf("replicaset %s bootstrapped!", replicasetUuid)

		firstBucketId += etalonBucketCount
	}
	return nil
}

func getBucketCount(conn *tarantool.Connection) (bucketCount int64, err error) {
	cmd := "box.space._bucket:count" // or "vshard.storage.buckets_count"
	rawBucketCount, err := conn.Exec(
		tarantool.Call(cmd, []interface{}{}))
	if err != nil {
		return 0, fmt.Errorf("could not get %s\n%v", cmd, err)
	}

	bucketCount, ok := rawBucketCount[0].(int64)
	if !ok {
		_bucketCount, ok := rawBucketCount[0].(uint64)
		if !ok {
			return 0, fmt.Errorf("could not cast rawBucketCount[0] to int64 and uint64")
		}
		bucketCount = int64(_bucketCount)
	}
	return bucketCount, nil
}

func clusterCalculateEtalonBalance(vshardCfg *VshardConfig) error {
	log.Println("calculating etalon balance")
	replicasets := vshardCfg.Sharding
	bucketCount := vshardCfg.BucketCount
	isBalanceFound := false
	var weightSum float64 = 0
	stepCount := 0
	replicasetCount := 0

	for _, replicaset := range replicasets {
		weightSum = weightSum + replicaset.Weight
		replicasetCount = replicasetCount + 1
	}

	for !isBalanceFound {
		stepCount = stepCount + 1
		if weightSum <= 0 {
			return fmt.Errorf("assert(weight_sum > 0) but weight_sum = %f", weightSum)
		}
		var bucketPerWeight float64 = float64(bucketCount) / weightSum
		bucketsCalculated := 0
		for k, replicaset := range replicasets {
			if !replicaset.IgnoreDisbalance {
				replicaset.EtalonBucketCount = int(math.Ceil(replicaset.Weight * bucketPerWeight))
				bucketsCalculated = bucketsCalculated + replicaset.EtalonBucketCount
			}
			replicasets[k] = replicaset
		}

		bucketsRest := bucketsCalculated - bucketCount
		isBalanceFound = true
		for k, replicaset := range replicasets {
			if !replicaset.IgnoreDisbalance {
				// A situation is possible, when bucket_per_weight
				// is not integer. Lets spread this disbalance
				// over the cluster.
				if bucketsRest > 0 {
					n := replicaset.Weight * bucketPerWeight
					ceil := math.Ceil(n)
					floor := math.Floor(n)
					if replicaset.EtalonBucketCount > 0 && ceil != floor {
						replicaset.EtalonBucketCount = replicaset.EtalonBucketCount - 1
						bucketsRest = bucketsRest - 1
					}
				}
				//
				// Search for incorrigible disbalance due to
				// pinned buckets.
				//
				pinned := replicaset.PinnedCount
				if pinned != 0 && replicaset.EtalonBucketCount < pinned {
					// This replicaset can not send out enough
					// buckets to reach a balance. So do the best
					// effort balance by sending from the
					// replicaset though non-pinned buckets. This
					// replicaset and its pinned buckets does not
					// participate in the next steps of balance
					// calculation.
					isBalanceFound = false
					bucketCount = bucketCount - replicaset.PinnedCount
					replicaset.EtalonBucketCount = replicaset.PinnedCount
					replicaset.IgnoreDisbalance = true
					weightSum = weightSum - replicaset.Weight
				}
			}
			replicasets[k] = replicaset
		}
		if bucketsRest != 0 {
			return fmt.Errorf("assert(buckets_rest == 0) but buckets_rest = %d", bucketsRest)
		}
		if stepCount > replicasetCount {
			// This can happed only because of a bug in this
			// algorithm. But it occupies 100% of transaction
			// thread, so check step count explicitly.
			return fmt.Errorf("the rebalancer is broken")
		}
	}
	return nil
}
