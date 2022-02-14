package main

import (
	"io/ioutil"
	"log"
	"math"
	"net/url"
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

type VshardCfg struct {
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

var cfgFilename = "/tmp/vshard_cfg.yaml"

func main() {
	replicasets := map[string]*tarantool.Connection{}

	log.Printf("read vshard cfg yaml file %s", cfgFilename)
	yamlFile, err := ioutil.ReadFile(cfgFilename)
	if err != nil {
		log.Fatalf("reading yaml error\n%q", err)
	}

	vshardCfgData := VshardCfg{}
	err = yaml.Unmarshal(yamlFile, &vshardCfgData)
	if err != nil {
		log.Fatalf("unmarshal vshard config error\n%q", err)
	}

	for replicasetUuid, replicaset := range vshardCfgData.Sharding {
		for _, replica := range replicaset.Replicas {
			if replica.Master {
				u, err := url.Parse("tarantool://" + replica.Uri)
				if err != nil {
					log.Fatalf("could not parse URI %s\n%q", replica.Uri, err)
				}

				conn, err := connection(u.Host)
				if err != nil {
					log.Fatalf("could connect to %s\n%q", u.Host, err)
				}
				defer conn.Close()
				replicasets[replicasetUuid] = conn // append
				log.Printf("append replicaset %s", replicasetUuid)

				bucketCount := getBucketCount(conn)
				log.Printf("bucketCount = %d", bucketCount)
				if bucketCount > 0 {
					log.Fatalf("storage %s is already bootstrapped.", u.Host)
				}
				break
			}
		}
	}
	/*
		// cartridge enter srv-2
		// box.space._bucket.index.status:select("active", {limit=10})
		// https://github.com/tarantool/cartridge-cli/blob/master/cli/commands/cartridge.go

		var routing map[uint64]*tarantool.Connection = make(map[uint64]*tarantool.Connection)
		for {
			for _, conn := range replicasets {
				result, err := conn.Exec(
					tarantool.Select("_bucket", "status", 0, uint32(getBucketCount(conn)), tarantool.IterEq, []interface{}{"active"}))
				if err != nil {
					log.Fatalf("fail to select active buckets\n%q", err)
				}

				for _, bucket := range result {

					bucket_id, ok := bucket.([]interface{})[0].(uint64)
					if !ok {
						bucket_id_i, ok := bucket.([]interface{})[0].(int64)
						if !ok {
							log.Fatalf("bucket_id_i not int64")
						}
						bucket_id = uint64(bucket_id_i)
					}

					routing[bucket_id] = conn
				}
			}
			break
		}
		log.Println(routing)
	*/

	clusterCalculateEtalonBalance(&vshardCfgData)
	spew.Dump(vshardCfgData)

	var firstBucketId int = 1
	for replicasetUuid, conn := range replicasets {
		bootstrap(replicasetUuid, conn, firstBucketId, vshardCfgData.Sharding[replicasetUuid].EtalonBucketCount)
		firstBucketId = firstBucketId + vshardCfgData.Sharding[replicasetUuid].EtalonBucketCount
	}

}

func connection(host string) (conn *tarantool.Connection, err error) {
	log.Println("connecting to " + host)
	opts := tarantool.Opts{
		RequestTimeout: 500 * time.Millisecond,
		User:           "admin",
	}

	conn, err = tarantool.Connect(host, opts)
	if err != nil {
		log.Fatalf("connection to %s refused\n%q", host, err)
	}

	_, err = conn.Exec(
		tarantool.Eval("__vshard_storage_init = require('vshard.storage.init')", []interface{}{}))
	if err != nil {
		log.Fatalf("could not init vshard storage %s\n%q", host, err)
	}

	log.Println(host + " connected!")
	return conn, err
}

func getBucketCount(conn *tarantool.Connection) (bucketCount int64) {
	cmd := "box.space._bucket:count" //"vshard.storage.buckets_count"
	rawBucketCount, err := conn.Exec(
		tarantool.Call(cmd, []interface{}{}))
	if err != nil {
		log.Fatalf("could not get %s\n%q", cmd, err)
	}

	bucketCount, ok := rawBucketCount[0].(int64)
	if !ok {
		_bucketCount, ok := rawBucketCount[0].(uint64)
		if !ok {
			log.Fatalf("could not cast rawBucketCount[0] to int64 and uint64")
		}
		bucketCount = int64(_bucketCount)
	}

	return bucketCount
}

func bootstrap(replicasetUuid string, conn *tarantool.Connection, firstBucketId int, etalonBucketCount int) {
	log.Printf("bootstrap replicaset %s firstBucketId=%d etalonBucketCount=%d", replicasetUuid, firstBucketId, etalonBucketCount)
	cmd := "__vshard_storage_init.bucket_force_create"
	result, err := conn.Exec(
		tarantool.Call(cmd, []interface{}{firstBucketId, etalonBucketCount}))
	if err != nil {
		log.Fatalf("fail to %s\n%q", cmd, err)
	}

	if !result[0].(bool) {
		log.Fatalf("fail to bootstrap replicaset %s", replicasetUuid)
	}
	log.Printf("replicaset %s bootstrapped!", replicasetUuid)
}

func clusterCalculateEtalonBalance(vshardCfg *VshardCfg) {
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
		if weightSum <= 0 { // assert(weight_sum > 0)
			log.Fatalf("assert(weight_sum > 0) but weight_sum = %f", weightSum)
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
		if bucketsRest != 0 { // assert(buckets_rest == 0)
			log.Fatalf("assert(buckets_rest == 0) but buckets_rest = %d", bucketsRest)
		}
		if stepCount > replicasetCount {
			// This can happed only because of a bug in this
			// algorithm. But it occupies 100% of transaction
			// thread, so check step count explicitly.
			log.Fatalf("the rebalancer is broken")
			return
		}
	}
}
