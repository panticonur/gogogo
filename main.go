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
	Weight            float64
	Replicas          map[string]Replica
	EtalonBucketCount int
	IgnoreDisbalance  bool
	PinnedCount       int
}

type Replica struct {
	Master bool
	Uri    string
	Name   string
}

type VshardCfg struct {
	RebalancerMaxReceiving        int
	BucketCount                   int
	CollectLuaGarbage             bool
	SyncTimeout                   int
	ReadOnly                      bool
	SchedRefQuota                 int
	RebalancerDisbalanceThreshold int
	RebalancerMaxSending          int
	SchedMoveQuota                int
	Sharding                      map[string]Replicaset
}

var cfgFilename = "/tmp/vshard_cfg.yaml"

func main() {
	instances := map[string]*tarantool.Connection{}

	log.Printf("read yaml vshard cfg %s", cfgFilename)
	yamlFile, err := ioutil.ReadFile(cfgFilename)
	if err != nil {
		log.Fatalf("Reading yamlFile.Get err  %s\n%q", cfgFilename, err)
	}

	vshard_cfg_data := VshardCfg{}
	err = yaml.Unmarshal(yamlFile, &vshard_cfg_data)
	if err != nil {
		log.Fatalf("Unmarshal error %q", err)
	}

	for uuid, replicaset := range vshard_cfg_data.Sharding {
		for _, replica := range replicaset.Replicas {
			if replica.Master {
				u, err := url.Parse("tarantool://" + replica.Uri)
				if err != nil {
					log.Fatalf("Could not parse URI %s\n%q", replica.Uri, err)
				}

				conn, err := connection(u.Host)
				if err != nil {
					log.Fatalf("Could connect to %s\n%q", u.Host, err)
				}
				defer conn.Close()
				instances[uuid] = conn // append

				bucket_count := get_bucket_count(conn)
				if bucket_count > 0 {
					log.Fatalf("Storage %s is already bootstrapped!", u.Host)
				}
				break
			}
		}
	}

	clusterCalculateEtalonBalance(&vshard_cfg_data)
	spew.Dump(vshard_cfg_data)

	var first_bucket_id int = 1
	for uuid, conn := range instances {
		bootstrap(uuid, conn, first_bucket_id, vshard_cfg_data.Sharding[uuid].EtalonBucketCount)
		first_bucket_id = first_bucket_id + vshard_cfg_data.Sharding[uuid].EtalonBucketCount
	}

}

func connection(host string) (conn *tarantool.Connection, err error) {
	log.Print("\n\n")
	log.Println("Connecting " + host)
	opts := tarantool.Opts{
		RequestTimeout: 500 * time.Millisecond,
		User:           "admin",
	}

	conn, err = tarantool.Connect(host, opts)
	if err != nil {
		log.Fatalf("Connection %s refused: %v", host, err)
	}

	_, err = conn.Exec(
		tarantool.Eval("__vshard_storage_init = require('vshard.storage.init')", []interface{}{}))
	if err != nil {
		log.Fatalf("Could not init vshard storage %s\n%q", host, err)
	}

	log.Println(host + " connected!")
	return conn, err
}

func get_bucket_count(conn *tarantool.Connection) (bucket_count int64) {
	log.Print("\n\n")
	cmd := "box.space._bucket:count" //"vshard.storage.buckets_count"
	log.Printf("Call( " + cmd + " )")
	raw_bucket_count, err := conn.Exec(
		tarantool.Call(cmd, []interface{}{}))
	if err != nil {
		log.Fatalf("Could not get %s %q", cmd, err)
	}
	log.Println("raw_bucket_count = ", raw_bucket_count)

	bucket_count, ok := raw_bucket_count[0].(int64)
	if !ok {
		log.Println("bucket_count not int64")
		bucket_count_u, ok := raw_bucket_count[0].(uint64)
		if !ok {
			log.Fatalf("bucket_count not uint64")
		}
		bucket_count = int64(bucket_count_u)
	}
	log.Println("bucket_count = ", bucket_count)

	return bucket_count
}

func bootstrap(host string, conn *tarantool.Connection, first_bucket_id int, etalon_bucket_count int) {
	log.Print("\n\n")
	log.Printf("Bootstrap " + host)
	cmd := "__vshard_storage_init.bucket_force_create"
	result, err := conn.Exec(
		tarantool.Call(cmd, []interface{}{first_bucket_id, etalon_bucket_count}))
	if err != nil {
		log.Fatalf("Fail to %s on %s\n%q", cmd, host, err)
	}
	log.Println(result)
}

func clusterCalculateEtalonBalance(vshard_cfg *VshardCfg) {
	log.Print("\n\n")
	log.Println("calc etalon balance")
	replicasets := vshard_cfg.Sharding
	bucket_count := vshard_cfg.BucketCount
	is_balance_found := false
	var weight_sum float64 = 0
	step_count := 0
	replicaset_count := 0

	for _, replicaset := range replicasets {
		weight_sum = weight_sum + replicaset.Weight
		replicaset_count = replicaset_count + 1
	}

	for !is_balance_found {
		step_count = step_count + 1
		if weight_sum <= 0 { // assert(weight_sum > 0)
			log.Fatalf("assert(weight_sum > 0) but weight_sum = %f", weight_sum)
		}
		var bucket_per_weight float64 = float64(bucket_count) / weight_sum
		buckets_calculated := 0
		for k, replicaset := range replicasets {
			if !replicaset.IgnoreDisbalance {
				replicaset.EtalonBucketCount = int(math.Ceil(replicaset.Weight * bucket_per_weight))
				buckets_calculated = buckets_calculated + replicaset.EtalonBucketCount
			}
			replicasets[k] = replicaset
		}

		buckets_rest := buckets_calculated - bucket_count
		is_balance_found = true
		for k, replicaset := range replicasets {
			if !replicaset.IgnoreDisbalance {
				// A situation is possible, when bucket_per_weight
				// is not integer. Lets spread this disbalance
				// over the cluster.
				if buckets_rest > 0 {
					n := replicaset.Weight * bucket_per_weight
					ceil := math.Ceil(n)
					floor := math.Floor(n)
					if replicaset.EtalonBucketCount > 0 && ceil != floor {
						replicaset.EtalonBucketCount = replicaset.EtalonBucketCount - 1
						buckets_rest = buckets_rest - 1
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
					is_balance_found = false
					bucket_count = bucket_count - replicaset.PinnedCount
					replicaset.EtalonBucketCount = replicaset.PinnedCount
					replicaset.IgnoreDisbalance = true
					weight_sum = weight_sum - replicaset.Weight
				}
			}
			replicasets[k] = replicaset
		}
		if buckets_rest != 0 { // assert(buckets_rest == 0)
			log.Fatalf("assert(buckets_rest == 0) but buckets_rest = %d", buckets_rest)
		}
		if step_count > replicaset_count {
			// This can happed only because of a bug in this
			// algorithm. But it occupies 100% of transaction
			// thread, so check step count explicitly.
			log.Fatalf("PANIC: the rebalancer is broken")
			return
		}
	}
}
