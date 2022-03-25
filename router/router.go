package router

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

type Index struct {
	ID   uint64
	Name string
}

type Space struct {
	ID      uint64
	Name    string
	Indexes []Index
}

type MasterInstance struct {
	Host   string
	UUID   string
	Conn   *tarantool.Connection
	Spaces map[uint64]Space
}

type Router struct {
	//Replicasets map[string]*tarantool.Connection
	Routes      sync.Map // bucketId uint64: conn *tarantool.Connection
	Groups      sync.Map // group string: bucketId []uint64
	Replicasets map[string]MasterInstance
	VshardCfg   VshardConfig
}

// console1> rm -rf tmp; cartridge start
// console2> cartridge replicasets setup --bootstrap-vshard
// console3> cartridge enter srv-2
// console3> function p1(a) local log = require('log') log.info("p1") log.info(a) end

// go to storage instance and call:
// vshard.storage.internal.current_cfg

func (r *Router) ReadConfigFile(configFilename string) error {
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

				conn, err := Connection(u.Host)
				if err != nil {
					return fmt.Errorf("could not connect to %s\n%v", u.Host, err)
				}

				var instance MasterInstance
				instance.Host = u.Host
				instance.UUID = replicasetUuid
				instance.Conn = conn
				instance.Spaces = make(map[uint64]Space)
				r.Replicasets[replicasetUuid] = instance

				log.Printf("append replicaset %s, master %s", replicasetUuid, u.Host)
				break
			}
		}
	}
	return nil
}

func Connection(host string) (conn *tarantool.Connection, err error) {
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

	_, err = conn.Exec(
		tarantool.Eval("__lua_api_vshard = require('cartridge.lua-api.vshard')", []interface{}{}))
	if err != nil {
		return nil, fmt.Errorf("could not load vshard api %s\n%v", host, err)
	}

	log.Println(host + " connected!")
	return conn, nil
}

func (r *Router) CloseConnections() {
	for replicasetUuid, instance := range r.Replicasets {
		instance.Conn.Close()
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

func (r *Router) Bootstrap() error {
	for replicasetUuid, instance := range r.Replicasets {
		log.Printf("get bucket.count from %s", replicasetUuid)
		bucketCount, err := getBucketCount(instance.Conn)
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
	for replicasetUuid, instance := range r.Replicasets {
		etalonBucketCount := r.VshardCfg.Sharding[replicasetUuid].EtalonBucketCount
		log.Printf("bootstrap replicaset %s firstBucketId=%d etalonBucketCount=%d", replicasetUuid, firstBucketId, etalonBucketCount)
		cmd := "__vshard_storage_init.bucket_force_create"
		result, err := instance.Conn.Exec(
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

func GetConfig(hostAddr string) (cfg []interface{}, err error) {
	conn, err := Connection(hostAddr)
	if err != nil {
		return nil, fmt.Errorf("could not connect to %s\n%v", hostAddr, err)
	}

	cmd := "__lua_api_vshard.get_config"
	cfg, err = conn.Exec(
		tarantool.Call(cmd, []interface{}{}))
	if err != nil {
		return cfg, fmt.Errorf("fail to load vshard cfg by calling %s\n%v", cmd, err)
	}

	return cfg, nil
}

type readBucketHook func(uint64, string, *tarantool.Connection)

func (r *Router) activeBucketHook(bucketId uint64, status string, conn *tarantool.Connection) {
	if status == "active" || status == "pinned" {
		r.Routes.Store(bucketId, conn)
	}
}

var mutex = sync.Mutex{}

func (r *Router) sortedBucketsHook(bucketId uint64, status string, conn *tarantool.Connection) {
	mutex.Lock()
	v, loaded := r.Groups.Load(status)
	var vector = []uint64{}
	if !loaded {
		vector = make([]uint64, 0)
	} else {
		vector = v.([]uint64)
	}
	vector = append(vector, bucketId)
	r.Groups.Store(status, vector)
	mutex.Unlock()
}

func (r *Router) readBuckets(conn *tarantool.Connection, wg *sync.WaitGroup, hook readBucketHook) {
	defer wg.Done()
	r.bulkSelect("_bucket", "pk", 0, 1000, conn, func(dataResponse []interface{}) uint64 {
		var lastId uint64 = 0
		for _, bucket := range dataResponse {
			bucketId, ok := bucket.([]interface{})[0].(uint64)
			if !ok {
				_bucketId, ok := bucket.([]interface{})[0].(int64)
				if !ok {
					log.Printf("could not cast bucketID to int64 and uint64")
					continue
				}
				bucketId = uint64(_bucketId)
			}
			status := bucket.([]interface{})[1].(string)
			//log.Printf("%d %s", bucketId, status)
			hook(bucketId, status, conn)
			lastId = bucketId
		}
		return lastId
	})
}

func (r *Router) discoveryBuckets(hook readBucketHook) {
	var wg sync.WaitGroup
	log.Println("start async tasks")
	for _, instance := range r.Replicasets {
		wg.Add(1)
		go r.readBuckets(instance.Conn, &wg, hook)
	}
	wg.Wait()
}

func (r *Router) CreateRoutesTable() {
	r.discoveryBuckets(r.activeBucketHook)
}

func (r *Router) CreateSortesBucketTable() {
	r.discoveryBuckets(r.sortedBucketsHook)
}

func (r *Router) CreateSpacesTable() {
	log.Println("sync  ")
	for replicasetUuid, instance := range r.Replicasets {
		log.Printf("%s %s %s", instance.Conn.NetConn().RemoteAddr().String(), instance.Host, replicasetUuid)
		for spaceName, space := range instance.Conn.Schema().Spaces {
			log.Printf("  space name = '%s'  id = %d\n", spaceName, space.ID)
			for indexName, index := range space.Indexes {
				log.Printf("    index = '%s'  id = %d\n", indexName, index.ID)
			}
		}
	}

	var wg sync.WaitGroup
	log.Println("start async tasks")
	for _, instance := range r.Replicasets {
		wg.Add(1)
		go r.readSpacesAndIndexes(instance, &wg)
	}
	wg.Wait()
}

type bulkSelectHook func([]interface{}) uint64

func (r *Router) bulkSelect(space, index interface{}, offset, limit uint32, conn *tarantool.Connection, hook bulkSelectHook) {
	var lastId uint64 = 0
	for bulk := 0; bulk < 777; bulk++ {
		resp, err := conn.Exec(
			tarantool.Select(space, index, offset, limit, tarantool.IterGt, []interface{}{lastId}))
		if err != nil {
			log.Printf("fail to select %s\n%v", space, err)
			break
		}
		if len(resp) == 0 {
			break
		}
		lastId = hook(resp)
	}
}

func (r *Router) readSpacesAndIndexes(instance MasterInstance, wg *sync.WaitGroup) {
	defer wg.Done()

	// see tarantool.loadSchema() 66
	r.bulkSelect("_space", 0, 0, 100, instance.Conn, func(dataResponse []interface{}) uint64 {
		var lastId uint64 = 0
		for _, row := range dataResponse {
			row := row.([]interface{})
			var space Space
			space.ID = row[0].(uint64)
			space.Name = row[2].(string)
			lastId = space.ID
			instance.Spaces[space.ID] = space
		}
		return lastId
	})

	// see tarantool.loadSchema() 121
	r.bulkSelect("_index", 0, 0, 100, instance.Conn, func(dataResponse []interface{}) uint64 {
		var lastId uint64 = 0
		for _, row := range dataResponse {
			row := row.([]interface{})
			var index Index
			index.ID = uint64(row[1].(int64))
			index.Name = row[2].(string)
			spaceID := row[0].(uint64)
			lastId = spaceID
			space, loaded := instance.Spaces[spaceID]
			if !loaded {
				log.Printf("fail to load space %d", spaceID)
			} else {
				space.Indexes = append(space.Indexes, index)
				instance.Spaces[spaceID] = space
			}
		}
		return lastId
	})
}
