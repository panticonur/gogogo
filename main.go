package main

import (
	"log"

	"github.com/spf13/cobra"

	"tarapower/router"

	"github.com/FZambia/tarantool"
	"github.com/davecgh/go-spew/spew"
)

func run(command *cobra.Command, args []string) {
	/*
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
	*/

	tarantool_addr, err := command.Flags().GetString("tarantool-addr")
	if err != nil {
		log.Fatalf("could not load param tarantool-addr\n%v", err)
	}
	conn, err := router.Connection(tarantool_addr)
	if err != nil {
		log.Fatalf("connection fail\n%v", err)
	}
	cmd := "cartridge_vshard_get_config"
	cfg, err := conn.Exec(
		tarantool.Call(cmd, []interface{}{}))
	if err != nil {
		log.Fatalf("fail to load vshard cfg %s\n%v", cmd, err)
	}
	spew.Dump(cfg)

	/*
		if err := router.Bootstrap(); err != nil {
			log.Fatalf("bootstap error\n%v", err)
		}

		router.DiscoveryBuckets()

		var bucketId uint64 = 1
		for ; bucketId <= uint64(router.VshardCfg.BucketCount); bucketId += 500 {
			proc := "p1"
			if _, err := router.RPC(bucketId, proc, []interface{}{101}); err != nil {
				log.Printf("could not call remote proc '%s'\n%v", proc, err)
			}
		}
	*/

}

func main() {
	var rootCmd = &cobra.Command{Use: "tarapower", Run: run}
	rootCmd.Flags().StringP("tarantool-addr", "t", "127.0.0.1:3301", "use this param to connect to tarapower")
	rootCmd.Execute()

}
