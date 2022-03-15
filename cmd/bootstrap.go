/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"log"
	"tarapower/router"

	"github.com/FZambia/tarantool"
	"github.com/spf13/cobra"
)

// bootstrapCmd represents the bootstrap command
var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap [config_source]",
	Short: "bootstrap vshard",
	Long:  ` `,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		//spew.Dump(args)
		cfg_file, err := cmd.Flags().GetString("cfg-file")
		if err != nil {
			log.Fatalf("could not load param instance-addr\n%v", err)
		}

		switch args[0] {
		case "file":
			bootstrapFromFile(cfg_file)
		case "remote":
			log.Fatalf("remote not implemented")
		default:
			log.Fatalf("valid arguments: file, remote")
		}
	},
}

func init() {
	rootCmd.AddCommand(bootstrapCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// bootstrapCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	bootstrapCmd.Flags().StringP("cfg-file", "f", "/tmp/vshard_cfg.yaml",
		"use this param to load vshard cfg file")
	bootstrapCmd.SetArgs([]string{"arg1", "arg2"})
}

func bootstrapFromFile(configFile string) {
	r := router.Router{
		Replicasets: make(map[string]*tarantool.Connection),
	}

	if err := r.ReadConfigFile(configFile); err != nil {
		log.Fatalf("error reading '%s' vshard config\n%v", configFile, err)
	}
	if err := r.ConnectMasterInstancies(); err != nil {
		log.Fatalf("connection error\n%v", err)
	}
	defer r.CloseConnections()

	if err := r.Bootstrap(); err != nil {
		log.Fatalf("bootstap error\n%v", err)
	}

	r.CreateRoutesTable()

	r.Routes.Range(func(b, c interface{}) bool {
		bucketId := b.(uint64)
		conn := c.(*tarantool.Connection)
		log.Printf("%d %p", bucketId, conn)
		return true
	})
	/*
		var bucketId uint64 = 1
		for ; bucketId <= uint64(router.VshardCfg.BucketCount); bucketId += 500 {
			proc := "p1"
			if _, err := router.RPC(bucketId, proc, []interface{}{101}); err != nil {
				log.Printf("could not call remote proc '%s'\n%v", proc, err)
			}
		}
	*/
}
