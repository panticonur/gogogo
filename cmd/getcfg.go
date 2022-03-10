/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"log"
	"tarapower/router"

	"github.com/davecgh/go-spew/spew"
	"github.com/spf13/cobra"
)

// getcfgCmd represents the getcfg command
var getcfgCmd = &cobra.Command{
	Use:   "getcfg",
	Short: "get config from cartridge vshard",
	Long: `first need to bootstrap vshard:
		cartridge replicasets setup --bootstrap-vshard`,
	Run: func(cmd *cobra.Command, args []string) {
		tarantool_addr, err := cmd.Flags().GetString("address")
		if err != nil {
			log.Fatalf("could not load param 'address'\n%v", err)
		}
		getCfg(tarantool_addr)
	},
}

func init() {
	rootCmd.AddCommand(getcfgCmd)
	getcfgCmd.Flags().StringP("address", "a", "127.0.0.1:3301",
		"use this param to connect to cartridge instance")
}

func getCfg(tarantoolAddr string) {
	cfg, err := router.GetConfig(tarantoolAddr)
	if err != nil {
		log.Fatalf("get config fail\n%v", err)
	}
	spew.Dump(cfg)
}
