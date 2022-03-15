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

// scanBucketsCmd represents the scanBuckets command
var scanBucketsCmd = &cobra.Command{
	Use:   "scanBuckets",
	Short: "A brief description of your command",
	Long: `- зайти на все стораджа:
	- посчитать все бакеты
	- рассортировать бакеты по статусам
	- вывести пользователю`,
	Run: func(cmd *cobra.Command, args []string) {
		scanBuckets()
	},
}

func init() {
	rootCmd.AddCommand(scanBucketsCmd)
}

func scanBuckets() {
	r := router.Router{
		Replicasets: make(map[string]*tarantool.Connection),
	}
	var configFile = "/tmp/vshard_cfg.yaml"
	if err := r.ReadConfigFile(configFile); err != nil {
		log.Fatalf("error reading '%s' vshard config\n%v", configFile, err)
	}
	if err := r.ConnectMasterInstancies(); err != nil {
		log.Fatalf("connection error\n%v", err)
	}
	defer r.CloseConnections()

	//if err := r.Bootstrap(); err != nil {
	//	log.Fatalf("scan error\n%v", err)
	//}

	r.CreateSortesBucketTable()

	r.Groups.Range(func(b, g interface{}) bool {
		bucketId := b.(uint64)
		group := g.(string)
		log.Printf("%d %s", bucketId, group)
		return true
	})
}
