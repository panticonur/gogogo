/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"
	"log"
	"tarapower/router"

	"github.com/spf13/cobra"
)

// spacesCmd represents the spaces command
var spacesCmd = &cobra.Command{
	Use:   "spaces",
	Short: "A brief description of your command",
	Long: `
- зайти на стораджа:
	- получить список всех спейсов
		- conn.select(  "_space", ......  )
	- получить какие индексы в этих спейсах
		- conn.select( "_index", ..... )
	- вывести в виде статистики`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("spaces called")
		showSpaces()
	},
}

func init() {
	showCmd.AddCommand(spacesCmd)
}

func showSpaces() {
	r := router.Router{
		//Replicasets: make(map[string]*tarantool.Connection),
		Replicasets: make(map[string]router.Instance),
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

	/*
		r.CreateSpacesTable_sync()
		log.Println("\n\nSpaces Table:")
		r.Spaces.Range(func(i, s interface{}) bool {
			spaceID := i.(uint64)
			space := s.(router.Space)
			log.Printf("  space name = '%s'  id = %d\n", space.Name, spaceID)
			for _, index := range space.Indexes {
				log.Printf("    index = '%s'  id = %d\n", index.Name, index.ID)
			}
			return true
		})
	*/
	r.CreateSpacesTable()
	log.Println("\n\nSpaces Table:")
	for uuid, instance := range r.Replicasets {
		log.Printf("instance name = '%s'  host = %s\n", uuid, instance.Host)
		for spaceID, space := range instance.Spaces {
			log.Printf("  space name = '%s'  id = %d\n", space.Name, spaceID)
			for _, index := range space.Indexes {
				log.Printf("    index = '%s'  id = %d\n", index.Name, index.ID)
			}
		}
	}
}
