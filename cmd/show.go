/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "A brief description of your command",
	Long:  ` `,
	Run: func(cmd *cobra.Command, args []string) {
		cfg_file, err := cmd.Flags().GetString("cfg-file")
		if err != nil {
			log.Fatalf("could not load param instance-addr\n%v", err)
		}
		fmt.Printf("show\ncfg file = %s", cfg_file)
	},
}

func init() {
	rootCmd.AddCommand(showCmd)
	showCmd.Flags().StringP("cfg-file", "f", "/tmp/vshard_cfg.yaml",
		"use this param to load vshard cfg file")
}
