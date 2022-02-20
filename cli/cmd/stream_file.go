package cmd

import (
	"context"
	"fmt"

	"github.com/godoylucase/s3-file-stream-reader/internal/orch"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
)

// streamFileCmd represents the streamFile command
var streamFileCmd = streamFile()

func streamFile() *cobra.Command {
	return &cobra.Command{
		Use:   "streamFile",
		Short: "",
		Run:   commandFn(),
	}
}

func commandFn() func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var sfargs orch.Config
		if err := mapstructure.Decode(argsMap(args), &sfargs); err != nil {
			fmt.Println(err)
			return
		}

		or, err := orch.FromConfig(&sfargs)
		if err != nil {
			fmt.Println(err)
		}

		for d := range or.Run(ctx) {
			fmt.Printf("read data from filestream with values: %+v\n", d)
		}
	}
}

func init() {
	rootCmd.AddCommand(streamFileCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// streamFileCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// streamFileCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
