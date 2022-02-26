package cmd

import (
	"context"
	"fmt"
	"strconv"

	"github.com/godoylucase/s3-file-stream-reader/internal/fsr/orchrestation"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
)

// streamFileCmd represents the streamFile command
var streamFileCmd = streamFile()

type arguments struct {
	Type             string                 `mapstructure:"type"`
	ChunkByteSize    string                 `mapstructure:"chunk-size"`
	OnReadFnName     string                 `mapstructure:"reader-fn"`
	StreamersQty     string                 `mapstructure:"streamers"`
	ReadersQty       string                 `mapstructure:"readers"`
	LocationMetadata map[string]interface{} `mapstructure:",remain"`
}

func streamFile() *cobra.Command {
	return &cobra.Command{
		Use:   "streamFile",
		Short: "",
		Run:   commandFn(),
	}
}

func commandFn() func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		var sfargs arguments
		if err := mapstructure.Decode(argsMap(args), &sfargs); err != nil {
			fmt.Println(err)
			return
		}

		sqty, err := strconv.Atoi(sfargs.StreamersQty)
		if err != nil {
			fmt.Println(err)
			return
		}

		rqty, err := strconv.Atoi(sfargs.ReadersQty)
		if err != nil {
			fmt.Println(err)
			return
		}

		cbs, err := strconv.Atoi(sfargs.ChunkByteSize)
		if err != nil {
			fmt.Println(err)
			return
		}

		or, err := orchrestation.FromConfig(&orchrestation.Config{
			Type: sfargs.Type,
			StreamConf: &orchrestation.StreamConf{
				ChunkSize:        int64(cbs),
				Qty:              uint(sqty),
				LocationMetadata: sfargs.LocationMetadata,
			},
			ReadConf: &orchrestation.ReadConf{
				Qty:          uint(rqty),
				OnReadFnName: sfargs.OnReadFnName},
		})
		if err != nil {
			fmt.Println(err)
		}

		for d := range or.Run(ctx) {
			fmt.Printf("read data from fstream with values: %+v\n", d)
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
