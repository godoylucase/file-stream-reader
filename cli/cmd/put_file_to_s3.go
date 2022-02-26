package cmd

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/godoylucase/file-stream-reader/internal/platform"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
)

type PutArgs struct {
	Path   string `mapstructure:"from"`
	Bucket string `mapstructure:"bucket"`
	Key    string `mapstructure:"key"`
}

// pushToS3Cmd represents the pushToS3 command
var pushToS3Cmd = &cobra.Command{
	Use:   "pushToS3",
	Short: "Puts a file into the specified bucket",
	Run: func(cmd *cobra.Command, args []string) {
		var pargs PutArgs
		err := mapstructure.Decode(argsMap(args), &pargs)
		if err != nil {
			fmt.Println(err)
			return
		}

		s3cli, err := platform.Client()
		if err != nil {
			fmt.Println(err)
			return
		}

		if _, err := s3cli.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(pargs.Bucket),
		}); err != nil {
			fmt.Printf("creating %v bucket\n", pargs.Bucket)

			if _, err := s3cli.CreateBucket(&s3.CreateBucketInput{
				Bucket: aws.String(pargs.Bucket),
			}); err != nil {
				fmt.Printf("error creating the bucket with name %v\n with error %+v\n", pargs.Bucket, err)
				return
			}
		}

		content, err := ioutil.ReadFile(pargs.Path)
		if err != nil {
			fmt.Println(err)
			return
		}

		if _, err = s3cli.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(pargs.Bucket),
			Key:    aws.String(pargs.Key),
			Body:   bytes.NewReader(content),
		}); err != nil {
			fmt.Println(err)
			return
		}

		return
	},
}

func init() {
	rootCmd.AddCommand(pushToS3Cmd)
}
