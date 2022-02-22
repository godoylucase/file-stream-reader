package fsource

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func Client() (*s3.S3, error) {
	config := aws.Config{
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}

	ae, ok := os.LookupEnv("AWS_ENDPOINT")
	if ok {
		config.Credentials = credentials.NewStaticCredentials("test", "test", "")
		config.Endpoint = aws.String(ae)
	}

	sess, err := session.NewSessionWithOptions(session.Options{Config: config})

	if err != nil {
		fmt.Printf("Failed to initialize new session: %v", err)
		return nil, err
	}

	return s3.New(sess), nil
}
