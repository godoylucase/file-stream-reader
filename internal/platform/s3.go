package platform

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Proxy struct {
	client *s3.S3
}

func NewS3Proxy() (*s3Proxy, error) {
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

	return &s3Proxy{client: s3.New(sess)}, nil
}

func (s *s3Proxy) ContentLength(filename string) (int64, error) {
	bucket, key := s.bucketKeyValues(filename)

	head, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}

	return *head.ContentLength, nil
}

func (s *s3Proxy) ByteRange(filename string, from, to int64, chunk []byte) error {
	bucket, key := s.bucketKeyValues(filename)
	byteRange := fmt.Sprintf("bytes=%v-%v", from, to)

	object, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(byteRange),
	})
	if err != nil {
		return err
	}

	defer object.Body.Close()
	if n, err := object.Body.Read(chunk); n == 0 && err == io.EOF {
		return err
	}

	return nil
}

func (s *s3Proxy) bucketKeyValues(filename string) (string, string) {
	parts := strings.Split(filename, "/")
	bucket := parts[0]
	key := fmt.Sprintf("/%v", strings.Join(parts[1:], "/"))
	return bucket, key
}
