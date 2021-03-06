package fsource

import (
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/godoylucase/s3-file-stream-reader/internal/platform"
)

type s3Proxy struct {
	client *s3.S3
}

func newS3() (*s3Proxy, error) {
	client, err := platform.Client()
	if err != nil {
		return nil, err
	}

	return &s3Proxy{client: client}, nil
}

func (s *s3Proxy) Length(filename string) (int64, error) {
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

func (s *s3Proxy) GetBytes(filename string, from int64, chunk []byte) error {
	bucket, key := s.bucketKeyValues(filename)
	byteRange := fmt.Sprintf("bytes=%v-%v", from, from+int64(len(chunk)))

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
