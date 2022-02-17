package awss3

import (
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Proxy struct {
	client *s3.S3
}

func NewProxy() (*s3Proxy, error) {
	client, err := Client()
	if err != nil {
		return nil, err
	}

	return &s3Proxy{client: client}, nil
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
