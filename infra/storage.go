package infra

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

type Storage struct {
	client *s3.S3
}

func NewStorage() (*Storage, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String("us-east-1"),
		},
	})

	if err != nil {
		fmt.Printf("Failed to initialize new session: %v", err)
		return nil, err
	}

	return &Storage{client: s3.New(sess)}, nil
}

func (s *Storage) ContentLength(filename string) (int64, error) {
	bucket, key := s.buckedKeyValues(filename)

	head, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}

	return *head.ContentLength, nil
}

func (s *Storage) ByteRange(filename string, from, to int64, chunk []byte) error {
	bucket, key := s.buckedKeyValues(filename)
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
	if _, err := object.Body.Read(chunk); err != nil {
		return err
	}

	return nil
}

func (s *Storage) buckedKeyValues(filename string) (string, string) {
	parts := strings.Split(filename, "/")
	bucket := parts[0]
	key := fmt.Sprintf("/%v", strings.Join(parts, "/"))
	return bucket, key
}
