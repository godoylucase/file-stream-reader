package infrastructure

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Storage struct {
	client *s3.S3
}

type FileData struct {
	Bucket string
	Key    string
}

type ContentLengthQuery struct {
	FileData
}

type BytesRangeCmd struct {
	FileData
	From  int64
	To    int64
	Chunk []byte
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

func (s *Storage) ContentLength(query *ContentLengthQuery) (int64, error) {
	head, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(query.Bucket),
		Key:    aws.String(query.Key),
	})
	if err != nil {
		return 0, err
	}

	return *head.ContentLength, nil
}

func (s *Storage) BytesRange(cmd *BytesRangeCmd) error {
	byteRange := fmt.Sprintf("bytes=%v-%v", cmd.From, cmd.To)

	object, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(cmd.Bucket),
		Key:    aws.String(cmd.Key),
		Range:  aws.String(byteRange),
	})
	if err != nil {
		return err
	}

	defer object.Body.Close()
	if _, err := object.Body.Read(cmd.Chunk); err != nil {
		return err
	}

	return nil
}
