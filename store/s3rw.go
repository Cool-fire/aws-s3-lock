package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3rw struct {
	s3Client *s3.Client
	opts S3StoreOpts	
}

type S3StoreOpts struct {
	awsBucketName string
	awsLockFolder string
	lockName string
}

func (opts S3StoreOpts) validate() error {
	// TODO
	return nil
}

func New(opts S3StoreOpts) (*S3rw ,error) {
	cfg, err := config.LoadDefaultConfig(context.TODO()) 
	if err != nil {
		return nil, fmt.Errorf("error loading s3 config %w", err)
	}

	client := s3.NewFromConfig(cfg)

	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("invalid S3 store options: %w", err)
	}

	return &S3rw{
		s3Client: client,
		opts: opts,
	}, nil
}

func (s *S3rw) GetLockOwner() (*LockOwner, error) {
	// TODO
	return nil, errors.New("method not implemented")
}

func (s *S3rw) GetLockCounter() (int, error) {
	return -1 , errors.New("method not implemented")
}

func (s *S3rw) SetLockCounter(counter int) error {	
	contents := strconv.Itoa(counter)
	bucketKey := fmt.Sprintf("%s%s-counter.json", s.opts.awsLockFolder, s.opts.lockName)
	putObjectRequest := &s3.PutObjectInput{
		Bucket: aws.String(s.opts.awsBucketName),
		Key: aws.String(bucketKey),
		Body: strings.NewReader(contents),
	}
	
	_, err := s.s3Client.PutObject(context.TODO(), putObjectRequest)

	if err != nil {
		return fmt.Errorf("error setting the lock counter %d", counter)
	}
	return nil
}

func (s *S3rw) SetLockOwner(owner LockOwner) error {
	jsonData, err := json.Marshal(owner)
	if err != nil {
		return fmt.Errorf("unable to set lock owner, Error marshalling")
	}

	bucketKey := fmt.Sprintf("%s%s-owner.json", s.opts.awsLockFolder, s.opts.lockName)
	putObjectRequest := &s3.PutObjectInput{
		Bucket: aws.String(s.opts.awsBucketName),
		Key: aws.String(bucketKey),
		Body: bytes.NewReader(jsonData),
	}

	_, err = s.s3Client.PutObject(context.TODO(), putObjectRequest)
	if err != nil {
		return fmt.Errorf("error setting the lock owner %s", owner.Name)
	}

	return nil
}