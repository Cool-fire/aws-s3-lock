package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3rw struct {
	s3Client *s3.Client
	opts S3StoreOpts	
}

type S3StoreOpts struct {
	AwsBucketName string
	AwsLockFolder string
	LockName string
}

func (opts S3StoreOpts) validate() error {
	// TODO
	return nil
}

func NewS3Store(opts S3StoreOpts) (*S3rw ,error) {
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
	bucketKey := fmt.Sprintf("%s%s-owner.json", s.opts.AwsLockFolder, s.opts.LockName)
	getLockOwnerObject := &s3.GetObjectInput{
		Bucket: aws.String(s.opts.AwsBucketName),
		Key: &bucketKey,
	}

	output, err := s.s3Client.GetObject(context.TODO(), getLockOwnerObject)

	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil
		}
		return nil, fmt.Errorf("error getting the lock owner")
	}

	body, err := io.ReadAll(output.Body)
	defer output.Body.Close()


	if err != nil {
		return nil, fmt.Errorf("error reading the lock owner file")
	}

	var lockOwner LockOwner
	err = json.Unmarshal(body, &lockOwner)
	if err != nil {
		return nil, fmt.Errorf("error reading the owner file")
	}

	return &lockOwner, nil
}

func (s *S3rw) GetLockCounter() (*LockCounter, error) {
	bucketKey := fmt.Sprintf("%s%s-counter.json", s.opts.AwsLockFolder, s.opts.LockName)
	getLockOwnerObject := &s3.GetObjectInput{
		Bucket: aws.String(s.opts.AwsBucketName),
		Key: &bucketKey,
	}

	output, err := s.s3Client.GetObject(context.TODO(), getLockOwnerObject)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil 
		}
		return nil, fmt.Errorf("error getting the lock counter")
	}

	body, err := io.ReadAll(output.Body)
	defer output.Body.Close()

	if err != nil {
		return nil, fmt.Errorf("error reading the lock counter file")
	}
	
	b := string(body)
	if b == "" {
		return nil, fmt.Errorf("error reading the lock counter file")
	}

	c, e := strconv.Atoi(b); 
	if e != nil{
		return nil, fmt.Errorf("error reading the lock counter file")
	}

	return &LockCounter{
		counter: c,
	}, nil
}

func (s *S3rw) SetLockCounter(c LockCounter) error {	
	contents := strconv.Itoa(c.counter)
	bucketKey := fmt.Sprintf("%s%s-counter.json", s.opts.AwsLockFolder, s.opts.LockName)
	putObjectRequest := &s3.PutObjectInput{
		Bucket: aws.String(s.opts.AwsBucketName),
		Key: aws.String(bucketKey),
		Body: strings.NewReader(contents),
	}
	
	_, err := s.s3Client.PutObject(context.TODO(), putObjectRequest)

	if err != nil {
		return fmt.Errorf("error setting the lock counter %d", c.counter)
	}
	return nil
}

func (s *S3rw) SetLockOwner(owner LockOwner) error {
	jsonData, err := json.Marshal(owner)
	if err != nil {
		return fmt.Errorf("unable to set lock owner, Error marshalling")
	}

	bucketKey := fmt.Sprintf("%s%s-owner.json", s.opts.AwsLockFolder, s.opts.LockName)
	putObjectRequest := &s3.PutObjectInput{
		Bucket: aws.String(s.opts.AwsBucketName),
		Key: aws.String(bucketKey),
		Body: bytes.NewReader(jsonData),
	}

	_, err = s.s3Client.PutObject(context.TODO(), putObjectRequest)
	if err != nil {
		return fmt.Errorf("error setting the lock owner %s", owner.Name)
	}

	return nil
}