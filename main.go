package main

import (
	"fmt"
	"s3test/s3copier"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var copier *s3copier.S3Copier
var s3client *s3.S3

func init() {
	session := session.New(&aws.Config{
		Region: aws.String("ap-northeast-1"),
	})
	s3client = s3.New(session)
	copier = s3copier.NewS3Copier(session)
}

func main() {
	s := time.Now()
	err := copier.CopyWithPrefix("test-from-bucket", "test-to-bucket", "prefix/001")
	if err != nil {
		panic(err)
	}
	duration := time.Now().Sub(s)
	fmt.Printf("duration: %v\n", duration)
}
