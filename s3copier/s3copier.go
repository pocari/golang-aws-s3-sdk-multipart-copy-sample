package s3copier

import (
	"fmt"
	"math"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// https://github.com/aws/aws-sdk-ruby/blob/97b28ccf18558fc908fd56f52741cf3329de9869/gems/aws-sdk-s3/lib/aws-sdk-s3/object_multipart_copier.rb

const (
	FIVE_MB      = 5 * 1024 * 1024
	WORKER_COUNT = 50
)

type S3Object struct {
	bucket string
	key    string
}

func (s *S3Object) bucketKeyPath() string {
	return fmt.Sprintf("%s/%s", s.bucket, s.key)
}

type S3Copier struct {
	partSize int64
	s3client *s3.S3
}

func NewS3Copier(sess *session.Session) *S3Copier {
	return &S3Copier{
		s3client: s3.New(sess),
		// rubyのsdkは 50Mだったのでそれぐらいで良さそう。一旦分割されるケースをみるために小さめで
		partSize: FIVE_MB * 2,
	}
}

func (c *S3Copier) runWorker(workerId int, srcBucket, destBucket string, jobs <-chan string, done chan<- string, statusChan chan<- error) {
	for key := range jobs {
		src := &S3Object{bucket: srcBucket, key: key}
		dest := &S3Object{bucket: destBucket, key: key}
		err := c.CopyTo(src, dest)
		if err != nil {
			statusChan <- err
			return
		}
		done <- key
	}
}

func (c *S3Copier) CopyWithPrefix(srcBucket, destBucket, prefix string) error {
	count := 0
	jobs := make(chan string, 20000)
	done := make(chan string, 20000)
	statusChan := make(chan error, 1)

	for w := 0; w < WORKER_COUNT; w++ {
		go c.runWorker(w, srcBucket, destBucket, jobs, done, statusChan)
	}

	c.s3client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(srcBucket),
		Prefix: aws.String(prefix),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, c := range page.Contents {
			k := *c.Key
			// log.Infof("ListObjectsV2Output: add key to jobs: %s\n", k)
			jobs <- k
			count++
		}
		return true
	})

	check := 0
	for {
		select {
		case key := <-done:
			fmt.Printf("%s copied.\n", key)
			check++
		case err := <-statusChan:
			fmt.Printf("raise error: %v\n", err)
			// 途中でエラー発生
			return err
		}
		if check >= count {
			// 正常
			return nil
		}
	}
}

func (c *S3Copier) CopyTo(src *S3Object, dest *S3Object) error {
	head, err := c.headObject(src)
	if err != nil {
		return err
	}

	if strings.HasSuffix(src.key, ".m3u8") {
		if err := c.ensureContentTypeM3u8(src); err != nil {
			return err
		}
		// log.Infof("CopyTo: ContentTypeUpdated %v\n", src)
	}

	objectSize := *head.ContentLength
	if objectSize <= FIVE_MB {
		return c.copyToSinglePart(src, dest)
	} else {
		return c.copyToMultiPart(src, dest)
	}
}

func (c *S3Copier) ensureContentTypeM3u8(src *S3Object) error {
	_, err := c.s3client.CopyObject(&s3.CopyObjectInput{
		Bucket:            aws.String(src.bucket),
		Key:               aws.String(src.key),
		ContentType:       aws.String("application/vnd.apple.mpegurl"),
		CopySource:        aws.String(src.bucketKeyPath()),
		MetadataDirective: aws.String(s3.MetadataDirectiveReplace),
	})
	return err
}

func (c *S3Copier) copyToSinglePart(src *S3Object, dest *S3Object) error {
	_, err := c.s3client.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(dest.bucket),
		Key:        aws.String(dest.key),
		CopySource: aws.String(src.bucketKeyPath()),
	})
	// log.Infof("copyToSinglePart:%v -> %v, err: %v\n", src, dest, err)
	return err
}

func (c *S3Copier) copyToMultiPart(src *S3Object, dest *S3Object) error {
	head, err := c.headObject(src)
	if err != nil {
		return err
	}

	multipartUploadInit, err := c.createMultiPartUpload(dest, head)
	if err != nil {
		return err
	}

	objectSize := *head.ContentLength
	// log.Infof("copyToMultiPart:from % objectSize: %v\n", src, objectSize)
	bytePosition := int64(0)
	partNum := int64(1)
	partsSize := int(math.Ceil(float64(objectSize) / float64(c.partSize)))
	// log.Infof("copyToMultiPart:partSize %v\n", partsSize)
	completedParts := make([]*s3.CompletedPart, partsSize)

	for bytePosition < objectSize {
		lastByte := bytePosition + c.partSize - 1
		if lastByte > objectSize-1 {
			lastByte = objectSize - 1
		}

		partResult, err := c.uploadPartCopy(
			partNum,
			src,
			dest,
			bytePosition,
			lastByte,
			multipartUploadInit.UploadId,
		)
		if err != nil {
			return nil
		}

		etag := *partResult.CopyPartResult.ETag
		completedParts[partNum-1] = &s3.CompletedPart{
			ETag:       aws.String(etag[1 : len(etag)-1]), // escape duble quote
			PartNumber: aws.Int64(partNum),
		}
		partNum++
		bytePosition += c.partSize
	}
	// ここまでで分割したやつの処理終わり

	_, err = c.s3client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(dest.bucket),
		Key:    aws.String(dest.key),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
		UploadId: multipartUploadInit.UploadId,
	})

	// log.Infof("copyToMultiPart:%v -> %v, err: %v\n", src, dest, err)
	if err != nil {
		return err
	}
	return nil
}

func (c *S3Copier) uploadPartCopy(partNum int64, src *S3Object, dest *S3Object, bytePosition int64, lastByte int64, uploadId *string) (*s3.UploadPartCopyOutput, error) {
	return c.s3client.UploadPartCopy(&s3.UploadPartCopyInput{
		Bucket:          aws.String(dest.bucket),
		CopySource:      aws.String(src.bucketKeyPath()),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", bytePosition, lastByte)),
		Key:             aws.String(dest.key),
		PartNumber:      aws.Int64(partNum),
		UploadId:        uploadId,
	})
}

func (c *S3Copier) headObject(obj *S3Object) (*s3.HeadObjectOutput, error) {
	head, err := c.s3client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(obj.bucket),
		Key:    aws.String(obj.key),
	})
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (c *S3Copier) createMultiPartUpload(dest *S3Object, srcHead *s3.HeadObjectOutput) (*s3.CreateMultipartUploadOutput, error) {
	multiUploadInit, err := c.s3client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:      aws.String(dest.bucket),
		Key:         aws.String(dest.key),
		ContentType: srcHead.ContentType,
		Metadata:    srcHead.Metadata,
	})

	if err != nil {
		return nil, err
	}

	return multiUploadInit, nil
}
