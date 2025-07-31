package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	as3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	Client *as3.Client
	Bucket string
}

// news3client creates an s3 client with anonymous credentials (for public buckets).
func NewS3Client(ctx context.Context, bucket, region string) (*S3Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	if err != nil {
		return nil, err
	}
	return &S3Client{Client: as3.NewFromConfig(cfg), Bucket: bucket}, nil
}

// listfamily lists all objects in s3 with the prefix "images/{family}/".
// returns layers sorted by key (lexicographic order).
func (s *S3Client) ListFamily(ctx context.Context, family string) ([]Layer, error) {
	prefix := fmt.Sprintf("images/%s/", family)
	var out []Layer
	var token *string

	// paginate through all objects with the prefix
	for {
		resp, err := s.Client.ListObjectsV2(ctx, &as3.ListObjectsV2Input{
			Bucket:            aws.String(s.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range resp.Contents {
			if obj.Key == nil || obj.Size == nil {
				continue
			}

			etag := ""
			if obj.ETag != nil {
				etag = strings.Trim(*obj.ETag, "\"")
			}

			out = append(out, Layer{Key: *obj.Key, ETag: etag, Size: *obj.Size})
		}

		// check if there are more pages
		if resp.IsTruncated != nil && *resp.IsTruncated && resp.NextContinuationToken != nil {
			token = resp.NextContinuationToken
			continue
		}
		break
	}

	// sort layers by key to ensure consistent ordering
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// downloadifmissing downloads a blob from s3 if it doesn't already exist locally.
// returns the local path and sha256 digest.
// if the file already exists, computes digest without re-downloading.
func (s *S3Client) DownloadIfMissing(ctx context.Context, l *Layer) (string, string, error) {
	base := strings.ReplaceAll(l.Key, "/", "_")
	dest := filepath.Join("blobs", base)

	// check if blob already exists locally
	if _, err := os.Stat(dest); err == nil {
		// compute digest of existing file
		dg, err := fileSHA256(dest)
		return dest, dg, err
	}

	// create blobs directory if needed
	if err := os.MkdirAll("blobs", 0755); err != nil {
		return "", "", err
	}

	// download from s3
	obj, err := s.Client.GetObject(ctx, &as3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(l.Key),
	})
	if err != nil {
		return "", "", err
	}
	defer obj.Body.Close()

	// create local file
	f, err := os.Create(dest)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	// stream from s3 while computing sha256
	h := sha256.New()
	w := io.MultiWriter(f, h)
	if _, err := io.Copy(w, obj.Body); err != nil {
		return "", "", err
	}

	digest := hex.EncodeToString(h.Sum(nil))
	return dest, digest, nil
}

// filesha256 computes the sha256 digest of a file.
func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
