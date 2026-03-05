package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	log "github.com/sirupsen/logrus"

	"github.com/mulbc/gosbench/common"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
)

var svc, housekeepingSvc *s3.Client
var ctx context.Context
var hc *http.Client
var noRedirectClient *http.Client // For probe requests
var handleRedirect bool

func init() {
	if err := view.Register([]*view.View{
		ochttp.ClientSentBytesDistribution,
		ochttp.ClientReceivedBytesDistribution,
		ochttp.ClientRoundtripLatencyDistribution,
		ochttp.ClientCompletedCount,
	}...); err != nil {
		log.WithError(err).Fatalf("Failed to register HTTP client views:")
	}
	view.RegisterExporter(pe)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", pe)
		// http://localhost:8888/metrics
		log.Infof("Starting Prometheus Exporter on port %d", prometheusPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", prometheusPort), mux); err != nil {
			log.WithError(err).Fatalf("Failed to run Prometheus /metrics endpoint:")
		}
	}()

}

// InitS3 initialises the S3 session
// Also starts the Prometheus exporter on Port 8888
func InitS3(config common.S3Configuration) {
	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: config.SkipSSLVerify, MinVersion: tls.VersionTLS12},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		MaxConnsPerHost:       2048,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       90 * time.Second,
	}
	tr2 := &ochttp.Transport{Base: tr}

	// Store config for use in putObjectWithRedirect
	handleRedirect = config.HandleRedirect
	if handleRedirect {
		log.Info("Redirect handling enabled (application-layer, like AIStore Python SDK)")
	}

	// HTTP client for performance monitoring
	hc = &http.Client{Transport: tr2}

	// HTTP client for probe requests that should not follow redirects
	// Reuses the same transport for connection pooling
	noRedirectClient = &http.Client{
		Transport: tr2,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	// TODO Create a context with a timeout - we already use this context in all S3 calls
	// Usually this shouldn't be a problem ;)
	ctx = context.Background()

	cfg, err := s3config.LoadDefaultConfig(ctx,
		s3config.WithHTTPClient(hc),
		s3config.WithRegion(config.Region),
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, "")),
		s3config.WithRetryer(func() aws.Retryer {
			return aws.NopRetryer{}
		}),
	)
	if err != nil {
		log.WithError(err).Fatal("Unable to build S3 config")
	}
	// Use this Session to do things that are hidden from the performance monitoring
	// Setting up the housekeeping S3 client
	hkhc := &http.Client{
		Transport: tr,
	}

	hkCfg, err := s3config.LoadDefaultConfig(ctx,
		s3config.WithHTTPClient(hkhc),
		s3config.WithRegion(config.Region),
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, "")),
		s3config.WithRetryer(func() aws.Retryer {
			return aws.NopRetryer{}
		}),
	)
	if err != nil {
		log.WithError(err).Fatal("Unable to build S3 housekeeping config")
	}

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	svc = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(config.Endpoint)
		o.UsePathStyle = config.UsePathStyle
	})
	// Use this service to do things that are hidden from the performance monitoring
	housekeepingSvc = s3.NewFromConfig(hkCfg, func(o *s3.Options) {
		if config.CacheMode {
			o.BaseEndpoint = aws.String(config.BackendEndpoint)
		} else {
			o.BaseEndpoint = aws.String(config.Endpoint)
		}
		o.UsePathStyle = config.UsePathStyle
	})

	log.Debug("S3 Init done")
}

func putObject(service *s3.Client, objectName string, objectContent io.ReadSeeker, bucket string) error {
	if handleRedirect {
		// Handle redirects at application layer.
		return putObjectWithRedirect(objectName, objectContent, bucket)
	}

	// Use upload manager for non-redirect case (original behavior)
	uploader := manager.NewUploader(service, func(d *manager.Uploader) {
		d.MaxUploadParts = 1
	})

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &objectName,
		Body:   objectContent,
	})

	if err != nil {
		log.WithError(err).WithField("object", objectName).WithField("bucket", bucket).Errorf("Failed to upload object,")
		return err
	}

	log.WithField("bucket", bucket).WithField("key", objectName).Tracef("Upload successful")

	return err
}

// putObjectWithRedirect implements redirect handling at the application layer,
// This approach:
// 1. Sends a probe request (no body) to discover redirect location
// 2. If redirect found, sends data directly to target
func putObjectWithRedirect(objectName string, objectContent io.ReadSeeker, bucket string) error {
	objectURL, err := buildProbeURL(bucket, objectName)
	if err != nil {
		return err
	}

	// Send probe request (no body, no Content-Length) to discover redirect
	// exclude data from initial request, use allow_redirects=False equivalent
	probeReq, err := http.NewRequestWithContext(ctx, http.MethodPut, objectURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create probe request: %w", err)
	}
	probeReq.Header.Set("Content-Type", "application/octet-stream")

	probeResp, err := noRedirectClient.Do(probeReq)
	if err != nil {
		log.WithError(err).Debug("Probe request failed, sending with body directly")
		return sendToTarget(objectURL, objectContent)
	}

	// Drain and close response body
	_, _ = io.Copy(io.Discard, probeResp.Body)
	probeResp.Body.Close()

	// Check for redirect response
	if probeResp.StatusCode == http.StatusTemporaryRedirect || probeResp.StatusCode == http.StatusPermanentRedirect {
		if location := probeResp.Header.Get("Location"); location != "" {
			log.Debugf("Redirect discovered: %s -> %s", objectURL, location)

			// Send data directly to target
			return sendToTarget(location, objectContent)
		}
	}
	log.Debugf("Probe returned %d, sending with body to original URL", probeResp.StatusCode)

	// No redirect found - send to original URL with body
	return sendToTarget(objectURL, objectContent)
}

func buildProbeURL(bucket string, objectName string) (string, error) {
	u, err := url.Parse(config.S3Config.Endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL %q: %w", config.S3Config.Endpoint, err)
	}

	if config.S3Config.CacheMode {
		u.Path = "/v1/objects/" + bucket + "/" + url.PathEscape(objectName)
	} else if config.S3Config.UsePathStyle {
		u.Path = "/" + bucket + "/" + url.PathEscape(objectName)
	} else {
		u.Host = bucket + "." + u.Host
		u.Path = "/" + url.PathEscape(objectName)
	}

	if config.S3Config.CacheMode {
		q := u.Query()
		q.Set("provider", "ais")
		namespace := strings.TrimSpace(config.S3Config.BackendEndpointUuid)
		if namespace != "" && !strings.HasPrefix(namespace, "@") {
			namespace = "@" + namespace
		}
		if namespace != "" {
			q.Set("namespace", namespace)
		}
		u.RawQuery = q.Encode()
	}

	log.Debugf("redirect URL: %s", u.String())
	return u.String(), nil
}

// sendToTarget sends the PUT request with body to the target URL.
func sendToTarget(targetURL string, body io.ReadSeeker) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("PUT request failed: %w", err)
	}

	// Drain and close response body to free connection
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Tracef("Upload successful to %s", targetURL)
		return nil
	}

	return fmt.Errorf("PUT failed with status %d", resp.StatusCode)
}

// func getObjectProperties(service *s3.S3, objectName string, bucket string) {
// 	service.ListObjects(&s3.ListObjectsInput{
// 		Bucket: &bucket,
// 	})
// 	result, err := service.GetObjectWithContext(ctx, &s3.GetObjectInput{
// 		Bucket: &bucket,
// 		Key:    &objectName,
// 	})
// 	if err != nil {
// 		// Cast err to awserr.Error to handle specific error codes.
// 		aerr, ok := err.(awserr.Error)
// 		if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
// 			log.WithError(aerr).Errorf("Could not find object %s in bucket %s when querying properties", objectName, bucket)
// 		}
// 	}

// 	// Make sure to close the body when done with it for S3 GetObject APIs or
// 	// will leak connections.
// 	defer result.Body.Close()

// 	log.Debugf("Object Properties:\n%+v", result)
// }

func listObjects(service *s3.Client, prefix string, bucket string) ([]types.Object, error) {
	var bucketContents []types.Object
	p := s3.NewListObjectsV2Paginator(service, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		// Next Page takes a new context for each page retrieval. This is where
		// you could add timeouts or deadlines.
		page, err := p.NextPage(ctx)
		if err != nil {
			log.WithError(err).WithField("prefix", prefix).WithField("bucket", bucket).Errorf("Failed to list objects")
			return nil, err
		}
		bucketContents = append(bucketContents, page.Contents...)
	}

	return bucketContents, nil
}

func getObject(service *s3.Client, objectName string, bucket string, objectSize uint64) error {
	result, err := service.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &objectName,
	}, func(o *s3.Options) {
		if config.S3Config.CacheMode {
			o.APIOptions = append(o.APIOptions, addGetQueryParams(config.S3Config.BackendEndpointUuid))
		}
	})
	if err != nil {
		return err
	}

	defer result.Body.Close()

	numBytes, err := io.Copy(io.Discard, result.Body)
	if err != nil {
		return err
	}
	if numBytes != int64(objectSize) {
		return fmt.Errorf("Expected object length %d is not matched to actual object length %d", objectSize, numBytes)
	}
	return nil
}


func addGetQueryParams(uuid string) func(*middleware.Stack) error {
	namespace := strings.TrimSpace(uuid)
	if namespace != "" && !strings.HasPrefix(namespace, "@") {
		namespace = "@" + namespace
	}

	return func(stack *middleware.Stack) error {
		if err := stack.Build.Add(middleware.BuildMiddlewareFunc("GetQueryParams", func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
			req, ok := in.Request.(*smithyhttp.Request)
			if !ok {
				return middleware.BuildOutput{}, middleware.Metadata{}, fmt.Errorf("unexpected request type %T", in.Request)
			}

			q := req.URL.Query()
			q.Set("provider", "ais")
			if namespace != "" {
				q.Set("namespace", namespace)
			}
			req.URL.RawQuery = q.Encode()
			log.Debugf("FULL URL: %s", req.URL.String())

			return next.HandleBuild(ctx, in)
		}), middleware.After); err != nil {
			return err
		}

		return nil
	}
}

func deleteObject(service *s3.Client, objectName string, bucket string) error {
	_, err := service.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &objectName,
	})
	if err != nil {
		log.WithError(err).Errorf("Could not find object %s in bucket %s for deletion", objectName, bucket)
	}
	return err
}

func createBucket(service *s3.Client, bucket string) error {
	// Do not err when the bucket is already there...
	_, err := service.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		var bne *types.BucketAlreadyExists
		// Ignore error if bucket already exists
		if errors.As(err, &bne) {
			return nil
		}
		log.WithError(err).Errorf("Issues when creating bucket %s", bucket)
	}
	return err
}

func deleteBucket(service *s3.Client, bucket string) error {
	// First delete all objects in the bucket
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	var bucketContents []types.Object
	isTruncated := true
	for isTruncated {
		result, err := service.ListObjectsV2(ctx, input)
		if err != nil {
			return err
		}
		bucketContents = append(bucketContents, result.Contents...)
		input.ContinuationToken = result.NextContinuationToken
		isTruncated = *result.IsTruncated
	}

	if len(bucketContents) > 0 {
		var objectsToDelete []types.ObjectIdentifier
		for _, item := range bucketContents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key: item.Key,
			})
		}

		deleteObjectsInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(true),
			},
		}

		_, err := svc.DeleteObjects(ctx, deleteObjectsInput)
		if err != nil {
			return err
		}
	}

	// Then delete the (now empty) bucket itself
	_, err := service.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	return err
}
