// Convenience functions to access uplink directly (without instantiating storage)
// e.g. to handle buckets
package storj

import (
	"context"
	"errors"
	"fmt"

	"storj.io/uplink"
)

// Since this direct access is only used for convenience in local dev/test,
// but we don't want to do unnecessary network requests (mainly to avoid spamming the satellite)
// we make a singleton map that can hold the project for each accessed access grant
var (
	projAccess = make(map[string]*uplink.Project)
)

func getProject(ctx context.Context, accessGrant string) (project *uplink.Project, err error) {
	if project, ok := projAccess[accessGrant]; ok {
		return project, nil
	}
	access, err := uplink.ParseAccess(accessGrant)
	if err != nil {
		return nil, fmt.Errorf("could not request access grant: %w", err)
	}

	project, err = uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, fmt.Errorf("could not open project: %w", err)
	}
	projAccess[accessGrant] = project
	return project, nil
}

func UlDeleteBucket(ctx context.Context, bucketName, accessGrant string) (err error) {
	proj, err := getProject(ctx, accessGrant)
	defer func() {
		if closeErr := proj.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err != nil {
		return err
	}
	_, err = proj.DeleteBucket(ctx, bucketName)

	return err
}

func UlDeleteBucketWithObjects(ctx context.Context, bucketName, accessGrant string) (err error) {
	proj, err := getProject(ctx, accessGrant)
	defer func() {
		if closeErr := proj.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err != nil {
		return err
	}
	_, err = proj.DeleteBucketWithObjects(ctx, bucketName)

	return err
}

func UlEnsureBucket(ctx context.Context, bucketName, accessGrant string) (bucket *uplink.Bucket, err error) {
	proj, err := getProject(ctx, accessGrant)
	defer func() {
		if closeErr := proj.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err != nil {
		return nil, err
	}
	return proj.EnsureBucket(ctx, bucketName)
}

func UlCreateBucket(ctx context.Context, bucketName, accessGrant string) (bucket *uplink.Bucket, err error) {
	proj, err := getProject(ctx, accessGrant)
	defer func() {
		if closeErr := proj.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	if err != nil {
		return nil, err
	}
	return proj.CreateBucket(ctx, bucketName)
}

func ULDeleteAllObjects(ctx context.Context, bucketName, accessGrant string) error {
	proj, err := getProject(ctx, accessGrant)
	defer func() {
		if closeErr := proj.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	if err != nil {
		return err
	}
	objects := proj.ListObjects(ctx, bucketName, &uplink.ListObjectsOptions{Recursive: true})

	var errs []error
	for objects.Next() {
		key := objects.Item().Key
		if _, err := proj.DeleteObject(ctx, bucketName, key); err != nil {
			errs = append(errs, fmt.Errorf("deleting object %q: %w", key, err))
		}
	}
	if err := objects.Err(); err != nil {
		errs = append(errs, fmt.Errorf("listing objects: %w", err))
	}

	return errors.Join(errs...)
}
