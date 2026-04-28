package storj

import (
	"context"
	"errors"
	"fmt"

	"storj.io/uplink"
)

// projAccess is a singleton map caching uplink projects per access grant to avoid repeated network requests.
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

// UlDeleteBucket deletes the named bucket using the given access grant.
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
	if err != nil {
		return fmt.Errorf("deleting bucket %q: %w", bucketName, err)
	}

	return nil
}

// UlDeleteBucketWithObjects deletes the named bucket and all its objects using the given access grant.
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
	if err != nil {
		return fmt.Errorf("deleting bucket with objects %q: %w", bucketName, err)
	}

	return nil
}

// UlEnsureBucket ensures the named bucket exists, creating it if necessary.
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

	bucket, err = proj.EnsureBucket(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("ensuring bucket %q: %w", bucketName, err)
	}

	return bucket, nil
}

// UlCreateBucket creates the named bucket using the given access grant.
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

	bucket, err = proj.CreateBucket(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("creating bucket %q: %w", bucketName, err)
	}

	return bucket, nil
}

// ULDeleteAllObjects deletes all objects in the named bucket using the given access grant.
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
