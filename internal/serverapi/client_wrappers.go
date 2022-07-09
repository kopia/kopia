package serverapi

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// CreateSnapshotSource creates snapshot source with a given path.
func CreateSnapshotSource(ctx context.Context, c *apiclient.KopiaAPIClient, req *CreateSnapshotSourceRequest) (*CreateSnapshotSourceResponse, error) {
	resp := &CreateSnapshotSourceResponse{}
	if err := c.Post(ctx, "sources", req, resp); err != nil {
		return nil, errors.Wrap(err, "CreateSnapshotSource")
	}

	return resp, nil
}

// Estimate starts snapshot estimation task for a given directory.
func Estimate(ctx context.Context, c *apiclient.KopiaAPIClient, req *EstimateRequest) (*uitask.Info, error) {
	resp := &uitask.Info{}
	if err := c.Post(ctx, "estimate", req, resp); err != nil {
		return nil, errors.Wrap(err, "Estimate")
	}

	return resp, nil
}

// Restore starts snapshot restore task for a given directory.
func Restore(ctx context.Context, c *apiclient.KopiaAPIClient, req *RestoreRequest) (*uitask.Info, error) {
	resp := &uitask.Info{}
	if err := c.Post(ctx, "restore", req, resp); err != nil {
		return nil, errors.Wrap(err, "Restore")
	}

	return resp, nil
}

// GetTask starts snapshot estimation task for a given directory.
func GetTask(ctx context.Context, c *apiclient.KopiaAPIClient, taskID string) (*uitask.Info, error) {
	resp := &uitask.Info{}
	if err := c.Get(ctx, "tasks/"+taskID, nil, resp); err != nil {
		return nil, errors.Wrap(err, "GetTask")
	}

	return resp, nil
}

// UploadSnapshots triggers snapshot upload on matching snapshots.
func UploadSnapshots(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*MultipleSourceActionResponse, error) {
	resp := &MultipleSourceActionResponse{}
	if err := c.Post(ctx, "sources/upload"+matchSourceParameters(match), &Empty{}, resp); err != nil {
		return nil, errors.Wrap(err, "UploadSnapshots")
	}

	return resp, nil
}

// CancelUpload cancels snapshot upload on matching snapshots.
func CancelUpload(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*MultipleSourceActionResponse, error) {
	resp := &MultipleSourceActionResponse{}
	if err := c.Post(ctx, "sources/cancel"+matchSourceParameters(match), &Empty{}, resp); err != nil {
		return nil, errors.Wrap(err, "CancelUpload")
	}

	return resp, nil
}

// CreateRepository invokes the 'repo/create' API.
func CreateRepository(ctx context.Context, c *apiclient.KopiaAPIClient, req *CreateRepositoryRequest) error {
	// nolint:wrapcheck
	return c.Post(ctx, "repo/create", req, &StatusResponse{})
}

// ConnectToRepository invokes the 'repo/connect' API.
func ConnectToRepository(ctx context.Context, c *apiclient.KopiaAPIClient, req *ConnectRepositoryRequest) error {
	// nolint:wrapcheck
	return c.Post(ctx, "repo/connect", req, &StatusResponse{})
}

// DisconnectFromRepository invokes the 'repo/disconnect' API.
func DisconnectFromRepository(ctx context.Context, c *apiclient.KopiaAPIClient) error {
	// nolint:wrapcheck
	return c.Post(ctx, "repo/disconnect", &Empty{}, &Empty{})
}

// Shutdown invokes the 'control/shutdown' API.
func Shutdown(ctx context.Context, c *apiclient.KopiaAPIClient) error {
	// nolint:wrapcheck
	return c.Post(ctx, "control/shutdown", &Empty{}, &Empty{})
}

// RepoStatus invokes the 'repo/status' API.
func RepoStatus(ctx context.Context, c *apiclient.KopiaAPIClient) (*StatusResponse, error) {
	resp := &StatusResponse{}
	if err := c.Get(ctx, "repo/status", nil, resp); err != nil {
		return nil, errors.Wrap(err, "Status")
	}

	return resp, nil
}

// Status invokes the 'control/status' API.
func Status(ctx context.Context, c *apiclient.KopiaAPIClient) (*StatusResponse, error) {
	resp := &StatusResponse{}
	if err := c.Get(ctx, "control/status", nil, resp); err != nil {
		return nil, errors.Wrap(err, "Status")
	}

	return resp, nil
}

// GetThrottlingLimits gets the throttling limits.
func GetThrottlingLimits(ctx context.Context, c *apiclient.KopiaAPIClient) (throttling.Limits, error) {
	resp := throttling.Limits{}
	if err := c.Get(ctx, "repo/throttle", nil, &resp); err != nil {
		return throttling.Limits{}, errors.Wrap(err, "throttling")
	}

	return resp, nil
}

// SetThrottlingLimits sets the throttling limits.
func SetThrottlingLimits(ctx context.Context, c *apiclient.KopiaAPIClient, l throttling.Limits) error {
	if err := c.Put(ctx, "repo/throttle", &l, &Empty{}); err != nil {
		return errors.Wrap(err, "throttling")
	}

	return nil
}

// ListSources lists the snapshot sources managed by the server.
func ListSources(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*SourcesResponse, error) {
	resp := &SourcesResponse{}
	if err := c.Get(ctx, "sources"+matchSourceParameters(match), nil, resp); err != nil {
		return nil, errors.Wrap(err, "ListSources")
	}

	return resp, nil
}

// ListSnapshots lists the snapshots managed by the server for a given source source.
func ListSnapshots(ctx context.Context, c *apiclient.KopiaAPIClient, src snapshot.SourceInfo, all bool) (*SnapshotsResponse, error) {
	resp := &SnapshotsResponse{}

	u := "snapshots" + matchSourceParameters(&src)
	if all {
		u += "&all=1"
	}

	if err := c.Get(ctx, u, nil, resp); err != nil {
		return nil, errors.Wrap(err, "ListSnapshots")
	}

	return resp, nil
}

// ListPolicies lists the policies managed by the server for a given target filter.
func ListPolicies(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*PoliciesResponse, error) {
	resp := &PoliciesResponse{}
	if err := c.Get(ctx, "policies"+matchSourceParameters(match), nil, resp); err != nil {
		return nil, errors.Wrap(err, "ListPolicies")
	}

	return resp, nil
}

func policyTargetURLParamters(si snapshot.SourceInfo) string {
	return fmt.Sprintf("userName=%v&host=%v&path=%v", si.UserName, si.Host, si.Path)
}

// SetPolicy sets the policy.
func SetPolicy(ctx context.Context, c *apiclient.KopiaAPIClient, si snapshot.SourceInfo, pol *policy.Policy) error {
	resp := &Empty{}
	if err := c.Put(ctx, "policy?"+policyTargetURLParamters(si), pol, resp); err != nil {
		return errors.Wrap(err, "SetPolicy")
	}

	return nil
}

// ResolvePolicy resolves the policy.
func ResolvePolicy(ctx context.Context, c *apiclient.KopiaAPIClient, si snapshot.SourceInfo, req *ResolvePolicyRequest) (*ResolvePolicyResponse, error) {
	resp := &ResolvePolicyResponse{}

	if err := c.Post(ctx, "policy/resolve?"+policyTargetURLParamters(si), req, resp); err != nil {
		return nil, errors.Wrap(err, "ResolvePolicy")
	}

	return resp, nil
}

// ListTasks lists the tasks.
func ListTasks(ctx context.Context, c *apiclient.KopiaAPIClient) (*TaskListResponse, error) {
	resp := &TaskListResponse{}
	if err := c.Get(ctx, "tasks", nil, resp); err != nil {
		return nil, errors.Wrap(err, "ListTasks")
	}

	return resp, nil
}

// GetObject returns the object payload.
func GetObject(ctx context.Context, c *apiclient.KopiaAPIClient, objectID string) ([]byte, error) {
	var b []byte

	if err := c.Get(ctx, "objects/"+objectID, object.ErrObjectNotFound, &b); err != nil {
		return nil, errors.Wrap(err, "GetObject")
	}

	return b, nil
}

func matchSourceParameters(match *snapshot.SourceInfo) string {
	if match == nil {
		return ""
	}

	var clauses []string
	if v := match.Host; v != "" {
		clauses = append(clauses, "host="+v)
	}

	if v := match.UserName; v != "" {
		clauses = append(clauses, "username="+v)
	}

	if v := match.Path; v != "" {
		clauses = append(clauses, "path="+v)
	}

	if len(clauses) == 0 {
		return ""
	}

	return "?" + strings.Join(clauses, "&")
}
