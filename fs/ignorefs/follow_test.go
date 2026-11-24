package ignorefs_test

import (
	"context"
	"io"
	"os"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/cachedir"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/snapshot/policy"
)

var falseValue = policy.OptionalBool(false)

func followSymlinksPolicy(enabled bool) *policy.Tree {
	v := policy.OptionalBool(enabled)

	return policy.BuildTree(map[string]*policy.Policy{
		".": {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &v}},
	}, policy.DefaultPolicy)
}

// describeTree lists "path kind" for every entry below dir, where kind is file:<content>, dir, symlink or other.
func describeTree(t *testing.T, dir fs.Directory) []string {
	t.Helper()

	var out []string

	var walk func(prefix string, d fs.Directory)

	walk = func(prefix string, d fs.Directory) {
		require.NoError(t, fs.IterateEntries(testlogging.Context(t), d, func(ctx context.Context, e fs.Entry) error {
			p := prefix + "/" + e.Name()

			switch e := e.(type) {
			case fs.Directory:
				out = append(out, p+" dir")
				walk(p, e)
			case fs.Symlink:
				out = append(out, p+" symlink")
			case fs.File:
				out = append(out, p+" file:"+readAll(t, e))
			default:
				out = append(out, p+" other")
			}

			return nil
		}))
	}

	walk(".", dir)
	sort.Strings(out)

	return out
}

func readAll(t *testing.T, f fs.File) string {
	t.Helper()

	r, err := f.Open(testlogging.Context(t))
	require.NoError(t, err)

	defer r.Close() //nolint:errcheck

	b, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(b)
}

func Test_followSymlinks_file(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddFile("f", []byte("hello"), 0o644)
	root.AddSymlink("l", "f", 0o777)

	require.Equal(t, []string{
		"./f file:hello",
		"./l file:hello",
	}, describeTree(t, ignorefs.New(root, followSymlinksPolicy(true))))
}

func Test_followSymlinks_fileMetadataComesFromTarget(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddFile("f", []byte("hello"), 0o640)
	root.AddSymlink("l", "f", 0o777)

	e, err := ignorefs.New(root, followSymlinksPolicy(true)).Child(testlogging.Context(t), "l")
	require.NoError(t, err)

	f, ok := e.(fs.File)
	require.True(t, ok, "got %T", e)
	require.Equal(t, "l", f.Name())
	require.Equal(t, int64(5), f.Size())
	require.Zero(t, f.Mode()&os.ModeSymlink)
	require.Equal(t, os.FileMode(0o640), f.Mode().Perm())
}

func Test_followSymlinks_directory(t *testing.T) {
	root := mockfs.NewDirectory()
	d := root.AddDir("d", 0o755)
	d.AddFile("x", []byte("xx"), 0o644)
	d.AddDir("sub", 0o755).AddFile("y", []byte("yy"), 0o644)
	root.AddSymlink("ld", "d", 0o777)

	require.Equal(t, []string{
		"./d dir",
		"./d/sub dir",
		"./d/sub/y file:yy",
		"./d/x file:xx",
		"./ld dir",
		"./ld/sub dir",
		"./ld/sub/y file:yy",
		"./ld/x file:xx",
	}, describeTree(t, ignorefs.New(root, followSymlinksPolicy(true))))
}

func Test_followSymlinks_chainRelativeAndAbsoluteLinks(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddFile("f", []byte("data"), 0o644)
	root.AddDir("a", 0o755)
	root.AddSymlink("l2", "f", 0o777)
	root.AddSymlink("l1", "l2", 0o777)
	root.AddSymlink("a/up", "../f", 0o777)
	root.AddSymlink("a/abs", "/f", 0o777)

	require.Equal(t, []string{
		"./a dir",
		"./a/abs file:data",
		"./a/up file:data",
		"./f file:data",
		"./l1 file:data",
		"./l2 file:data",
	}, describeTree(t, ignorefs.New(root, followSymlinksPolicy(true))))
}

func Test_followSymlinks_disabledKeepsSymlinks(t *testing.T) {
	for name, tree := range map[string]*policy.Tree{
		"explicitly off": followSymlinksPolicy(false),
		"default policy": policy.BuildTree(nil, policy.DefaultPolicy),
		"no policy tree": nil,
	} {
		t.Run(name, func(t *testing.T) {
			root := mockfs.NewDirectory()
			root.AddFile("f", []byte("hello"), 0o644)
			root.AddDir("d", 0o755)
			root.AddSymlink("l", "f", 0o777)
			root.AddSymlink("ld", "d", 0o777)

			require.Equal(t, []string{
				"./d dir",
				"./f file:hello",
				"./l symlink",
				"./ld symlink",
			}, describeTree(t, ignorefs.New(root, tree)))
		})
	}
}

func Test_followSymlinks_policyInheritanceAndOverride(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddFile("f", []byte("hello"), 0o644)
	root.AddSymlink("l", "f", 0o777)

	// "other" has no policy, "inherits" has one that does not set the option, "overrides" turns it off and "back-on" on again.
	root.AddDir("other", 0o755).AddSymlink("l", "../f", 0o777)
	root.AddDir("inherits", 0o755).AddSymlink("l", "../f", 0o777)
	root.AddDir("inherits/below", 0o755).AddSymlink("l", "../../f", 0o777)
	root.AddDir("overrides", 0o755).AddSymlink("l", "../f", 0o777)
	root.AddDir("overrides/below", 0o755).AddSymlink("l", "../../f", 0o777)
	root.AddDir("overrides/back-on", 0o755).AddSymlink("l", "../../f", 0o777)

	tree := policy.BuildTree(map[string]*policy.Policy{
		".":                   {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &trueValue}},
		"./inherits":          {FilesPolicy: policy.FilesPolicy{MaxFileSize: 1000}},
		"./overrides":         {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &falseValue}},
		"./overrides/back-on": {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &trueValue}},
	}, policy.DefaultPolicy)

	require.Equal(t, []string{
		"./f file:hello",
		"./inherits dir",
		"./inherits/below dir",
		"./inherits/below/l file:hello",
		"./inherits/l file:hello",
		"./l file:hello",
		"./other dir",
		"./other/l file:hello",
		"./overrides dir",
		"./overrides/back-on dir",
		"./overrides/back-on/l file:hello",
		"./overrides/below dir",
		"./overrides/below/l symlink",
		"./overrides/l symlink",
	}, describeTree(t, ignorefs.New(root, tree)))
}

func Test_followSymlinks_policyOnTheLinkItself(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddFile("f", []byte("hello"), 0o644)
	root.AddSymlink("followed", "f", 0o777)
	root.AddSymlink("kept", "f", 0o777)

	tree := policy.BuildTree(map[string]*policy.Policy{
		"./followed": {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &trueValue}},
	}, policy.DefaultPolicy)

	require.Equal(t, []string{
		"./f file:hello",
		"./followed file:hello",
		"./kept symlink",
	}, describeTree(t, ignorefs.New(root, tree)))
}

func Test_followSymlinks_fallbackToSymlink(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddFile("f", []byte("hello"), 0o644)
	root.AddErrorEntryIrregular("special", 0o644, errors.New("unsupported"))
	root.AddSymlink("dangling", "no-such-file", 0o777)
	root.AddSymlink("to-special", "special", 0o777)
	root.AddSymlink("cycle-a", "cycle-b", 0o777)
	root.AddSymlink("cycle-b", "cycle-a", 0o777)

	got := describeTree(t, ignorefs.New(root, followSymlinksPolicy(true)))

	require.Contains(t, got, "./f file:hello")
	require.Contains(t, got, "./dangling symlink")
	require.Contains(t, got, "./to-special symlink")
	require.Contains(t, got, "./cycle-a symlink")
	require.Contains(t, got, "./cycle-b symlink")
}

func Test_followSymlinks_dotIgnoreFilesAndCacheDirsApplyInFollowedDirectories(t *testing.T) {
	root := mockfs.NewDirectory()

	ignoring := root.AddDir("ignoring", 0o755)
	ignoring.AddFileLines(".kopiaignore", []string{"secret*"}, 0o644)
	ignoring.AddFile("secret", []byte("s"), 0o644)
	ignoring.AddFile("public", []byte("p"), 0o644)

	cache := root.AddDir("cache", 0o755)
	cache.AddFile(cachedir.CacheDirMarkerFile, []byte(cachedir.CacheDirMarkerHeader), 0o644)
	cache.AddFile("data", []byte("d"), 0o644)

	root.AddSymlink("link-to-ignoring", "ignoring", 0o777)
	root.AddSymlink("link-to-cache", "cache", 0o777)

	followed := policy.OptionalBool(true)
	tree := policy.BuildTree(map[string]*policy.Policy{
		".": {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &followed, DotIgnoreFiles: []string{".kopiaignore"}}},
	}, policy.DefaultPolicy)

	require.Equal(t, []string{
		"./cache dir",
		"./ignoring dir",
		"./ignoring/.kopiaignore file:secret*",
		"./ignoring/public file:p",
		"./link-to-cache dir",
		"./link-to-ignoring dir",
		"./link-to-ignoring/.kopiaignore file:secret*",
		"./link-to-ignoring/public file:p",
	}, describeTree(t, ignorefs.New(root, tree)))
}

func Test_followSymlinks_ignoreRulesSeeTheTarget(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddDir("d", 0o755).AddFile("x", []byte("x"), 0o644)
	root.AddFile("f", []byte("f"), 0o644)
	root.AddSymlink("ld", "d", 0o777)
	root.AddSymlink("lf", "f", 0o777)

	rule := func(r string, follow bool) *policy.Tree {
		v := policy.OptionalBool(follow)

		return policy.BuildTree(map[string]*policy.Policy{
			".": {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &v, IgnoreRules: []string{r}}},
		}, policy.DefaultPolicy)
	}

	// a pattern ending in '/' only matches directories.
	require.Equal(t, []string{
		"./d dir",
		"./d/x file:x",
		"./f file:f",
		"./lf file:f",
	}, describeTree(t, ignorefs.New(root, rule("ld/", true))))

	require.Contains(t, describeTree(t, ignorefs.New(root, rule("ld/", false))), "./ld symlink")
}

func Test_followSymlinks_maxFileSizeUsesTargetSize(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddFile("big", []byte("0123456789"), 0o644)
	root.AddFile("small", []byte("01"), 0o644)
	root.AddSymlink("l-big", "big", 0o777)
	root.AddSymlink("l-small", "small", 0o777)

	v := policy.OptionalBool(true)
	tree := policy.BuildTree(map[string]*policy.Policy{
		".": {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &v, MaxFileSize: 5}},
	}, policy.DefaultPolicy)

	require.Equal(t, []string{
		"./l-small file:01",
		"./small file:01",
	}, describeTree(t, ignorefs.New(root, tree)))
}

func Test_followSymlinks_oneFileSystem(t *testing.T) {
	otherDev := fs.DeviceInfo{Dev: 7}

	root := mockfs.NewDirectory()
	root.AddFile("local", []byte("l"), 0o644)
	root.AddFileDevice("remote-file", []byte("r"), 0o644, otherDev)
	root.AddDirDevice("remote-dir", 0o755, otherDev).AddFileDevice("x", []byte("x"), 0o644, otherDev)
	root.AddSymlink("l-local", "local", 0o777)
	root.AddSymlink("l-remote-file", "remote-file", 0o777)
	root.AddSymlink("l-remote-dir", "remote-dir", 0o777)

	followed := policy.OptionalBool(true)
	tree := policy.BuildTree(map[string]*policy.Policy{
		".": {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &followed, OneFileSystem: &followed}},
	}, policy.DefaultPolicy)

	require.Equal(t, []string{
		"./l-local file:l",
		"./local file:l",
	}, describeTree(t, ignorefs.New(root, tree)))
}

func Test_followSymlinks_childMatchesIteration(t *testing.T) {
	root := mockfs.NewDirectory()
	root.AddFile("f", []byte("hello"), 0o644)
	root.AddSymlink("l", "f", 0o777)

	ctx := testlogging.Context(t)
	ifs := ignorefs.New(root, followSymlinksPolicy(true))

	e, err := ifs.Child(ctx, "l")
	require.NoError(t, err)

	f, ok := e.(fs.File)
	require.True(t, ok, "got %T", e)
	require.Equal(t, "hello", readAll(t, f))
}
