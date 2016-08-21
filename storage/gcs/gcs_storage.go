// Package gcs implements Storage based on Google Cloud Storage bucket.
package gcs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"time"

	"github.com/skratchdot/open-golang/open"

	"github.com/kopia/kopia/storage"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/api/googleapi"
	gcsclient "google.golang.org/api/storage/v1"
)

const (
	gcsStorageType = "gcs"

	// Those are not really set, since the app is installed.
	googleCloudClientID     = "194841383482-nmn10h4mnllnsvou7qr55tfh5jsmtkap.apps.googleusercontent.com"
	googleCloudClientSecret = "ZL52E96Q7iRCD9YXVA7U6UaI"
)

type gcsStorage struct {
	Options
	objectsService *gcsclient.ObjectsService
}

func (gcs *gcsStorage) BlockExists(b string) (bool, error) {
	_, err := gcs.objectsService.Get(gcs.BucketName, gcs.getObjectNameString(b)).Do()

	if err == nil {
		return true, nil
	}

	return false, err
}

func (gcs *gcsStorage) GetBlock(b string) ([]byte, error) {
	v, err := gcs.objectsService.Get(gcs.BucketName, gcs.getObjectNameString(b)).Download()
	if err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			if err.Code == http.StatusNotFound {
				return nil, storage.ErrBlockNotFound
			}
		}

		return nil, fmt.Errorf("unable to get block '%s': %v", b, err)
	}

	defer v.Body.Close()

	return ioutil.ReadAll(v.Body)
}

func (gcs *gcsStorage) PutBlock(b string, data storage.ReaderWithLength, options storage.PutOptions) error {
	defer data.Close()
	object := gcsclient.Object{
		Name: gcs.getObjectNameString(b),
	}

	call := gcs.objectsService.Insert(gcs.BucketName, &object).Media(
		data,
		googleapi.ContentType("application/octet-stream"),
		// Specify exact chunk size to ensure data is uploaded in one shot or not at all.
		googleapi.ChunkSize(data.Len()),
	)
	if options&storage.PutOptionsOverwrite == 0 {
		if ok, _ := gcs.BlockExists(b); ok {
			return nil
		}

		// To avoid the race, also check this server-side.
		call = call.IfGenerationMatch(0)
	}
	_, err := call.Do()

	if err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			if err.Code == http.StatusPreconditionFailed {
				// Condition not met indicates that the block already exists.
				return nil
			}
		}
	}

	return err
}

func (gcs *gcsStorage) DeleteBlock(b string) error {
	err := gcs.objectsService.Delete(gcs.BucketName, string(b)).Do()
	if err != nil {
		return fmt.Errorf("unable to delete block %s: %v", b, err)
	}

	return nil
}

func (gcs *gcsStorage) getObjectNameString(b string) string {
	return gcs.Prefix + string(b)
}

func (gcs *gcsStorage) ListBlocks(prefix string) chan (storage.BlockMetadata) {
	ch := make(chan storage.BlockMetadata, 100)

	go func() {
		ps := gcs.getObjectNameString(prefix)
		page, _ := gcs.objectsService.List(gcs.BucketName).
			Prefix(ps).Do()
		for {
			if page == nil {
				break
			}
			for _, o := range page.Items {
				t, e := time.Parse(time.RFC3339, o.TimeCreated)
				if e != nil {
					ch <- storage.BlockMetadata{
						Error: e,
					}
				} else {
					ch <- storage.BlockMetadata{
						BlockID:   string(o.Name)[len(gcs.Prefix):],
						Length:    o.Size,
						TimeStamp: t,
					}
				}
			}

			if page.NextPageToken != "" {
				page, _ = gcs.objectsService.List(gcs.BucketName).
					PageToken(ps).
					Prefix(gcs.getObjectNameString(prefix)).Do()
			} else {
				break
			}
		}
		close(ch)
	}()

	return ch
}

func (gcs *gcsStorage) Flush() error {
	return nil
}

func (gcs *gcsStorage) ConnectionInfo() storage.ConnectionInfo {
	return storage.ConnectionInfo{
		Type:   gcsStorageType,
		Config: &gcs.Options,
	}
}

func (gcs *gcsStorage) Close() error {
	gcs.objectsService = nil
	return nil
}

func (gcs *gcsStorage) String() string {
	return fmt.Sprintf("gcs://%v/%v", gcs.BucketName, gcs.Prefix)
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	t := oauth2.Token{}
	err = json.NewDecoder(f).Decode(&t)
	return &t, err
}

func saveToken(file string, token *oauth2.Token) {
	f, err := os.Create(file)
	if err != nil {
		log.Printf("Warning: failed to cache oauth token: %v", err)
		return
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

// New creates new Google Cloud Storage-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func New(options *Options) (storage.Storage, error) {
	ctx := context.TODO()

	//ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{})

	gcs := &gcsStorage{
		Options: *options,
	}

	if gcs.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	var scope string
	if options.ReadOnly {
		scope = gcsclient.DevstorageReadOnlyScope
	} else {
		scope = gcsclient.DevstorageReadWriteScope
	}

	// Try to get default client if possible and not disabled by options.
	var client *http.Client
	var err error

	if !gcs.IgnoreDefaultCredentials {
		client, _ = google.DefaultClient(ctx, scope)
	}

	if client == nil {
		// Fall back to asking user to authenticate.
		config := &oauth2.Config{
			ClientID:     googleCloudClientID,
			ClientSecret: googleCloudClientSecret,
			Endpoint:     google.Endpoint,
			Scopes:       []string{scope},
		}

		var token *oauth2.Token
		if gcs.Token != nil {
			// Token was provided, use it.
			token = gcs.Token
		} else {
			if gcs.TokenCacheFile == "" {
				// Cache file not provided, token will be saved in storage configuration.
				token, err = tokenFromWeb(ctx, config)
				if err != nil {
					return nil, fmt.Errorf("cannot retrieve OAuth2 token: %v", err)
				}
				gcs.Token = token
			} else {
				token, err = tokenFromFile(gcs.TokenCacheFile)
				if err != nil {
					token, err = tokenFromWeb(ctx, config)
					if err != nil {
						return nil, fmt.Errorf("cannot retrieve OAuth2 token: %v", err)
					}
				}
				saveToken(gcs.TokenCacheFile, token)
			}
		}
		client = config.Client(ctx, token)
	}

	svc, err := gcsclient.New(client)
	if err != nil {
		return nil, fmt.Errorf("Unable to create GCS client: %v", err)
	}

	gcs.objectsService = svc.Objects

	return gcs, nil
}

func readGcsTokenFromFile(filePath string) (*oauth2.Token, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	token := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(token)
	if err != nil {
		return nil, fmt.Errorf("Unable to decode token: %v", err)
	}

	return token, err
}

func writeTokenToFile(filePath string, token *oauth2.Token) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	json.NewEncoder(f).Encode(*token)
	return nil
}

func tokenFromWeb(ctx context.Context, config *oauth2.Config) (*oauth2.Token, error) {
	if runtime.GOOS == "windows" {
		return tokenFromWebLocalServer(ctx, config)
	}

	// On non-SSH Unix, that has X11 configured use local web server.
	if os.Getenv("DISPLAY") != "" && os.Getenv("SSH_CLIENT") == "" {
		return tokenFromWebLocalServer(ctx, config)
	}

	// Otherwise fall back to asking user to manually copy/paste the code.
	return tokenFromWebManual(ctx, config)
}

func tokenFromWebLocalServer(ctx context.Context, config *oauth2.Config) (*oauth2.Token, error) {
	ch := make(chan string)
	randState := fmt.Sprintf("st%d", time.Now().UnixNano())
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/favicon.ico" {
			http.Error(rw, "", 404)
			return
		}
		if req.FormValue("state") != randState {
			log.Printf("State doesn't match: req = %#v", req)
			http.Error(rw, "", 500)
			return
		}
		if code := req.FormValue("code"); code != "" {
			fmt.Fprintf(rw, "<h1>Success</h1>Authorized.")
			rw.(http.Flusher).Flush()
			ch <- code
			return
		}
		log.Printf("no code")
		http.Error(rw, "", 500)
	}))
	defer ts.Close()

	config.RedirectURL = ts.URL
	authURL := config.AuthCodeURL(randState)
	go open.Start(authURL)
	fmt.Println("Opening URL in web browser to get OAuth2 authorization token:")
	fmt.Println()
	fmt.Println("  ", authURL)
	fmt.Println()
	code := <-ch

	token, err := config.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("token exchange error: %v", err)
	}

	return token, nil
}

func tokenFromWebManual(ctx context.Context, config *oauth2.Config) (*oauth2.Token, error) {
	config.RedirectURL = "urn:ietf:wg:oauth:2.0:oob"
	authURL := config.AuthCodeURL("")
	var code string
	for {
		fmt.Println("Please open the following URL in your browser and paste the authorization code below:")
		fmt.Println()
		fmt.Println("  ", authURL)
		fmt.Println()
		fmt.Printf("Enter authorization code: ")
		n, err := fmt.Scanf("%s", &code)
		if err != nil {
			return nil, err
		}

		if n == 1 && len(code) > 0 {
			break
		}
	}
	log.Printf("Got code: %s", code)

	token, err := config.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("token exchange error: %v", err)
	}

	return token, nil
}

func init() {
	storage.AddSupportedStorage(
		gcsStorageType,
		func() interface{} {
			return &Options{}
		},
		func(o interface{}) (storage.Storage, error) {
			return New(o.(*Options))
		})
}
