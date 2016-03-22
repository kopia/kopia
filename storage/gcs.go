package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/api/googleapi"
	gcsclient "google.golang.org/api/storage/v1"
)

const (
	gcsRepositoryType = "gcs"
	gcsTokenCacheDir  = ".kopia"

	// Those are not really secret, since the app is installed.
	googleCloudClientID     = "194841383482-nmn10h4mnllnsvou7qr55tfh5jsmtkap.apps.googleusercontent.com"
	googleCloudClientSecret = "ZL52E96Q7iRCD9YXVA7U6UaI"
)

// GCSRepositoryOptions defines options Google Cloud Storage-backed repository.
type GCSRepositoryOptions struct {
	// BucketName is the name of the GCS bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// TokenCacheFile is the name of the file that will persist the OAuth2 token.
	// If not specified, the token will be persisted in GCSRepositoryOptions.
	TokenCacheFile string `json:"tokenCacheFile,omitempty"`

	// Token stored the OAuth2 token (when TokenCacheFile is empty)
	Token *oauth2.Token `json:"token,omitempty"`

	// ReadOnly causes the repository to be configured without write permissions, to prevent accidental
	// modifications to the data.
	ReadOnly bool `json:"readonly"`

	// IgnoreDefaultCredentials disables the use of credentials managed by Google Cloud SDK (gcloud).
	IgnoreDefaultCredentials bool `json:"ignoreDefaultCredentials"`
}

type gcsRepository struct {
	GCSRepositoryOptions
	objectsService *gcsclient.ObjectsService
}

func (gcs *gcsRepository) BlockExists(b BlockID) (bool, error) {
	_, err := gcs.objectsService.Get(gcs.BucketName, gcs.getObjectNameString(b)).Do()

	if err == nil {
		return true, nil
	}

	return false, err
}

func (gcs *gcsRepository) GetBlock(b BlockID) ([]byte, error) {
	v, err := gcs.objectsService.Get(gcs.BucketName, gcs.getObjectNameString(b)).Download()
	if err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			if err.Code == 404 {
				return nil, ErrBlockNotFound
			}
		}

		return nil, fmt.Errorf("unable to get block '%s': %v", b, err)
	}

	defer v.Body.Close()

	return ioutil.ReadAll(v.Body)
}

func (gcs *gcsRepository) PutBlock(b BlockID, data io.ReadCloser, options PutOptions) error {
	object := gcsclient.Object{
		Name: gcs.getObjectNameString(b),
	}
	defer data.Close()
	_, err := gcs.objectsService.Insert(gcs.BucketName, &object).
		IfGenerationMatch(0).
		Media(data).
		Do()

	return err
}

func (gcs *gcsRepository) DeleteBlock(b BlockID) error {
	err := gcs.objectsService.Delete(gcs.BucketName, string(b)).Do()
	if err != nil {
		return fmt.Errorf("unable to delete block %s: %v", b, err)
	}

	return nil
}

func (gcs *gcsRepository) getObjectNameString(b BlockID) string {
	return gcs.Prefix + string(b)
}

func (gcs *gcsRepository) ListBlocks(prefix BlockID) chan (BlockMetadata) {
	ch := make(chan BlockMetadata, 100)

	go func() {
		ps := gcs.getObjectNameString(prefix)
		page, _ := gcs.objectsService.List(gcs.BucketName).
			Prefix(ps).Do()
		for {
			for _, o := range page.Items {
				t, e := time.Parse(time.RFC3339, o.TimeCreated)
				if e != nil {
					ch <- BlockMetadata{
						Error: e,
					}
				} else {
					ch <- BlockMetadata{
						BlockID:   BlockID(o.Name)[len(gcs.Prefix):],
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

func (gcs *gcsRepository) Flush() error {
	return nil
}

func (gcs *gcsRepository) Configuration() RepositoryConfiguration {
	return RepositoryConfiguration{
		gcsRepositoryType,
		&gcs.GCSRepositoryOptions,
	}
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

// NewGCSRepository creates new Google Cloud Storage-backed repository with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func NewGCSRepository(options *GCSRepositoryOptions) (Repository, error) {
	ctx := context.TODO()

	gcs := &gcsRepository{
		GCSRepositoryOptions: *options,
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
		client, _ = google.DefaultClient(context.TODO(), scope)
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
				// Cache file not provided, token will be saved in repository configuration.
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
	go openURL(authURL)
	log.Printf("Authorize this app at: %s", authURL)
	code := <-ch
	log.Printf("Got code: %s", code)

	token, err := config.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("token exchange error: %v", err)
	}

	return token, nil
}

func openURL(url string) error {
	try := []string{"xdg-open", "google-chrome", "open"}
	for _, bin := range try {
		err := exec.Command(bin, url).Run()
		if err == nil {
			return nil
		}
	}
	log.Printf("Error opening URL in browser.")
	return fmt.Errorf("Error opening URL in browser")
}

func authPrompt(url string, state string) (authenticationCode string, err error) {
	ch := make(chan string)

	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/favicon.ico" {
			http.Error(rw, "", 404)
			return
		}
		if req.FormValue("state") != state {
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

	log.Println("Go to", url)
	var code string
	n, err := fmt.Scanf("%s", &code)
	if n == 1 {
		return code, nil
	}

	return "", err

}

func init() {
	AddSupportedRepository(
		gcsRepositoryType,
		func() interface{} {
			return &GCSRepositoryOptions{}
		},
		func(cfg interface{}) (Repository, error) {
			return NewGCSRepository(cfg.(*GCSRepositoryOptions))
		})
}
