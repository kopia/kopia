package gdrive

import (
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	drive "google.golang.org/api/drive/v3"
)

func CreateGoogleOAuth2Config(opt *OAuthConfig) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     opt.ClientId,
		ClientSecret: opt.ClientSecret,
		RedirectURL:  "postmessage",
		Scopes:       []string{drive.DriveFileScope},
		Endpoint:     google.Endpoint,
	}
}
