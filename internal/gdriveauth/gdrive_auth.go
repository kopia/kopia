package gdriveauth

import (
	_ "embed"
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/kopia/kopia/repo/blob/gdrive"
	"golang.org/x/oauth2"
)

//go:embed index.html
var htmlTemplate string

type gdriveAuthServer struct {
	gdrive.OAuthConfig
	result chan Result
	config oauth2.Config
	token  *oauth2.Token
	srv    *http.Server
}

// Result is the server's successful return value.
type Result struct {
	oauth2.Token
	FolderId string
}

type ExchangeTokenRequest struct {
	// AuthCode is used to exchange for a long-term refresh token.
	AuthCode string `json:"authCode"`
}

type ExchangeTokenResponse struct {
	// AccessToken provides access for the duration of the session (1hr).
	AccessToken string `json:"accessToken"`
}

type SaveFolderIdRequest struct {
	FolderId string `json:"folderId"`
}
type SaveFolderIdResponse struct {
}

type apiRequestHandler = func(w http.ResponseWriter, r *http.Request)

func (s *gdriveAuthServer) handleServeIndex() apiRequestHandler {
	return func(w http.ResponseWriter, req *http.Request) {
		t, err := template.New("gdrive_auth_template").Parse(htmlTemplate)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		t.Execute(w, s.OAuthConfig)
	}
}

func (s *gdriveAuthServer) handleExchangeToken() apiRequestHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ExchangeTokenRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		tok, err := s.config.Exchange(r.Context(), req.AuthCode)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		s.token = tok

		res := ExchangeTokenResponse{AccessToken: tok.AccessToken}
		json.NewEncoder(w).Encode(res)
	}
}

func (s *gdriveAuthServer) handleSaveFolderId() apiRequestHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SaveFolderIdRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		folderId := req.FolderId
		s.result <- Result{
			Token:    *s.token,
			FolderId: folderId,
		}
		close(s.result)

		res := SaveFolderIdResponse{}
		json.NewEncoder(w).Encode(res)

		s.srv.Shutdown(r.Context())
	}
}

func New(opt *gdrive.OAuthConfig, result chan Result) error {
	srv := &gdriveAuthServer{
		OAuthConfig: *opt,
		result:      result,
		config:      *gdrive.CreateGoogleOAuth2Config(opt),
	}

	r := mux.NewRouter()
	r.HandleFunc("/", srv.handleServeIndex()).Methods("GET")
	r.HandleFunc("/api/exchange-token", srv.handleExchangeToken()).Methods("POST")
	r.HandleFunc("/api/save-folder-id", srv.handleSaveFolderId()).Methods("POST")

	http_server := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}
	srv.srv = http_server
	return http_server.ListenAndServe()
}
