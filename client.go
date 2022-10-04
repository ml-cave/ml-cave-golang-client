package ml_cave_golang_client

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/ml-cave/ml-cave-golang-client/config"
)

const ModelParams = "ModelParams"           // Name of HTTP-header with creating model parameters.
const AuthorizationHeader = "Authorization" // Name of Bearer authorization token
const ContentType = "Content-Type"          // Name of HTTP-header with info about the model packer.

// MLCaveAPIClient HTTP API client for MLCave server.
type MLCaveAPIClient struct {
	httpClient *http.Client
	URI        string
	Token      string
}

// NewMLCaveAPIClient init structure MLCaveAPIClient.
func NewMLCaveAPIClient(cfg *config.MLCaveClientConfig) *MLCaveAPIClient {
	return &MLCaveAPIClient{
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.TimeoutSec) * time.Second,
		},

		Token: cfg.Token,
		URI:   cfg.URI,
	}
}

// PushModel push model to MLCave storage.
func (o *MLCaveAPIClient) PushModel(serviceGroup string, model []byte, headers map[string][]byte) error {
	uri := fmt.Sprintf("%v/%v", o.URI, serviceGroup)
	return o.executeWithoutResponseBody(serviceGroup, http.MethodPost, uri, bytes.NewBuffer(model), headers)
}

// GetModel get model from MLCave storage.
func (o *MLCaveAPIClient) GetModel(serviceGroup string, skip int) ([]byte, error) {
	uri := fmt.Sprintf("%v/%v/%v", o.URI, serviceGroup, skip)
	return o.execute(serviceGroup, http.MethodGet, uri, nil, nil)
}

// UpdateStatusModel update status model to current.
func (o *MLCaveAPIClient) UpdateStatusModel(serviceGroup string, skip int) error {
	uri := fmt.Sprintf("%v/%v/%v", o.URI, serviceGroup, skip)
	return o.executeWithoutResponseBody(serviceGroup, http.MethodPut, uri, nil, nil)
}

func (o *MLCaveAPIClient) executeWithoutResponseBody(serviceGroup, method, uri string, body io.Reader, headers map[string][]byte) error {
	_, err := o.execute(serviceGroup, method, uri, body, headers)
	return err
}

func (o *MLCaveAPIClient) execute(serviceGroup, method, uri string, body io.Reader, headers map[string][]byte) ([]byte, error) {
	httpRequest, err := http.NewRequest(method, uri, body)

	if err != nil {
		errString := "execute() create http %v request, uri: %v, err: %v"
		errMessage := fmt.Errorf(errString, method, o.URI, err)

		return nil, errMessage
	}

	httpRequest.Header.Set(AuthorizationHeader, fmt.Sprintf("Bearer %v", o.Token))

	if headers != nil && headers[ModelParams] != nil {
		httpRequest.Header.Set(ModelParams, string(headers[ModelParams]))
	}

	if headers != nil && headers[ContentType] != nil {
		httpRequest.Header.Set(ContentType, string(headers[ContentType]))
	}

	response, err := o.httpClient.Do(httpRequest)
	if err != nil {
		errString := "execute() send http %v error, httpRequest: %+v. Error: %s"
		errMessage := fmt.Errorf(errString, method, httpRequest, err)

		return nil, errMessage
	}

	defer func() {
		err = response.Body.Close()
		if err != nil {
			glog.Errorf("response.Body.Close() error: %v", err)
		}
	}()

	if response.StatusCode != http.StatusOK {
		errString := "execute() received http status code: %d for service group: %v"
		errMessage := fmt.Errorf(errString, response.StatusCode, serviceGroup)
		return nil, errMessage
	}

	return io.ReadAll(response.Body)
}
