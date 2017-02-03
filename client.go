// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
)

const (
	// Version is the current version of Elastic.
	Version = "3.0.65"

	// DefaultUrl is the default endpoint of Elasticsearch on the local machine.
	// It is used e.g. when initializing a new Client without a specific URL.
	DefaultURL = "http://127.0.0.1:9200"

	// DefaultScheme is the default protocol scheme to use when sniffing
	// the Elasticsearch cluster.
	DefaultScheme = "http"

	// DefaultSendGetBodyAs is the HTTP method to use when elastic is sending
	// a GET request with a body.
	DefaultSendGetBodyAs = "GET"

	// DefaultGzipEnabled specifies if gzip compression is enabled by default.
	DefaultGzipEnabled = false

	// off is used to disable timeouts.
	off = -1 * time.Second
)

var (
	// ErrNoClient is raised when no Elasticsearch node is available.
	ErrNoClient = errors.New("no Elasticsearch node available")

	// ErrRetry is raised when a request cannot be executed after the configured
	// number of retries.
	ErrRetry = errors.New("cannot connect after several retries")

	// ErrTimeout is raised when a request timed out, e.g. when WaitForStatus
	// didn't return in time.
	ErrTimeout = errors.New("timeout")

	// noRetries is a retrier that does not retry.
	noRetries = NewStopRetrier()
)

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// Client is an Elasticsearch client. Create one by calling NewClient.
type Client struct {
	c *http.Client // net/http Client to use for requests

	mu                sync.RWMutex // guards the next block
	urls              []string     // set of URLs passed initially to the client
	urlIndex          int          // last used URL
	errorlog          Logger       // error log for critical messages
	infolog           Logger       // information log for e.g. response times
	tracelog          Logger       // trace log for debugging
	scheme            string       // http or https
	decoder           Decoder      // used to decode data sent from Elasticsearch
	basicAuth         bool         // indicates whether to send HTTP Basic Auth credentials
	basicAuthUsername string       // username for HTTP Basic Auth
	basicAuthPassword string       // password for HTTP Basic Auth
	sendGetBodyAs     string       // override for when sending a GET with a body
	requiredPlugins   []string     // list of required plugins
	gzipEnabled       bool         // gzip compression enabled or disabled (default)
	retrier           Retrier      // strategy for retries
}

// NewClient creates a new client to work with Elasticsearch.
//
// NewClient, by default, is meant to be long-lived and shared across
// your application. If you need a short-lived client, e.g. for request-scope,
// consider using NewSimpleClient instead.
//
// The caller can configure the new client by passing configuration options
// to the func.
//
// Example:
//
//   client, err := elastic.NewClient(
//     elastic.SetURL("http://127.0.0.1:9200", "http://127.0.0.1:9201"),
//     elastic.SetBasicAuth("user", "secret"))
//
// If no URL is configured, Elastic uses DefaultURL by default.
//
// If the sniffer is enabled (the default), the new client then sniffes
// the cluster via the Nodes Info API
// (see http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/cluster-nodes-info.html#cluster-nodes-info).
// It uses the URLs specified by the caller. The caller is responsible
// to only pass a list of URLs of nodes that belong to the same cluster.
// This sniffing process is run on startup and periodically.
// Use SnifferInterval to set the interval between two sniffs (default is
// 15 minutes). In other words: By default, the client will find new nodes
// in the cluster and remove those that are no longer available every
// 15 minutes. Disable the sniffer by passing SetSniff(false) to NewClient.
//
// The list of nodes found in the sniffing process will be used to make
// connections to the REST API of Elasticsearch. These nodes are also
// periodically checked in a shorter time frame. This process is called
// a health check. By default, a health check is done every 60 seconds.
// You can set a shorter or longer interval by SetHealthcheckInterval.
// Disabling health checks is not recommended, but can be done by
// SetHealthcheck(false).
//
// Connections are automatically marked as dead or healthy while
// making requests to Elasticsearch. When a request fails, Elastic will
// call into the Retry strategy which can be specified with SetRetry.
// The Retry strategy is also responsible for handling backoff i.e. the time
// to wait before starting the next request. There are various standard
// backoff implementations, e.g. ExponentialBackoff or SimpleBackoff.
// Retries are disabled by default.
//
// If no HttpClient is configured, then http.DefaultClient is used.
// You can use your own http.Client with some http.Transport for
// advanced scenarios.
//
// An error is also returned when some configuration option is invalid or
// the new client cannot sniff the cluster (if enabled).
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		c:             http.DefaultClient,
		scheme:        DefaultScheme,
		decoder:       &DefaultDecoder{},
		sendGetBodyAs: DefaultSendGetBodyAs,
		gzipEnabled:   DefaultGzipEnabled,
		retrier:       noRetries, // no retries by default
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	if len(c.urls) == 0 {
		c.urls = []string{DefaultURL}
	}
	c.urls = canonicalize(c.urls...)

	// Check the required plugins
	for _, plugin := range c.requiredPlugins {
		found, err := c.HasPlugin(plugin)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("elastic: plugin %s not found", plugin)
		}
	}

	return c, nil
}

// SetHttpClient can be used to specify the http.Client to use when making
// HTTP requests to Elasticsearch.
func SetHttpClient(httpClient *http.Client) ClientOptionFunc {
	return func(c *Client) error {
		if httpClient != nil {
			c.c = httpClient
		} else {
			c.c = http.DefaultClient
		}
		return nil
	}
}

// SetBasicAuth can be used to specify the HTTP Basic Auth credentials to
// use when making HTTP requests to Elasticsearch.
func SetBasicAuth(username, password string) ClientOptionFunc {
	return func(c *Client) error {
		c.basicAuthUsername = username
		c.basicAuthPassword = password
		c.basicAuth = c.basicAuthUsername != "" || c.basicAuthPassword != ""
		return nil
	}
}

// SetURL defines the URL endpoints of the Elasticsearch nodes. Notice that
// when sniffing is enabled, these URLs are used to initially sniff the
// cluster on startup.
func SetURL(urls ...string) ClientOptionFunc {
	return func(c *Client) error {
		switch len(urls) {
		case 0:
			c.urls = []string{DefaultURL}
		default:
			c.urls = urls
		}
		return nil
	}
}

// SetScheme sets the HTTP scheme to look for when sniffing (http or https).
// This is http by default.
func SetScheme(scheme string) ClientOptionFunc {
	return func(c *Client) error {
		c.scheme = scheme
		return nil
	}
}

// SetMaxRetries sets the maximum number of retries before giving up when
// performing a HTTP request to Elasticsearch.
//
// Deprecated: Replace with a Retry implementation.
func SetMaxRetries(maxRetries int) ClientOptionFunc {
	return func(c *Client) error {
		if maxRetries < 0 {
			return errors.New("MaxRetries must be greater than or equal to 0")
		} else if maxRetries == 0 {
			c.retrier = noRetries
		} else {
			// Create a Retrier that will wait for 100ms (+/- jitter) between requests.
			// This resembles the old behavior with maxRetries.
			ticks := make([]int, maxRetries)
			for i := 0; i < len(ticks); i++ {
				ticks[i] = 100
			}
			backoff := NewSimpleBackoff(ticks...)
			c.retrier = NewBackoffRetrier(backoff)
		}
		return nil
	}
}

// SetGzip enables or disables gzip compression (disabled by default).
func SetGzip(enabled bool) ClientOptionFunc {
	return func(c *Client) error {
		c.gzipEnabled = enabled
		return nil
	}
}

// SetDecoder sets the Decoder to use when decoding data from Elasticsearch.
// DefaultDecoder is used by default.
func SetDecoder(decoder Decoder) ClientOptionFunc {
	return func(c *Client) error {
		if decoder != nil {
			c.decoder = decoder
		} else {
			c.decoder = &DefaultDecoder{}
		}
		return nil
	}
}

// SetRequiredPlugins can be used to indicate that some plugins are required
// before a Client will be created.
func SetRequiredPlugins(plugins ...string) ClientOptionFunc {
	return func(c *Client) error {
		if c.requiredPlugins == nil {
			c.requiredPlugins = make([]string, 0)
		}
		c.requiredPlugins = append(c.requiredPlugins, plugins...)
		return nil
	}
}

// SetErrorLog sets the logger for critical messages like nodes joining
// or leaving the cluster or failing requests. It is nil by default.
func SetErrorLog(logger Logger) ClientOptionFunc {
	return func(c *Client) error {
		c.errorlog = logger
		return nil
	}
}

// SetInfoLog sets the logger for informational messages, e.g. requests
// and their response times. It is nil by default.
func SetInfoLog(logger Logger) ClientOptionFunc {
	return func(c *Client) error {
		c.infolog = logger
		return nil
	}
}

// SetTraceLog specifies the log.Logger to use for output of HTTP requests
// and responses which is helpful during debugging. It is nil by default.
func SetTraceLog(logger Logger) ClientOptionFunc {
	return func(c *Client) error {
		c.tracelog = logger
		return nil
	}
}

// SendGetBodyAs specifies the HTTP method to use when sending a GET request
// with a body. It is GET by default.
func SetSendGetBodyAs(httpMethod string) ClientOptionFunc {
	return func(c *Client) error {
		c.sendGetBodyAs = httpMethod
		return nil
	}
}

// SetRetrier specifies the retry strategy that handles errors during
// HTTP request/response with Elasticsearch.
func SetRetrier(retrier Retrier) ClientOptionFunc {
	return func(c *Client) error {
		if retrier == nil {
			retrier = noRetries // no retries by default
		}
		c.retrier = retrier
		return nil
	}
}

// String returns a string representation of the client status.
func (c *Client) String() string {
	var buf bytes.Buffer
	for i, u := range c.urls {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(u)
	}
	return buf.String()
}

func (c *Client) nextURL() string {
	c.mu.Lock()
	pick := c.urls[c.urlIndex]
	c.urlIndex++
	if c.urlIndex == len(c.urls) {
		c.urlIndex = 0
	}
	c.mu.Unlock()
	return pick
}

// errorf logs to the error log.
func (c *Client) errorf(format string, args ...interface{}) {
	if c.errorlog != nil {
		c.errorlog.Printf(format, args...)
	}
}

// infof logs informational messages.
func (c *Client) infof(format string, args ...interface{}) {
	if c.infolog != nil {
		c.infolog.Printf(format, args...)
	}
}

// tracef logs to the trace log.
func (c *Client) tracef(format string, args ...interface{}) {
	if c.tracelog != nil {
		c.tracelog.Printf(format, args...)
	}
}

// dumpRequest dumps the given HTTP request to the trace log.
func (c *Client) dumpRequest(r *http.Request) {
	if c.tracelog != nil {
		out, err := httputil.DumpRequestOut(r, true)
		if err == nil {
			c.tracef("%s\n", string(out))
		}
	}
}

// dumpResponse dumps the given HTTP response to the trace log.
func (c *Client) dumpResponse(resp *http.Response) {
	if c.tracelog != nil {
		out, err := httputil.DumpResponse(resp, true)
		if err == nil {
			c.tracef("%s\n", string(out))
		}
	}
}

// reSniffHostAndPort is used to extract hostname and port from a result
// from a Nodes Info API (example: "inet[/127.0.0.1:9200]").
var reSniffHostAndPort = regexp.MustCompile(`\/([^:]*):([0-9]+)\]`)

func (c *Client) extractHostname(scheme, address string) string {
	if strings.HasPrefix(address, "inet") {
		m := reSniffHostAndPort.FindStringSubmatch(address)
		if len(m) == 3 {
			return fmt.Sprintf("%s://%s:%s", scheme, m[1], m[2])
		}
	}
	s := address
	if idx := strings.Index(s, "/"); idx >= 0 {
		s = s[idx+1:]
	}
	if strings.Index(s, ":") < 0 {
		return ""
	}
	return fmt.Sprintf("%s://%s", scheme, s)
}

// PerformRequest does a HTTP request to Elasticsearch.
// It returns a response (which might be nil) and an error on failure.
//
// Optionally, a list of HTTP error codes to ignore can be passed.
// This is necessary for services that expect e.g. HTTP status 404 as a
// valid outcome (Exists, IndicesExists, IndicesTypeExists).
func (c *Client) PerformRequest(method, path string, params url.Values, body interface{}, ignoreErrors ...int) (*Response, error) {
	return c.PerformRequestC(nil, method, path, params, body, ignoreErrors...)
}

// PerformRequestC does a HTTP request to Elasticsearch.
// It returns a response and an error on failure.
//
// Optionally, a list of HTTP error codes to ignore can be passed.
// This is necessary for services that expect e.g. HTTP status 404 as a
// valid outcome (Exists, IndicesExists, IndicesTypeExists).
//
// If ctx is not nil, it uses the ctxhttp to do the request,
// enabling both request cancelation as well as timeout.
func (c *Client) PerformRequestC(ctx context.Context, method, path string, params url.Values, body interface{}, ignoreErrors ...int) (*Response, error) {
	start := time.Now().UTC()

	c.mu.RLock()
	basicAuth := c.basicAuth
	basicAuthUsername := c.basicAuthUsername
	basicAuthPassword := c.basicAuthPassword
	sendGetBodyAs := c.sendGetBodyAs
	gzipEnabled := c.gzipEnabled
	c.mu.RUnlock()

	var err error
	var req *Request
	var resp *Response
	var n int

	// Change method if sendGetBodyAs is specified.
	if method == "GET" && body != nil && sendGetBodyAs != "GET" {
		method = sendGetBodyAs
	}

	for {
		pathWithParams := path
		if len(params) > 0 {
			pathWithParams += "?" + params.Encode()
		}

		// Get a connection
		u := c.nextURL()
		req, err = NewRequest(method, u+pathWithParams)
		if err != nil {
			c.errorf("elastic: cannot create request for %s %s: %v", strings.ToUpper(method), u+pathWithParams, err)
			return nil, err
		}

		if basicAuth {
			req.SetBasicAuth(basicAuthUsername, basicAuthPassword)
		}

		// Set body
		if body != nil {
			err = req.SetBody(body, gzipEnabled)
			if err != nil {
				c.errorf("elastic: couldn't set body %+v for request: %v", body, err)
				return nil, err
			}
		}

		// Tracing
		c.dumpRequest((*http.Request)(req))

		// Get response
		var res *http.Response
		if ctx == nil {
			res, err = c.c.Do((*http.Request)(req))
		} else {
			res, err = ctxhttp.Do(ctx, c.c, (*http.Request)(req))
		}
		if err != nil {
			n++
			wait, ok, rerr := c.retrier.Retry(ctx, n, (*http.Request)(req), res, err)
			if rerr != nil {
				return nil, rerr
			}
			if !ok {
				return nil, err
			}
			time.Sleep(wait)
			continue // try again
		}
		if res.Body != nil {
			defer res.Body.Close()
		}

		// Check for errors
		if err := checkResponse((*http.Request)(req), res, ignoreErrors...); err != nil {
			// No retry if request succeeded
			// We still try to return a response.
			resp, _ = c.newResponse(res)
			return resp, err
		}

		// Tracing
		c.dumpResponse(res)

		resp, err = c.newResponse(res)
		if err != nil {
			return nil, err
		}

		break
	}

	duration := time.Now().UTC().Sub(start)
	c.infof("%s %s [status:%d, request:%.3fs]",
		strings.ToUpper(method),
		req.URL,
		resp.StatusCode,
		float64(int64(duration/time.Millisecond))/1000)

	return resp, nil
}

// -- Document APIs --

// Index a document.
func (c *Client) Index() *IndexService {
	return NewIndexService(c)
}

// Get a document.
func (c *Client) Get() *GetService {
	return NewGetService(c)
}

// MultiGet retrieves multiple documents in one roundtrip.
func (c *Client) MultiGet() *MgetService {
	return NewMgetService(c)
}

// Mget retrieves multiple documents in one roundtrip.
func (c *Client) Mget() *MgetService {
	return NewMgetService(c)
}

// Delete a document.
func (c *Client) Delete() *DeleteService {
	return NewDeleteService(c)
}

// DeleteByQuery deletes documents as found by a query.
func (c *Client) DeleteByQuery(indices ...string) *DeleteByQueryService {
	return NewDeleteByQueryService(c).Index(indices...)
}

// Update a document.
func (c *Client) Update() *UpdateService {
	return NewUpdateService(c)
}

// UpdateByQuery performs an update on a set of documents.
func (c *Client) UpdateByQuery(indices ...string) *UpdateByQueryService {
	return NewUpdateByQueryService(c).Index(indices...)
}

// Bulk is the entry point to mass insert/update/delete documents.
func (c *Client) Bulk() *BulkService {
	return NewBulkService(c)
}

// BulkProcessor allows setting up a concurrent processor of bulk requests.
func (c *Client) BulkProcessor() *BulkProcessorService {
	return NewBulkProcessorService(c)
}

// Reindex returns a service that will reindex documents from a source
// index into a target index.
//
// Notice that this Reindexer is an Elastic-specific solution that pre-dated
// the Reindex API introduced in Elasticsearch 2.3.0 (see ReindexTask).
//
// See http://www.elastic.co/guide/en/elasticsearch/guide/current/reindex.html
// for more information about reindexing.
func (c *Client) Reindex(sourceIndex, targetIndex string) *Reindexer {
	return NewReindexer(c, sourceIndex, CopyToTargetIndex(targetIndex))
}

// ReindexTask copies data from a source index into a destination index.
//
// The Reindex API has been introduced in Elasticsearch 2.3.0. Notice that
// there is a Elastic-specific Reindexer that pre-dates the Reindex API from
// Elasticsearch. If you rely on that, use the ReindexerService via
// Client.Reindex.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html
// for details on the Reindex API.
func (c *Client) ReindexTask() *ReindexService {
	return NewReindexService(c)
}

// TermVectors returns information and statistics on terms in the fields
// of a particular document.
func (c *Client) TermVectors(index, typ string) *TermvectorsService {
	builder := NewTermvectorsService(c)
	builder = builder.Index(index).Type(typ)
	return builder
}

// MultiTermVectors returns information and statistics on terms in the fields
// of multiple documents.
func (c *Client) MultiTermVectors() *MultiTermvectorService {
	return NewMultiTermvectorService(c)
}

// -- Search APIs --

// Search is the entry point for searches.
func (c *Client) Search(indices ...string) *SearchService {
	return NewSearchService(c).Index(indices...)
}

// Suggest returns a service to return suggestions.
func (c *Client) Suggest(indices ...string) *SuggestService {
	return NewSuggestService(c).Index(indices...)
}

// MultiSearch is the entry point for multi searches.
func (c *Client) MultiSearch() *MultiSearchService {
	return NewMultiSearchService(c)
}

// Count documents.
func (c *Client) Count(indices ...string) *CountService {
	return NewCountService(c).Index(indices...)
}

// Explain computes a score explanation for a query and a specific document.
func (c *Client) Explain(index, typ, id string) *ExplainService {
	return NewExplainService(c).Index(index).Type(typ).Id(id)
}

// Percolate allows to send a document and return matching queries.
// See http://www.elastic.co/guide/en/elasticsearch/reference/current/search-percolate.html.
func (c *Client) Percolate() *PercolateService {
	return NewPercolateService(c)
}

// TODO Search Template
// TODO Search Shards API
// TODO Search Exists API
// TODO Validate API

// FieldStats returns statistical information about fields in indices.
func (c *Client) FieldStats(indices ...string) *FieldStatsService {
	return NewFieldStatsService(c).Index(indices...)
}

// Exists checks if a document exists.
func (c *Client) Exists() *ExistsService {
	return NewExistsService(c)
}

// Scan through documents. Use this to iterate inside a server process
// where the results will be processed without returning them to a client.
func (c *Client) Scan(indices ...string) *ScanService {
	return NewScanService(c).Index(indices...)
}

// Scroll through documents. Use this to efficiently scroll through results
// while returning the results to a client. Use Scan when you don't need
// to return requests to a client (i.e. not paginating via request/response).
func (c *Client) Scroll(indices ...string) *ScrollService {
	return NewScrollService(c).Index(indices...)
}

// ClearScroll can be used to clear search contexts manually.
func (c *Client) ClearScroll(scrollIds ...string) *ClearScrollService {
	return NewClearScrollService(c).ScrollId(scrollIds...)
}

// -- Indices APIs --

// CreateIndex returns a service to create a new index.
func (c *Client) CreateIndex(name string) *IndicesCreateService {
	return NewIndicesCreateService(c).Index(name)
}

// DeleteIndex returns a service to delete an index.
func (c *Client) DeleteIndex(indices ...string) *IndicesDeleteService {
	return NewIndicesDeleteService(c).Index(indices)
}

// IndexExists allows to check if an index exists.
func (c *Client) IndexExists(indices ...string) *IndicesExistsService {
	return NewIndicesExistsService(c).Index(indices)
}

// TypeExists allows to check if one or more types exist in one or more indices.
func (c *Client) TypeExists() *IndicesExistsTypeService {
	return NewIndicesExistsTypeService(c)
}

// IndexStats provides statistics on different operations happining
// in one or more indices.
func (c *Client) IndexStats(indices ...string) *IndicesStatsService {
	return NewIndicesStatsService(c).Index(indices...)
}

// OpenIndex opens an index.
func (c *Client) OpenIndex(name string) *IndicesOpenService {
	return NewIndicesOpenService(c).Index(name)
}

// CloseIndex closes an index.
func (c *Client) CloseIndex(name string) *IndicesCloseService {
	return NewIndicesCloseService(c).Index(name)
}

// IndexGet retrieves information about one or more indices.
// IndexGet is only available for Elasticsearch 1.4 or later.
func (c *Client) IndexGet(indices ...string) *IndicesGetService {
	return NewIndicesGetService(c).Index(indices...)
}

// IndexGetSettings retrieves settings of all, one or more indices.
func (c *Client) IndexGetSettings(indices ...string) *IndicesGetSettingsService {
	return NewIndicesGetSettingsService(c).Index(indices...)
}

// IndexPutSettings sets settings for all, one or more indices.
func (c *Client) IndexPutSettings(indices ...string) *IndicesPutSettingsService {
	return NewIndicesPutSettingsService(c).Index(indices...)
}

// IndexAnalyze performs the analysis process on a text and returns the
// token breakdown of the text.
func (c *Client) IndexAnalyze() *IndicesAnalyzeService {
	return NewIndicesAnalyzeService(c)
}

// Optimize asks Elasticsearch to optimize one or more indices.
// Optimize is deprecated as of Elasticsearch 2.1 and replaced by Forcemerge.
func (c *Client) Optimize(indices ...string) *OptimizeService {
	return NewOptimizeService(c).Index(indices...)
}

// Forcemerge optimizes one or more indices.
// It replaces the deprecated Optimize API.
func (c *Client) Forcemerge(indices ...string) *IndicesForcemergeService {
	return NewIndicesForcemergeService(c).Index(indices...)
}

// Refresh asks Elasticsearch to refresh one or more indices.
func (c *Client) Refresh(indices ...string) *RefreshService {
	return NewRefreshService(c).Index(indices...)
}

// Flush asks Elasticsearch to free memory from the index and
// flush data to disk.
func (c *Client) Flush(indices ...string) *IndicesFlushService {
	return NewIndicesFlushService(c).Index(indices...)
}

// Alias enables the caller to add and/or remove aliases.
func (c *Client) Alias() *AliasService {
	return NewAliasService(c)
}

// Aliases returns aliases by index name(s).
func (c *Client) Aliases() *AliasesService {
	return NewAliasesService(c)
}

// GetTemplate gets a search template.
// Use IndexXXXTemplate funcs to manage index templates.
func (c *Client) GetTemplate() *GetTemplateService {
	return NewGetTemplateService(c)
}

// PutTemplate creates or updates a search template.
// Use IndexXXXTemplate funcs to manage index templates.
func (c *Client) PutTemplate() *PutTemplateService {
	return NewPutTemplateService(c)
}

// DeleteTemplate deletes a search template.
// Use IndexXXXTemplate funcs to manage index templates.
func (c *Client) DeleteTemplate() *DeleteTemplateService {
	return NewDeleteTemplateService(c)
}

// IndexGetTemplate gets an index template.
// Use XXXTemplate funcs to manage search templates.
func (c *Client) IndexGetTemplate(names ...string) *IndicesGetTemplateService {
	return NewIndicesGetTemplateService(c).Name(names...)
}

// IndexTemplateExists gets check if an index template exists.
// Use XXXTemplate funcs to manage search templates.
func (c *Client) IndexTemplateExists(name string) *IndicesExistsTemplateService {
	return NewIndicesExistsTemplateService(c).Name(name)
}

// IndexPutTemplate creates or updates an index template.
// Use XXXTemplate funcs to manage search templates.
func (c *Client) IndexPutTemplate(name string) *IndicesPutTemplateService {
	return NewIndicesPutTemplateService(c).Name(name)
}

// IndexDeleteTemplate deletes an index template.
// Use XXXTemplate funcs to manage search templates.
func (c *Client) IndexDeleteTemplate(name string) *IndicesDeleteTemplateService {
	return NewIndicesDeleteTemplateService(c).Name(name)
}

// GetMapping gets a mapping.
func (c *Client) GetMapping() *IndicesGetMappingService {
	return NewIndicesGetMappingService(c)
}

// PutMapping registers a mapping.
func (c *Client) PutMapping() *IndicesPutMappingService {
	return NewIndicesPutMappingService(c)
}

// GetWarmer gets one or more warmers by name.
func (c *Client) GetWarmer() *IndicesGetWarmerService {
	return NewIndicesGetWarmerService(c)
}

// PutWarmer registers a warmer.
func (c *Client) PutWarmer() *IndicesPutWarmerService {
	return NewIndicesPutWarmerService(c)
}

// DeleteWarmer deletes one or more warmers.
func (c *Client) DeleteWarmer() *IndicesDeleteWarmerService {
	return NewIndicesDeleteWarmerService(c)
}

// -- cat APIs --

// TODO cat aliases
// TODO cat allocation
// TODO cat count
// TODO cat fielddata
// TODO cat health
// TODO cat indices
// TODO cat master
// TODO cat nodes
// TODO cat pending tasks
// TODO cat plugins
// TODO cat recovery
// TODO cat thread pool
// TODO cat shards
// TODO cat segments

// -- Cluster APIs --

// ClusterHealth retrieves the health of the cluster.
func (c *Client) ClusterHealth() *ClusterHealthService {
	return NewClusterHealthService(c)
}

// ClusterState retrieves the state of the cluster.
func (c *Client) ClusterState() *ClusterStateService {
	return NewClusterStateService(c)
}

// ClusterStats retrieves cluster statistics.
func (c *Client) ClusterStats() *ClusterStatsService {
	return NewClusterStatsService(c)
}

// NodesInfo retrieves one or more or all of the cluster nodes information.
func (c *Client) NodesInfo() *NodesInfoService {
	return NewNodesInfoService(c)
}

// NodesStats retrieves one or more or all of the cluster nodes statistics.
func (c *Client) NodesStats() *NodesStatsService {
	return NewNodesStatsService(c)
}

// TasksCancel cancels tasks running on the specified nodes.
func (c *Client) TasksCancel() *TasksCancelService {
	return NewTasksCancelService(c)
}

// TasksList retrieves the list of tasks running on the specified nodes.
func (c *Client) TasksList() *TasksListService {
	return NewTasksListService(c)
}

// TODO Pending cluster tasks
// TODO Cluster Reroute
// TODO Cluster Update Settings
// TODO Nodes Stats
// TODO Nodes hot_threads

// -- Snapshot and Restore --

// TODO Snapshot Create
// TODO Snapshot Create Repository
// TODO Snapshot Delete
// TODO Snapshot Delete Repository
// TODO Snapshot Get
// TODO Snapshot Get Repository
// TODO Snapshot Restore
// TODO Snapshot Status
// TODO Snapshot Verify Repository

// -- Helpers and shortcuts --

// ElasticsearchVersion returns the version number of Elasticsearch
// running on the given URL.
func (c *Client) ElasticsearchVersion(url string) (string, error) {
	res, _, err := c.Ping(url).Do()
	if err != nil {
		return "", err
	}
	return res.Version.Number, nil
}

// IndexNames returns the names of all indices in the cluster.
func (c *Client) IndexNames() ([]string, error) {
	res, err := c.IndexGetSettings().Index("_all").Do()
	if err != nil {
		return nil, err
	}
	var names []string
	for name := range res {
		names = append(names, name)
	}
	return names, nil
}

// Ping checks if a given node in a cluster exists and (optionally)
// returns some basic information about the Elasticsearch server,
// e.g. the Elasticsearch version number.
//
// Notice that you need to specify a URL here explicitly.
func (c *Client) Ping(url string) *PingService {
	return NewPingService(c).URL(url)
}

// WaitForStatus waits for the cluster to have the given status.
// This is a shortcut method for the ClusterHealth service.
//
// WaitForStatus waits for the specified timeout, e.g. "10s".
// If the cluster will have the given state within the timeout, nil is returned.
// If the request timed out, ErrTimeout is returned.
func (c *Client) WaitForStatus(status string, timeout string) error {
	health, err := c.ClusterHealth().WaitForStatus(status).Timeout(timeout).Do()
	if err != nil {
		return err
	}
	if health.TimedOut {
		return ErrTimeout
	}
	return nil
}

// WaitForGreenStatus waits for the cluster to have the "green" status.
// See WaitForStatus for more details.
func (c *Client) WaitForGreenStatus(timeout string) error {
	return c.WaitForStatus("green", timeout)
}

// WaitForYellowStatus waits for the cluster to have the "yellow" status.
// See WaitForStatus for more details.
func (c *Client) WaitForYellowStatus(timeout string) error {
	return c.WaitForStatus("yellow", timeout)
}
