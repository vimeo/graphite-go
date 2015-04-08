// Package graphite defines structures for interacting with a Graphite server.
package graphite

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Request holds query objects. Currently only absolute times are supported.
type Request struct {
	Start   *time.Time
	End     *time.Time
	Targets []string
}

type Graphite struct {
	u          url.URL
	httpClient *http.Client
}

func NewUrl(urlString string) (*Graphite, error) {
	url, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}
	return &Graphite{*url, &http.Client{}}, nil
}
func New(scheme, host, path string) *Graphite {
	if scheme == "" {
		scheme = "http"
	}
	return &Graphite{
		url.URL{
			Scheme: scheme,
			Host:   host,
			Path:   path,
		},
		&http.Client{},
	}
}

type RenderResponse []Series
type MetricsResponse []string

type Series struct {
	Datapoints []DataPoint
	Target     string
}

type DataPoint []json.Number

func (g *Graphite) Metrics() (metrics MetricsResponse, err error) {
	u := g.u
	u.Path += "/metrics/index.json"

	resp, err := g.httpClient.Get(u.String())
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}
	err = json.NewDecoder(resp.Body).Decode(&metrics)
	return
}

func queryWorker(wg *sync.WaitGroup, reqChan chan *Request, respChan chan *RenderResponse, errChan chan error, g *Graphite) {
	for r := range reqChan {
		resp, err := g.Query(r)
		respChan <- &resp
		errChan <- err
	}
	wg.Done()
}

func (g *Graphite) Query(r *Request) (RenderResponse, error) {
	if len(r.Targets) > 20 {
		workers := 10
		reqChan := make(chan *Request)
		respChan := make(chan *RenderResponse)
		errChan := make(chan error)

		// start workers
		wg := sync.WaitGroup{}
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go queryWorker(&wg, reqChan, respChan, errChan, g)
		}

		stopFeeding := make(chan struct{})
		// start sending jobs to workers in background
		go func(start, end *time.Time, targets []string) {
			for {
				upper := 20
				if upper > len(targets) {
					upper = len(targets)
				}
				section := targets[0:upper]
				select {
				case <-stopFeeding:
					close(reqChan)
					return
				default:
					reqChan <- &Request{
						start,
						end,
						section,
					}
				}
				if len(targets) > 20 {
					targets = targets[20:]
				} else {
					break
				}
			}
			close(reqChan)
		}(r.Start, r.End, r.Targets)

		totalResp := *new(RenderResponse)
		go func() {
			wg.Wait()
			fmt.Println("workers done")
			close(respChan)
			close(errChan)
		}()
		for {
			resp, ok := <-respChan
			err := <-errChan
			if err != nil {
				stopFeeding <- struct{}{}
				return RenderResponse{}, err
			}
			if !ok {
				break
			}
			totalResp = append(totalResp, *resp...)
		}
		return totalResp, nil
	}

	u := g.u
	u.Path += "/render"
	v := url.Values{
		"format": []string{"json"},
		"target": r.Targets,
	}
	if r.Start != nil {
		v.Add("from", fmt.Sprint(r.Start.Unix()))
	}
	if r.End != nil {
		v.Add("until", fmt.Sprint(r.End.Unix()))
	}
	var resp *http.Response
	var err error
	u.RawQuery = v.Encode()
	resp, err = g.httpClient.Get(u.String())
	if err != nil {
		return RenderResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return RenderResponse{}, errors.New(resp.Status)
	}
	var series RenderResponse
	err = json.NewDecoder(resp.Body).Decode(&series)
	return series, err
}
