// Package graphite defines structures for interacting with a Graphite server.
package graphite

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Request holds query objects. Currently only absolute times are supported.
type Request struct {
	Start   *time.Time
	End     *time.Time
	Targets []string
}

type Graphite struct {
    u url.URL
    httpClient *http.Client
}

func NewUrl(urlString string) (*Graphite, error) {
    url, err := url.Parse(urlString)
    if err != nil {
        return nil,err
    }
    return &Graphite{*url,&http.Client{}}, nil
}
func New(scheme, host, path string) *Graphite {
    if scheme == "" {
        scheme = "http"
    }
    return &Graphite{
        url.URL{
            Scheme: scheme,
            Host: host,
            Path: path,
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


func (g *Graphite) Query(r *Request) (RenderResponse, error) {
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
    // there's a max url length, so we can only use GET up to a certain point.
    if len(v["target"]) < 30 {
        u.RawQuery= v.Encode()
        resp, err = g.httpClient.Get(u.String())
    } else {
        resp, err = g.httpClient.PostForm(u.String(), v)
        fmt.Println("PostForm err", err)
    }
	if err != nil {
		return RenderResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
        fmt.Println("statuscode err", resp.Status)
		return RenderResponse{}, errors.New(resp.Status)
	}
	var series RenderResponse
	err = json.NewDecoder(resp.Body).Decode(&series)
    fmt.Println("json decode err", err)
	return series, err
}
