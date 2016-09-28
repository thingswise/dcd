package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
)

type Client struct {
	socket  string
	address string
	_type   string
	client  *http.Client
	ph      ClientProgressCallback
}

func NewClientUnixSocket(socket string, file string, ph ClientProgressCallback) *Client {
	return &Client{
		socket: socket,
		address: (&url.URL{
			Scheme: "http",
			Host:   "local-socket",
			Path:   file,
		}).String(),
		_type: "unix",
		client: &http.Client{
			Transport: &http.Transport{
				Dial: func(proto, addr string) (net.Conn, error) {
					return net.Dial("unix", socket)
				},
			},
		},
		ph: ph,
	}
}

func (c *Client) Get(w io.Writer) error {
	req, err := http.NewRequest("GET", c.address, nil)
	if err != nil {
		return err
	}

	var ph *ClientProgressHandler = nil

	if c.ph != nil {
		ph = NewClientProgressHandler(c, c.ph)
		req.URL.RawQuery = "progress=" + ph.Id
		defer ph.StopMonitoring()
		go ph.MonitorProgress()
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return getError(resp)
	}

	defer resp.Body.Close()
	if _, err := io.Copy(w, resp.Body); err != nil {
		return err
	}

	return nil
}

func (c *Client) Edit(force bool) error {
	req, err := http.NewRequest("EDIT", c.address, nil)
	if err != nil {
		return err
	}

	var ph *ClientProgressHandler = nil

	if c.ph != nil {
		ph = NewClientProgressHandler(c, c.ph)
		if force {
			req.URL.RawQuery = "progress=" + ph.Id + "&force=true"
		} else {
			req.URL.RawQuery = "progress=" + ph.Id
		}
		defer ph.StopMonitoring()
		go ph.MonitorProgress()
	} else {
		if force {
			req.URL.RawQuery = "force=true"
		}
	}

	log.Debug("Request: %s %s", req.Method, req.URL.String())

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return getError(resp)
	}

	if ph != nil {
		ph.StopMonitoring()
		ph.ReportProgress(resp)
	}

	return nil
}

func (c *Client) Commit(force bool) error {
	req, err := http.NewRequest("COMMIT", c.address, nil)
	if err != nil {
		return err
	}

	var ph *ClientProgressHandler = nil

	if c.ph != nil {
		ph = NewClientProgressHandler(c, c.ph)
		if force {
			req.URL.RawQuery = "progress=" + ph.Id + "&force=true"
		} else {
			req.URL.RawQuery = "progress=" + ph.Id
		}
		defer ph.StopMonitoring()
		go ph.MonitorProgress()
	} else {
		if force {
			req.URL.RawQuery = "force=true"
		}
	}

	log.Debug("Request: %s %s", req.Method, req.URL.String())

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return getError(resp)
	}

	if ph != nil {
		ph.StopMonitoring()
		ph.ReportProgress(resp)
	}

	return nil
}

func (c *Client) Update(force bool) error {
	req, err := http.NewRequest("UPDATE", c.address, nil)
	if err != nil {
		return err
	}

	var ph *ClientProgressHandler = nil

	if c.ph != nil {
		ph = NewClientProgressHandler(c, c.ph)
		if force {
			req.URL.RawQuery = "progress=" + ph.Id + "&force=true"
		} else {
			req.URL.RawQuery = "progress=" + ph.Id
		}
		defer ph.StopMonitoring()
		go ph.MonitorProgress()
	} else {
		if force {
			req.URL.RawQuery = "force=true"
		}
	}

	log.Debug("Request: %s %s", req.Method, req.URL.String())

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return getError(resp)
	}

	if ph != nil {
		ph.StopMonitoring()
		ph.ReportProgress(resp)
	}

	return nil
}

func (c *Client) GetProgressFromResp(resp *http.Response) (*ProgressHandler, error) {
	if resp.Header.Get("content-type") == "application/json" {
		msg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		var po ProgressHandler
		if err := json.Unmarshal(msg, &po); err != nil {
			return nil, err
		}

		return &po, nil
	} else {
		return nil, fmt.Errorf("No a JSON object")
	}
}

func (c *Client) GetProgress(id string) (*ProgressHandler, error) {
	if c.ph == nil {
		return nil, fmt.Errorf("No progress tracking configured")
	}

	req, err := http.NewRequest("PROGRESS", c.address, nil)
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = "progress=" + id

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, getError(resp)
	}

	return c.GetProgressFromResp(resp)
}

func getError(resp *http.Response) error {
	if resp.Header.Get("content-type") == "application/json" {
		msg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var msgObj ErrorMessage
		if err := json.Unmarshal(msg, &msgObj); err != nil {
			return err
		}

		return fmt.Errorf(msgObj.Message)
	} else {
		return fmt.Errorf("Unknown error")
	}
}
