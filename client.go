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
}

func NewClientUnixSocket(socket string, file string) *Client {
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
	}
}

func (c *Client) Get(w io.Writer) error {
	resp, err := c.client.Get(c.address)
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

func (c *Client) Edit() error {
	req, err := http.NewRequest("EDIT", c.address, nil)
	if err != nil {
		return err
	}

	log.Debug("Request: %s %s", req.Method, req.URL.String())

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return getError(resp)
	}

	return nil
}

func (c *Client) Commit(force bool) error {
	req, err := http.NewRequest("COMMIT", c.address, nil)
	if err != nil {
		return err
	}

	if force {
		req.URL.RawQuery = "force=true"
	}

	log.Debug("Request: %s %s", req.Method, req.URL.String())

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return getError(resp)
	}

	return nil
}

func (c *Client) Update(force bool) error {
	req, err := http.NewRequest("UPDATE", c.address, nil)
	if err != nil {
		return err
	}

	if force {
		req.URL.RawQuery = "force=true"
	}

	log.Debug("Request: %s %s", req.Method, req.URL.String())

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return getError(resp)
	}

	return nil
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
