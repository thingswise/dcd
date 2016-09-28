package main

import (
	"fmt"
	"net/http"
	"time"
)

type ProgressHandler struct {
	Id       string `json:"id"`
	Progress int64  `json:"progress"`
	Total    int64  `json:"total"`
}

func (p *ProgressHandler) SetTotal(Total int64) {
	p.Total = Total
}

func (p *ProgressHandler) SetProgress(Progress int64) {
	p.Progress = Progress
}

func (p *ProgressHandler) SendJson(w http.ResponseWriter) error {
	w.Header().Add("content-type", "application/json")
	return SendJson(w, p)
}

type ClientProgressCallback func(progress int64, total int64, final bool)

type ClientProgressHandler struct {
	cb         ClientProgressCallback
	Id         string
	monitoring bool
	client     *Client
}

func NewClientProgressHandler(client *Client, cb ClientProgressCallback) *ClientProgressHandler {
	return &ClientProgressHandler{
		cb:     cb,
		Id:     fmt.Sprintf("%d", time.Now().UnixNano()),
		client: client,
	}
}

func (c *ClientProgressHandler) MonitorProgress() {
	c.monitoring = true
	for c.monitoring {
		if p, err := c.client.GetProgress(c.Id); err != nil {
			// skip error
		} else {
			c.cb(p.Progress, p.Total, false)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (c *ClientProgressHandler) StopMonitoring() {
	c.monitoring = false
}

func (c *ClientProgressHandler) ReportProgress(w *http.Response) {
	if p, err := c.client.GetProgressFromResp(w); err != nil {
		// skip error
	} else {
		c.cb(p.Progress, p.Total, true)
	}
}
