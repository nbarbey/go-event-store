package httpclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"resty.dev/v3"
)

type Client[E any] struct {
	client *resty.Client
}

func (c *Client[E]) Publish(ctx context.Context, event E) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = c.client.R().WithContext(ctx).SetBody(body).Post("/publish")
	return err
}

func (c *Client[E]) All(ctx context.Context) ([]E, error) {
	response, err := c.client.R().WithContext(ctx).Get("/all")
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	var events []E
	err = json.Unmarshal(body, &events)
	return events, err
}

func (c *Client[E]) SetBaseURL(url string) *Client[E] {
	c.client.SetBaseURL(url)
	return c
}

func NewClient[E any](c *http.Client) *Client[E] {
	return &Client[E]{client: resty.NewWithClient(c)}
}
