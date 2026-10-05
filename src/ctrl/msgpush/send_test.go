package msgpush

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"net/textproto"
	"strings"
	"testing"
	"time"
)

func TestSendLarkChecksResponseAndEscapesText(t *testing.T) {
	old := feishuClient
	defer func() { feishuClient = old }()
	code := 999
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		var text map[string]string
		if err := json.Unmarshal([]byte(body["content"]), &text); err != nil {
			t.Error(err)
		}
		if text["text"] != "quote \" and\nnewline" {
			t.Errorf("text changed: %q", text["text"])
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"code": code})
	})
	feishuClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Result(), nil
	})}
	if err := SendMessage("test", "test", "quote \" and\nnewline"); err == nil {
		t.Fatal("business failure accepted")
	}
	code = 0
	if err := SendMessage("test", "test", "quote \" and\nnewline"); err != nil {
		t.Fatal(err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A failed QUIT after a positive DATA reply must not cause another delivery.
func TestSMTPAcceptanceSurvivesQuitFailure(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	_ = clientConn.SetDeadline(time.Now().Add(3 * time.Second))
	_ = serverConn.SetDeadline(time.Now().Add(3 * time.Second))
	finished := make(chan error, 1)
	go func() {
		tp := textproto.NewConn(serverConn)
		defer tp.Close()
		if err := tp.PrintfLine("220 localhost SMTP"); err != nil {
			finished <- err
			return
		}
		for {
			line, err := tp.ReadLine()
			if err != nil {
				finished <- err
				return
			}
			switch {
			case strings.HasPrefix(line, "EHLO"), strings.HasPrefix(line, "HELO"), strings.HasPrefix(line, "MAIL"), strings.HasPrefix(line, "RCPT"):
				err = tp.PrintfLine("250 ok")
			case line == "DATA":
				err = tp.PrintfLine("354 send body")
				if err == nil {
					_, err = tp.ReadDotBytes()
				}
				if err == nil {
					err = tp.PrintfLine("250 queued")
				}
			case line == "QUIT":
				err = tp.PrintfLine("500 quit failure")
				finished <- err
				return
			default:
				finished <- fmt.Errorf("unexpected SMTP command %q", line)
				return
			}
			if err != nil {
				finished <- err
				return
			}
		}
	}()
	client, err := smtp.NewClient(clientConn, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := sendEmail(client, "sender@example.com", "recipient@example.com", "test", "test body"); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}
