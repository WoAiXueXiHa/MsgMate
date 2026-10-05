package msgpush

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

var feishuBaseURL = "https://open.feishu.cn"
var feishuClient = &http.Client{Timeout: 15 * time.Second}

func feishuRequest(path, token string, body interface{}, result interface{}) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, feishuBaseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := feishuClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("feishu HTTP status %d", resp.StatusCode)
	}
	var status struct {
		Code *int   `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return err
	}
	if status.Code == nil {
		return fmt.Errorf("feishu response missing code")
	}
	if *status.Code != 0 {
		return fmt.Errorf("feishu business error %d", *status.Code)
	}
	return json.Unmarshal(raw, result)
}
func GetAccessToken() (string, error) {
	id, secret := os.Getenv("FEISHU_APP_ID"), os.Getenv("FEISHU_APP_SECRET")
	if id == "" || secret == "" {
		return "", fmt.Errorf("FEISHU_APP_ID and FEISHU_APP_SECRET must be set")
	}
	var result struct {
		Token string `json:"tenant_access_token"`
	}
	err := feishuRequest("/open-apis/auth/v3/tenant_access_token/internal/", "", map[string]string{"app_id": id, "app_secret": secret}, &result)
	if err != nil {
		return "", err
	}
	if result.Token == "" {
		return "", fmt.Errorf("feishu token missing")
	}
	return result.Token, nil
}
func SendMessage(token, to, content string) error {
	text, err := json.Marshal(map[string]string{"text": content})
	if err != nil {
		return err
	}
	var result json.RawMessage
	return feishuRequest("/open-apis/im/v1/messages?receive_id_type=open_id", token, map[string]string{"receive_id": to, "content": string(text), "msg_type": "text"}, &result)
}
