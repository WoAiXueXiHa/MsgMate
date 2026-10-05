// Command acceptance sends at most two real emails per mode and records IDs before polling.
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	mysql "github.com/go-sql-driver/mysql"
)

type result struct {
	Code       int    `json:"code"`
	TemplateID string `json:"templateID"`
	MsgID      string `json:"msgID"`
	Status     int    `json:"status"`
	RetryCount int    `json:"retryCount"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	file := flag.String("config-file", "", "private config")
	mode := flag.String("mode", "mysql", "mysql or kafka")
	recipient := flag.String("recipient", "", "explicit test recipient")
	base := flag.String("base", "http://msgmate-runtime:18109", "API base")
	ledger := flag.String("ledger", "/tmp/msgmate-mail-ledger.json", "persistent send ledger")
	flag.Parse()
	if *recipient == "" || (*mode != "mysql" && *mode != "kafka") {
		return fmt.Errorf("explicit recipient and valid mode required")
	}
	var cf config.TomlConfig
	if _, err := toml.DecodeFile(*file, &cf); err != nil {
		return err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	source := "msgmate-smoke-" + *mode
	call := func(method, path string, body interface{}) (result, error) {
		var out result
		raw, _ := json.Marshal(body)
		req, err := http.NewRequest(method, *base+path, bytes.NewReader(raw))
		if err != nil {
			return out, err
		}
		req.Header.Set("Source-Id", source)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return out, err
		}
		defer resp.Body.Close()
		err = json.NewDecoder(resp.Body).Decode(&out)
		if err == nil && out.Code != 0 {
			err = fmt.Errorf("API code %d", out.Code)
		}
		return out, err
	}

	// Startup can wait for Kafka topic leaders. Wait for the HTTP listener before submitting.
	readyDeadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := client.Get(*base + "/msg/get_template?templateID=msgmate-readiness-probe")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(readyDeadline) {
			return fmt.Errorf("service not ready: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	ids := map[string]string{}
	if raw, err := os.ReadFile(*ledger); err == nil {
		if err := json.Unmarshal(raw, &ids); err != nil {
			return err
		}
	}
	// Connect only to activate the test's own newly-created template.
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = cf.MySQL.Url
	cfg.User = cf.MySQL.User
	cfg.Passwd = cf.MySQL.Pwd
	cfg.DBName = cf.MySQL.Dbname
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	for _, kind := range []string{"immediate", "timer"} {
		key := *mode + "-" + kind
		id := ids[key]
		if id == "" {
			tp, err := call("POST", "/msg/create_template", map[string]interface{}{"sourceID": source, "name": key, "subject": "MsgMate 验收 " + key, "channel": 1, "content": "MsgMate {{.mode}} {{.kind}} 链路验收邮件。"})
			if err != nil {
				return err
			}
			if _, err := db.Exec("UPDATE t_msg_template SET status=2 WHERE template_id=?", tp.TemplateID); err != nil {
				return err
			}
			body := map[string]interface{}{"to": *recipient, "templateID": tp.TemplateID, "priority": 1, "templateData": map[string]string{"mode": *mode, "kind": kind}}
			if kind == "timer" {
				body["sendTimestamp"] = time.Now().Unix() + 5
			}
			sent, err := call("POST", "/msg/send_msg", body)
			if err != nil {
				return err
			}
			id = sent.MsgID
			ids[key] = id
			raw, _ := json.MarshalIndent(ids, "", "  ")
			if err := os.WriteFile(*ledger, raw, 0600); err != nil {
				return err
			}
			fmt.Printf("submitted %s msgID=%s\n", key, id)
		}
		deadline := time.Now().Add(60 * time.Second)
		for {
			r, err := call("GET", "/msg/get_msg_record?msgID="+id, nil)
			if err != nil {
				return err
			}
			if r.Status == 2 {
				fmt.Printf("SMTP accepted %s msgID=%s retries=%d\n", key, id, r.RetryCount)
				break
			}
			if r.Status == 3 {
				return fmt.Errorf("delivery failed %s msgID=%s retries=%d", key, id, r.RetryCount)
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("delivery pending after timeout: %s", id)
			}
			time.Sleep(time.Second)
		}
	}
	return nil
}
