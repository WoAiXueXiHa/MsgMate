// Isolated full-chain benchmark. This executable never registers real channels.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/consumer"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"github.com/WoAiXueXiHa/MsgMate/src/initialize"
	"github.com/gin-gonic/gin"
)

var calls atomic.Int64
var mockDelay time.Duration

type mock struct{ consumer.MsgBase }

func (p *mock) SendMsg() error { time.Sleep(mockDelay); calls.Add(1); return nil }
func main() {
	action := flag.String("action", "server", "server or load")
	rate := flag.Int("rate", 20, "offered requests per second")
	seconds := flag.Int("seconds", 30, "load seconds")
	delay := flag.Int("delay-ms", 10, "simulated channel delay")
	flag.Parse()
	if *action == "load" {
		load(*rate, *seconds)
		return
	}
	mockDelay = time.Duration(*delay) * time.Millisecond
	config.Init()
	if !strings.HasSuffix(config.Conf.MySQL.Dbname, "_test") {
		panic("database must end in _test")
	}
	dt, err := data.NewData(config.Conf)
	must(err)
	// Register only the simulated email channel: no credentials or external calls.
	consumer.RegisterHandler(&consumer.MsgHandler{Channel: 1, NewProc: func() consumer.MsgIntf { return &mock{} }})
	must(data.RecoverPending(dt.GetDB(), config.Conf.Common.MySQLAsMq))
	must(dt.GetDB().Where("template_id = ?", "perf-template").FirstOrCreate(&data.MsgTemplate{TemplateID: "perf-template", SourceID: "perf-send", Name: "perf", Subject: "perf", Content: "{{.name}}", Channel: 1, Status: 2}).Error)
	must(dt.GetDB().Exec("UPDATE t_global_quota SET num=10000000,unit=1000 WHERE channel=1").Error)
	cs := consumer.NewMsgConsume()
	cs.Consume()
	var timer consumer.TimerMsgConsume
	timer.Consume()
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	initialize.RegisterRouter(r)
	r.GET("/bench/stats", func(c *gin.Context) {
		var rows []struct {
			Status int
			N      int64
		}
		err := dt.GetDB().Table("t_msg_record").Select("status, COUNT(*) AS n").Group("status").Scan(&rows).Error
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		s := map[string]int64{"pending": 0, "success": 0, "failed": 0, "total": 0, "calls": calls.Load()}
		for _, row := range rows {
			s[map[int]string{1: "pending", 2: "success", 3: "failed"}[row.Status]] = row.N
			s["total"] += row.N
		}
		c.JSON(200, s)
	})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() { <-sig; timer.Unlock(); cs.UnlockAll(); dt.Close(); os.Exit(0) }()
	must(r.Run(":18081"))
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}

var client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 64}}

func snapshot() map[string]int64 {
	resp, err := client.Get("http://runtime:18081/bench/stats")
	must(err)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		panic("stats query failed")
	}
	s := map[string]int64{}
	must(json.NewDecoder(resp.Body).Decode(&s))
	return s
}
func emit(v interface{}) { b, _ := json.Marshal(v); fmt.Println(string(b)) }
func load(rate, seconds int) {
	if rate <= 0 || seconds <= 0 {
		panic("positive rate and duration required")
	}
	start := time.Now()
	emit(map[string]interface{}{"event": "start", "rate": rate, "seconds": seconds, "stats": snapshot()})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, bad, transport int
	lat := []float64{}
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()
	stop := time.NewTimer(time.Duration(seconds) * time.Second)
	sample := time.NewTicker(5 * time.Second)
	defer sample.Stop()
	issued := 0
loop:
	for {
		select {
		case <-stop.C:
			break loop
		case <-sample.C:
			emit(map[string]interface{}{"event": "sample", "elapsed": time.Since(start).Seconds(), "stats": snapshot()})
		case <-ticker.C:
			issued++
			wg.Add(1)
			go func() {
				defer wg.Done()
				t := time.Now()
				req, _ := http.NewRequest("POST", "http://runtime:18081/msg/send_msg", bytes.NewBufferString(`{"to":"sink@benchmark.invalid","templateID":"perf-template","priority":1,"templateData":{"name":"benchmark"}}`))
				req.Header.Set("Source-Id", "perf-send")
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				mu.Lock()
				defer mu.Unlock()
				lat = append(lat, time.Since(t).Seconds()*1000)
				if err != nil {
					transport++
					return
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				var result struct {
					Code int `json:"code"`
				}
				result.Code = -1
				if err == nil && json.Unmarshal(body, &result) == nil && resp.StatusCode == 200 && result.Code == 0 {
					ok++
				} else {
					bad++
				}
			}()
		}
	}
	wg.Wait()
	loadEnd := time.Since(start).Seconds()
	s := snapshot()
	emit(map[string]interface{}{"event": "load_end", "elapsed": loadEnd, "stats": s})
	drainStart := time.Now()
	for s["pending"] > 0 && time.Since(drainStart) < 180*time.Second {
		time.Sleep(5 * time.Second)
		s = snapshot()
		emit(map[string]interface{}{"event": "drain", "elapsed": time.Since(start).Seconds(), "stats": s})
	}
	sort.Float64s(lat)
	p99 := 0.0
	if len(lat) > 0 {
		p99 = lat[(len(lat)-1)*99/100]
	}
	emit(map[string]interface{}{"event": "final", "issued": issued, "accepted": ok, "business_errors": bad, "transport_errors": transport, "response_p99_ms": p99, "load_elapsed": loadEnd, "drain_seconds": time.Since(drainStart).Seconds(), "total_elapsed": time.Since(start).Seconds(), "stats": s})
}
