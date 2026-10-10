// Command perf runs the real API/consumers with a local, zero-latency channel stub.
// It refuses normal databases and never registers SMTP/SMS/Feishu adapters.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BurntSushi/toml"
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/consumer"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"github.com/WoAiXueXiHa/MsgMate/src/initialize"
	"github.com/gin-gonic/gin"
)

var sinkCalls uint64
var sinkDelay time.Duration

type localSink struct{ consumer.MsgBase }

func (p *localSink) SendMsg() error {
	if !strings.HasSuffix(p.To, "@benchmark.invalid") {
		return fmt.Errorf("benchmark recipient required")
	}
	if sinkDelay > 0 {
		time.Sleep(sinkDelay)
	}
	atomic.AddUint64(&sinkCalls, 1)
	return nil
}

func main() {
	action := flag.String("action", "serve", "serve or load")
	file := flag.String("config-file", "", "benchmark-only config")
	consume := flag.Bool("consume", false, "start the real consumers")
	delay := flag.Duration("sink-delay", 0, "local fake channel latency")
	rate := flag.Int("rps", 10, "open-loop offered requests/sec")
	duration := flag.Duration("duration", 20*time.Second, "load scheduling duration")
	base := flag.String("base", "http://runtime:18081", "benchmark API URL")
	workers := flag.Int("workers", 64, "bounded HTTP workers")
	flag.Parse()
	var err error
	if *action == "load" {
		err = load(*base, *rate, *duration, *workers)
	} else if *action == "stats" {
		resp, e := http.Get(*base + "/bench/stats")
		if e != nil {
			err = e
		} else {
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				err = fmt.Errorf("stats HTTP %d", resp.StatusCode)
			} else {
				_, err = io.Copy(os.Stdout, resp.Body)
			}
		}
	} else if *action == "serve" {
		err = serve(*file, *consume, *delay)
	} else {
		err = fmt.Errorf("unknown action")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(file string, consume bool, delay time.Duration) error {
	var cf config.TomlConfig
	if _, err := toml.DecodeFile(file, &cf); err != nil {
		return err
	}
	if !strings.HasSuffix(cf.MySQL.Dbname, "_perf_test") || cf.Redis.Url != "redis:6379" {
		return fmt.Errorf("isolated _perf_test database and redis:6379 required")
	}
	for _, t := range cf.Kafka.Topics {
		if !strings.HasPrefix(t.Name, "perf_") || !strings.HasPrefix(t.GroupID, "perf_") {
			return fmt.Errorf("benchmark topic/group required")
		}
	}
	if cf.Common.EmailAccount != "" || cf.Common.EmailAuthCode != "" || cf.Common.AliAppSecret != "" {
		return fmt.Errorf("external channel credentials forbidden")
	}
	if err := os.MkdirAll("/tmp/perf-log", 0755); err != nil {
		return err
	}
	log.Init(log.WithLogPath("/tmp/perf-log"), log.WithConsole(false), log.WithLogLevel("info"), log.WithMaxSize(10), log.WithMaxBackups(2))
	config.Conf = &cf
	dt, err := data.NewData(&cf)
	if err != nil {
		return err
	}
	defer dt.Close()
	db := dt.GetDB()
	tp := data.MsgTemplate{TemplateID: "perf-template", SourceID: "perf-send", Name: "perf", Subject: "local benchmark", Content: "Hello {{.name}}", Channel: 1, Status: 2}
	if err = db.Where("template_id = ?", tp.TemplateID).FirstOrCreate(&tp).Error; err != nil {
		return err
	}
	quota := data.SourceQuota{SourceID: "perf-send", Channel: 1, Num: 10000000, Unit: 1000}
	if err = db.Where("source_id = ? AND channel = ?", quota.SourceID, 1).FirstOrCreate(&quota).Error; err != nil {
		return err
	}
	var existing int64
	if err = db.Model(&data.MsgRecord{}).Where("source_id = ?", "perf-query").Count(&existing).Error; err != nil {
		return err
	}
	if existing == 0 {
		for start := 0; start < 10000; start += 500 {
			rows := make([]data.MsgRecord, 0, 500)
			for i := start; i < start+500; i++ {
				rows = append(rows, data.MsgRecord{MsgId: fmt.Sprintf("perf-record-%05d", i), SourceID: "perf-query", Channel: 1, Subject: "local benchmark", To: "sink@benchmark.invalid", TemplateID: tp.TemplateID, TemplateData: `{"name":"benchmark"}`, Status: 2})
			}
			if err = db.Create(&rows).Error; err != nil {
				return err
			}
		}
	}
	// Register ONLY the safe local adapter; production channel adapters are unreachable.
	sinkDelay = delay
	consumer.RegisterHandler(&consumer.MsgHandler{Channel: 1, NewProc: func() consumer.MsgIntf { return &localSink{} }})
	cs := consumer.NewMsgConsume()
	if consume {
		cs.Consume()
		defer cs.UnlockAll()
	}
	gin.SetMode(gin.ReleaseMode)
	access, err := os.Create("/tmp/perf-log/access.log")
	if err != nil {
		return err
	}
	defer access.Close()
	gin.DefaultWriter = access
	router := gin.Default()
	_ = router.SetTrustedProxies(nil)
	initialize.RegisterRouter(router)
	router.GET("/bench/stats", func(c *gin.Context) {
		type counts struct {
			Total     int64
			Pending   int64
			Completed int64
			Failed    int64
			Retries   int64
		}
		var n counts
		q := db.Raw("SELECT COUNT(*) total, COALESCE(SUM(status=1),0) pending, COALESCE(SUM(status=2),0) completed, COALESCE(SUM(status=3),0) failed, COALESCE(SUM(retry_count),0) retries FROM t_msg_record WHERE source_id='perf-send'").Scan(&n)
		statsError := ""
		if q.Error != nil {
			statsError = q.Error.Error()
		}
		mysqlTimeWait := 0
		if raw, e := os.ReadFile("/proc/net/tcp"); e == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				fields := strings.Fields(line)
				if len(fields) > 3 && fields[3] == "06" && strings.HasSuffix(fields[2], ":0CEA") {
					mysqlTimeWait++
				}
			}
		}
		sqlDB, _ := db.DB()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		c.JSON(200, gin.H{"stats_error": statsError, "mysql_time_wait": mysqlTimeWait, "counts": n, "sink_calls": atomic.LoadUint64(&sinkCalls), "db_pool": sqlDB.Stats(), "goroutines": runtime.NumGoroutine(), "heap_bytes": mem.HeapAlloc, "mode_mysql": cf.Common.MySQLAsMq, "consume": consume, "sink_delay_ms": delay.Milliseconds(), "time": time.Now().UTC()})
	})
	server := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", cf.Common.Port), Handler: router}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	fmt.Printf("BENCHMARK ONLY mysql=%v consume=%v fake_channel_delay=%s\n", cf.Common.MySQLAsMq, consume, delay)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// load schedules arrivals independently of HTTP completion. A full worker queue is
// counted as a missed request, rather than silently reducing offered traffic.
func load(base string, rps int, duration time.Duration, workers int) error {
	if rps < 1 || workers < 1 || duration <= 0 || !strings.HasPrefix(base, "http://runtime:") {
		return fmt.Errorf("positive load settings and isolated runtime URL required")
	}
	tr := &http.Transport{MaxIdleConns: workers, MaxIdleConnsPerHost: workers, MaxConnsPerHost: workers}
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	defer tr.CloseIdleConnections()
	type result struct {
		Scheduled       int         `json:"scheduled"`
		Sent            int         `json:"sent"`
		Accepted        int         `json:"accepted"`
		BusinessErrors  int         `json:"business_errors"`
		TransportErrors int         `json:"transport_errors"`
		Missed          int         `json:"missed"`
		OfferedRPS      int         `json:"offered_rps"`
		DurationSeconds float64     `json:"duration_seconds"`
		ElapsedSeconds  float64     `json:"elapsed_seconds"`
		P50MS           float64     `json:"p50_ms"`
		P99MS           float64     `json:"p99_ms"`
		Codes           map[int]int `json:"codes"`
		SampleIDs       []string    `json:"sample_ids"`
	}
	res := result{OfferedRPS: rps, DurationSeconds: duration.Seconds(), Codes: map[int]int{}}
	jobs := make(chan int, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := []float64{}
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				// Balanced priorities test the existing independent priority loops.
				payload := fmt.Sprintf(`{"to":"sink@benchmark.invalid","templateID":"perf-template","priority":%d,"templateData":{"name":"benchmark"}}`, n%3+1)
				req, _ := http.NewRequest("POST", base+"/msg/send_msg", bytes.NewBufferString(payload))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Source-Id", "perf-send")
				t := time.Now()
				resp, err := client.Do(req)

				var body struct {
					Code  int    `json:"code"`
					MsgID string `json:"msgID"`
				}
				valid := false
				if err == nil {
					raw, e := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
					resp.Body.Close()
					if e == nil && resp.StatusCode == 200 && json.Unmarshal(raw, &body) == nil {
						valid = true
					}
				}
				lat := float64(time.Since(t).Microseconds()) / 1000
				mu.Lock()
				res.Sent++
				latencies = append(latencies, lat)
				if !valid {
					res.TransportErrors++
				} else {
					res.Codes[body.Code]++
					if body.Code == 0 && body.MsgID != "" {
						res.Accepted++
						if len(res.SampleIDs) < 10 {
							res.SampleIDs = append(res.SampleIDs, body.MsgID)
						}
					} else {
						res.BusinessErrors++
					}
				}
				mu.Unlock()
			}
		}()
	}
	total := int(float64(rps) * duration.Seconds())
	for n := 0; n < total; n++ {
		target := start.Add(time.Duration(float64(n) * float64(time.Second) / float64(rps)))
		if d := time.Until(target); d > 0 {
			time.Sleep(d)
		}
		res.Scheduled++
		select {
		case jobs <- n:
		default:
			res.Missed++
		}
	}
	close(jobs)
	wg.Wait()
	res.ElapsedSeconds = time.Since(start).Seconds()
	sort.Float64s(latencies)
	if len(latencies) > 0 {
		res.P50MS = latencies[(len(latencies)-1)/2]
		res.P99MS = latencies[int(float64(len(latencies)-1)*.99)]
	}
	return json.NewEncoder(os.Stdout).Encode(res)
}
