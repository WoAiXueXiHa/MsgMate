//go:build integration
// +build integration

package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/ctrlmodel"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/tools"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"github.com/WoAiXueXiHa/MsgMate/src/initialize"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var attempts sync.Map

// No external channel calls: failure/success is controlled by the test receiver.
type testProc struct{ MsgBase }

func (p *testProc) SendMsg() error {
	v, _ := attempts.LoadOrStore(p.To, 0)
	attempts.Store(p.To, v.(int)+1)
	if strings.HasPrefix(p.To, "fail") {
		return errors.New("controlled channel failure")
	}
	return nil
}
func TestMain(m *testing.M) {
	file := os.Getenv("MSGMATE_TEST_CONFIG")
	if file == "" {
		fmt.Fprintln(os.Stderr, "MSGMATE_TEST_CONFIG required for integration tests")
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "msgmate-test-log-")
	if err != nil {
		panic(err)
	}
	log.Init(log.WithLogPath(dir), log.WithConsole(false))
	config.TestFilePath = file
	config.InitConf("test")
	if !strings.HasSuffix(config.Conf.MySQL.Dbname, "_test") {
		fmt.Fprintln(os.Stderr, "integration database must end with _test")
		os.Exit(1)
	}
	if _, err := data.NewData(config.Conf); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	InitMsgProc()
	msgProcMap[int(data.Channel_EMAIL)] = &MsgHandler{Channel: int(data.Channel_EMAIL), NewProc: func() MsgIntf { return &testProc{} }}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
func seed(t *testing.T, to string) (*ctrlmodel.SendMsgReq, *data.MsgTemplate) {
	t.Helper()
	db := data.GetData().GetDB()
	id := data.NewMsgID()
	tp := &data.MsgTemplate{TemplateID: id, SourceID: "test-" + id, Name: "test", Subject: "test", Content: "{{.name}}", Channel: 1, Status: 2}
	if err := data.MsgTemplateNsp.Create(db, tp); err != nil {
		t.Fatal(err)
	}
	req := &ctrlmodel.SendMsgReq{MsgID: data.NewMsgID(), TemplateID: id, Priority: 1, To: to + id, Subject: "test", TemplateData: map[string]string{"name": "test"}}
	if err := tools.CreateMsgRecord(db, req.MsgID, req, tp, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, p := range []string{"high", "middle", "low", "retry"} {
			db.Table("t_msg_queue_"+p).Where("msg_id = ?", req.MsgID).Delete(&data.MsgQueue{})
		}
		db.Where("msg_id = ?", req.MsgID).Delete(&data.MsgTmpQueueTimer{})
		db.Where("msg_id = ?", req.MsgID).Delete(&data.MsgRecord{})
		db.Where("template_id = ?", id).Delete(&data.MsgTemplate{})
		data.GetData().InvalidateTemplate(id)
		data.InvalidateRecord(req.MsgID)
	})
	return req, tp
}
func record(t *testing.T, id string) *data.MsgRecord {
	t.Helper()
	r, err := data.MsgRecordNsp.Find(data.GetData().GetDB(), id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestMySQLRetryCapAndSuccess(t *testing.T) {
	db := data.GetData().GetDB()
	config.Conf.Common.MaxRetryCount = 3
	s := NewMsgConsume()
	defer s.cancel()
	req, _ := seed(t, "fail-")
	if err := data.Enqueue(db, req); err != nil {
		t.Fatal(err)
	}
	s.consumeMySQLMsg(data.PRIORITY_LOW)
	s.consumeMySQLMsg(data.PRIORITY_RETRY)
	s.consumeMySQLMsg(data.PRIORITY_RETRY)
	r := record(t, req.MsgID)
	if r.Status != 3 || r.RetryCount != 3 {
		t.Fatalf("failed record %+v", r)
	}
	q, _ := data.MsgQueueNsp.Find(db, "low", req.MsgID)
	if q.Status != 4 {
		t.Fatal("original queue not closed")
	}
	q, _ = data.MsgQueueNsp.Find(db, "retry", req.MsgID)
	if q.Status != 4 {
		t.Fatal("retry queue not closed")
	}
	req2, _ := seed(t, "ok-")
	if err := data.Enqueue(db, req2); err != nil {
		t.Fatal(err)
	}
	s.consumeMySQLMsg(data.PRIORITY_LOW)
	if r := record(t, req2.MsgID); r.Status != 2 {
		t.Fatalf("success recorded as %d", r.Status)
	}
}
func TestTimerWithoutRedisIndexAndStartupRecovery(t *testing.T) {
	db := data.GetData().GetDB()
	req, _ := seed(t, "timer-")
	// Legacy timer submissions did not create an ordinary record.
	if err := db.Where("msg_id = ?", req.MsgID).Delete(&data.MsgRecord{}).Error; err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(req)
	row := &data.MsgTmpQueueTimer{MsgId: req.MsgID, Req: string(body), SendTimestamp: time.Now().Unix() - 1, Status: 2}
	if err := data.MsgTmpQueueTimerNsp.Create(db, row); err != nil {
		t.Fatal(err)
	}
	if err := data.RecoverPending(db, true); err != nil {
		t.Fatal(err)
	}
	timer := TimerMsgConsume{ctx: context.Background()}
	timer.consumeTimerMsg()
	got, err := data.MsgTmpQueueTimerNsp.Find(db, req.MsgID)
	if err != nil || got.Status != 3 {
		t.Fatalf("timer forwarding failed: %v %+v", err, got)
	}
	s := NewMsgConsume()
	defer s.cancel()
	s.consumeMySQLMsg(data.PRIORITY_LOW)
	if record(t, req.MsgID).Status != 2 {
		t.Fatal("timer message not sent")
	}
}
func TestBadPayloadDoesNotBlockNextAndRecovery(t *testing.T) {
	db := data.GetData().GetDB()
	bad, _ := seed(t, "bad-")
	good, _ := seed(t, "good-")
	for _, req := range []*ctrlmodel.SendMsgReq{bad, good} {
		if err := data.Enqueue(db, req); err != nil {
			t.Fatal(err)
		}
	}
	db.Table("t_msg_queue_low").Where("msg_id = ?", bad.MsgID).Update("template_data", "invalid JSON")
	db.Table("t_msg_queue_low").Where("msg_id = ?", good.MsgID).Update("status", 2)
	if err := data.RecoverPending(db, true); err != nil {
		t.Fatal(err)
	}
	s := NewMsgConsume()
	defer s.cancel()
	s.consumeMySQLMsg(data.PRIORITY_LOW)
	if record(t, bad.MsgID).Status != 3 || record(t, good.MsgID).Status != 2 {
		t.Fatal("batch blocked or recovery failed")
	}
}
func TestAtomicRecordQueueRollback(t *testing.T) {
	db := data.GetData().GetDB()
	req, tp := seed(t, "rollback-")
	newID := data.NewMsgID()
	err := db.Transaction(func(tx *gorm.DB) error {
		copyReq := *req
		copyReq.MsgID = newID
		copyReq.Priority = 99
		if err := tools.CreateMsgRecord(tx, newID, &copyReq, tp, 1); err != nil {
			return err
		}
		return data.Enqueue(tx, &copyReq)
	})
	if err == nil {
		t.Fatal("invalid queue accepted")
	}
	if _, err := data.MsgRecordNsp.Find(db, newID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("record was not rolled back")
	}
}
func TestAPIValidationUpdateDeleteAndStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	initialize.RegisterRouter(router)
	call := func(method, path, body, source string) map[string]interface{} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Source-Id", source)
		router.ServeHTTP(w, r)
		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := call("POST", "/msg/send_msg", "{}", "test"); resp["code"].(float64) == 0 {
		t.Fatal("invalid request succeeded")
	}
	created := call("POST", "/msg/create_template", `{"sourceID":"api-test","name":"test","subject":"test","channel":1,"content":"{{.name}}"}`, "api-test")
	if created["code"] != float64(0) || created["templateID"] == "" {
		t.Fatalf("create template: %+v", created)
	}
	templateID := created["templateID"].(string)
	t.Cleanup(func() {
		data.GetData().GetDB().Where("template_id = ?", templateID).Delete(&data.MsgTemplate{})
		data.GetData().InvalidateTemplate(templateID)
	})
	queried := call("GET", "/msg/get_template?templateID="+templateID, "", "api-test")
	if queried["code"] != float64(0) || queried["content"] != "{{.name}}" {
		t.Fatalf("get template: %+v", queried)
	}
	pending, err := data.MsgTemplateNsp.Find(data.GetData().GetDB(), templateID)
	if err != nil || pending.Status != int(data.TEMPLATE_STATUS_PENDING) {
		t.Fatalf("new template must await approval: %+v, %v", pending, err)
	}
	req, tp := seed(t, "query-")
	db := data.GetData().GetDB()
	if err := data.MsgRecordNsp.UpdateStatus(db, req.MsgID, 2); err != nil {
		t.Fatal(err)
	}
	if denied := call("GET", "/msg/get_msg_record?msgID="+req.MsgID, "", "other-source"); denied["code"] != float64(8020) {
		t.Fatalf("record source mismatch: %+v", denied)
	}
	payload := fmt.Sprintf(`{"to":"controlled-api","templateID":%q,"priority":4,"templateData":{}}`, tp.TemplateID)
	if denied := call("POST", "/msg/send_msg", payload, tp.SourceID); denied["code"] != float64(8020) {
		t.Fatalf("invalid priority: %+v", denied)
	}
	payload = fmt.Sprintf(`{"to":"controlled-api","templateID":%q,"priority":1,"templateData":{}}`, tp.TemplateID)
	if denied := call("POST", "/msg/send_msg", payload, "other-source"); denied["code"] != float64(8035) {
		t.Fatalf("send source mismatch: %+v", denied)
	}
	mysqlMode := config.Conf.Common.MySQLAsMq
	config.Conf.Common.MySQLAsMq = true
	submitted := call("POST", "/msg/send_msg", payload, tp.SourceID)
	config.Conf.Common.MySQLAsMq = mysqlMode
	if submitted["code"] != float64(0) || submitted["msgID"] == "" {
		t.Fatalf("send API: %+v", submitted)
	}
	submittedID := submitted["msgID"].(string)
	t.Cleanup(func() {
		db.Table("t_msg_queue_low").Where("msg_id = ?", submittedID).Delete(&data.MsgQueue{})
		db.Where("msg_id = ?", submittedID).Delete(&data.MsgRecord{})
	})
	if got := record(t, submittedID); got.Status != int(data.MSG_STATUS_PENDING) || got.SourceID != tp.SourceID {
		t.Fatalf("submitted record: %+v", got)
	}
	resp := call("GET", "/msg/get_msg_record?msgID="+req.MsgID, "", tp.SourceID)
	if resp["status"] != float64(2) || resp["to"] != req.To {
		t.Fatalf("record response %+v", resp)
	}
	resp = call("POST", "/msg/update_template", fmt.Sprintf(`{"templateID":%q,"name":"updated"}`, tp.TemplateID), tp.SourceID)
	if resp["code"] != float64(0) {
		t.Fatal(resp)
	}
	got, _ := data.MsgTemplateNsp.Find(db, tp.TemplateID)
	if got.Content != tp.Content || got.Subject != tp.Subject || got.Channel != 1 {
		t.Fatal("partial update erased fields")
	}
	resp = call("POST", "/msg/del_template", fmt.Sprintf(`{"templateID":%q}`, tp.TemplateID), tp.SourceID)
	if resp["code"] != float64(0) {
		t.Fatal(resp)
	}
	if _, err := data.MsgTemplateNsp.Find(db, tp.TemplateID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("delete failed")
	}
}
func TestRedisQuotaAtomicTTL(t *testing.T) {
	client := data.GetData().GetCache().GetRedisBaseConn()
	prefix := "msgmate-test-quota-" + data.NewMsgID()
	ctx := context.Background()
	lm := tools.NewRateLimiter(client, 1000, 1)
	// A long fixed window avoids boundary flakes and also exercises millisecond TTL below one second separately.
	lm = tools.NewRateLimiter(client, 60000, 1)
	ok, err := lm.IsRequestAllowed(prefix)
	if err != nil || !ok {
		t.Fatalf("first request: %v", err)
	}
	ok, err = lm.IsRequestAllowed(prefix)
	if err != nil || ok {
		t.Fatalf("second request: %v", err)
	}
	keys, err := client.Keys(ctx, prefix+":*").Result()
	if err != nil || len(keys) != 1 {
		t.Fatal("counter missing")
	}
	defer client.Del(ctx, keys...)
	ttl, err := client.PTTL(ctx, keys[0]).Result()
	if err != nil || ttl <= 0 {
		t.Fatal("TTL missing")
	}
}

func TestKafkaBrokerSuccessAndRetry(t *testing.T) {
	original := config.Conf
	cf := *original
	cf.Common.MySQLAsMq = false
	cf.Common.MaxRetryCount = 3
	cf.Kafka.Topics = map[string]config.TopicConfig{}
	suffix := data.NewMsgID()
	for _, p := range consumePriority {
		name := "msgmate-test-" + suffix + "-" + data.GetPriorityStr(p)
		cf.Kafka.Topics[name] = config.TopicConfig{Name: name, Priority: int(p), GroupID: name}
	}
	config.Conf = &cf
	dt, err := data.NewData(&cf)
	if err != nil {
		t.Fatal(err)
	}
	s := NewMsgConsume()
	s.Consume()
	defer func() {
		s.UnlockAll()
		config.Conf = original
		if _, err := data.NewData(original); err != nil {
			t.Error(err)
		}
	}()
	good, _ := seed(t, "kafka-good-")
	bad, _ := seed(t, "fail-kafka-")
	for _, req := range []*ctrlmodel.SendMsgReq{good, bad} {
		if err := dt.Publish(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(45 * time.Second)
	for {
		success, failure := record(t, good.MsgID), record(t, bad.MsgID)
		if success.Status == 2 && failure.Status == 3 {
			if failure.RetryCount != 3 {
				t.Fatalf("failure count %d", failure.RetryCount)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Kafka did not finish: success=%d failure=%d retries=%d", success.Status, failure.Status, failure.RetryCount)
		}
		time.Sleep(100 * time.Millisecond)
	}
	timed, _ := seed(t, "kafka-timer-")
	body, _ := json.Marshal(timed)
	if err := data.MsgTmpQueueTimerNsp.Create(dt.GetDB(), &data.MsgTmpQueueTimer{MsgId: timed.MsgID, Req: string(body), SendTimestamp: time.Now().Unix() - 1, Status: 1}); err != nil {
		t.Fatal(err)
	}
	timer := TimerMsgConsume{ctx: context.Background()}
	timer.consumeTimerMsg()
	deadline = time.Now().Add(15 * time.Second)
	for record(t, timed.MsgID).Status != 2 {
		if time.Now().After(deadline) {
			t.Fatal("Kafka timer forwarding did not deliver")
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func TestConcurrentFailureCount(t *testing.T) {
	req, _ := seed(t, "count-")
	db := data.GetData().GetDB()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := data.MsgRecordNsp.IncrementRetryCount(db, req.MsgID); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if r := record(t, req.MsgID); r.RetryCount != 8 {
		t.Fatalf("lost increments: %d", r.RetryCount)
	}
}
