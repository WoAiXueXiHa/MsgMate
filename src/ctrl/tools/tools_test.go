package tools

import "testing"

func TestTemplateReplace(t *testing.T) {
	got, err := TemplateReplace("亲爱的 {{.user_name}}，订单 {{.order_id}} 已发货", map[string]string{"user_name": "张三", "order_id": "123"})
	if err != nil || got != "亲爱的 张三，订单 123 已发货" {
		t.Fatalf("got %q, err %v", got, err)
	}
	if _, err := TemplateReplace("{{", nil); err == nil {
		t.Fatal("invalid template accepted")
	}
}
func TestInvalidQuotaDoesNotCallRedis(t *testing.T) {
	for _, c := range [][2]int{{0, 1}, {1000, 0}, {-1, 1}} {
		allowed, err := NewRateLimiter(nil, c[0], c[1]).IsRequestAllowed("test")
		if err == nil || allowed {
			t.Fatal("invalid quota accepted")
		}
	}
}
