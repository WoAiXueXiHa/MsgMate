package tools

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter 限制通知提交入口；不限制定时转交和发送重试的渠道调用。
type RateLimiter struct {
	redisClient *redis.Client
	limit, div  int
}

// NewRateLimiter 的 div 为窗口长度（毫秒），limit 为窗口内允许的提交次数。
func NewRateLimiter(client *redis.Client, div, limit int) *RateLimiter {
	return &RateLimiter{client, limit, div}
}

func (r *RateLimiter) IsRequestAllowed(keyID string) (bool, error) {
	if r.div <= 0 || r.limit <= 0 {
		return false, fmt.Errorf("quota unit and limit must be positive")
	}
	// 用时间窗口编号构造独立计数 key；窗口边界两侧可能连续放行两批请求。
	key := fmt.Sprintf("%s:%d", keyID, time.Now().UnixMilli()/int64(r.div))
	// Lua 将递增与首次设置 TTL 原子执行，避免计数成功但过期未设置的残留 key。
	// 超额请求也会计数，计数 key 在窗口结束后自然过期。
	count, err := r.redisClient.Eval(context.Background(), `local n = redis.call('INCR', KEYS[1]); if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]); end; return n`, []string{key}, r.div).Int64()
	return count <= int64(r.limit) && err == nil, err
}
