local threads = {}
ok_count = 0
bad_count = 0
code_counts = {}
function setup(thread) table.insert(threads, thread) end
function init(args)
 local kind = args[1] or "record"
 if kind == "send" then
  wrk.method = "POST"
  wrk.path = "/msg/send_msg"
  wrk.headers["Content-Type"] = "application/json"
  wrk.headers["Source-Id"] = "perf-send"
  wrk.body = '{"to":"sink@benchmark.invalid","templateID":"perf-template","priority":1,"templateData":{"name":"benchmark"}}'
 else
  wrk.headers["Source-Id"] = "perf-query"
 end
end
local counter = 0
function request()
 if wrk.method == "POST" then return wrk.format() end
 counter = (counter + 1) % 10000
 return wrk.format("GET", string.format("/msg/get_msg_record?msgID=perf-record-%05d",counter))
end
function response(status, headers, body)
 local code = body:match('"code"%s*:%s*(%-?%d+)')
 if status == 200 and code == "0" and (wrk.method ~= "POST" or body:match('"msgID"%s*:%s*"[^"]+"')) then ok_count = ok_count + 1 else bad_count = bad_count + 1 end
end
function done(summary, latency, requests)
 local good, bad = 0, 0
 for _, thread in ipairs(threads) do good = good + thread:get("ok_count");bad = bad + thread:get("bad_count") end
 print(string.format('BENCH_JSON {"ok":%d,"bad":%d,"requests":%d,"duration_us":%d,"p50_ms":%.3f,"p99_ms":%.3f,"connect":%d,"read":%d,"write":%d,"timeout":%d,"http_errors":%d}',good,bad,summary.requests,summary.duration,latency:percentile(50)/1000,latency:percentile(99)/1000,summary.errors.connect,summary.errors.read,summary.errors.write,summary.errors.timeout,summary.errors.status))
end
