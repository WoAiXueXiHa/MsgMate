local threads = {}
ok_count, bad_count = 0, 0
function setup(thread) table.insert(threads, thread) end
function init(args)
 wrk.method = "POST"
 wrk.headers["Content-Type"] = "application/json"
 wrk.headers["Source-Id"] = "perf-send"
 if args[1] then wrk.method="GET"; wrk.path="/msg/get_msg_record?msgID=" .. args[1]
 else wrk.body = '{"to":"sink@benchmark.invalid","templateID":"perf-template","priority":1,"templateData":{"name":"benchmark"}}' end
end
function response(status, headers, body)
 if status == 200 and body:match('"code"%s*:%s*0%s*[,}]') then ok_count=ok_count+1 else bad_count=bad_count+1 end
end
function done(summary, latency, requests)
 local good, bad = 0, 0
 for _,t in ipairs(threads) do good=good+t:get("ok_count"); bad=bad+t:get("bad_count") end
 print(string.format('Business success: %d; business errors: %d; success QPS: %.2f; duration: %.6fs',good,bad,good/(summary.duration/1000000),summary.duration/1000000))
end
