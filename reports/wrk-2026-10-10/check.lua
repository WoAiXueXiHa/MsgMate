local threads = {}
function setup(thread) table.insert(threads, thread) end
-- Store counters in globals so the controller can read each worker.
function response(status, headers, body)
 if status == 200 and body:match('"code"%s*:%s*0%s*[,}]') then ok_count = ok_count + 1 else bad_count = bad_count + 1 end
end
ok_count = 0
bad_count = 0

function done(summary, latency, requests)
 local good, bad = 0, 0
 for _, thread in ipairs(threads) do
  good = good + thread:get('ok_count')
  bad = bad + thread:get('bad_count')
 end
 print(string.format('BUSINESS ok=%d bad=%d', good, bad))
end
