# 宿主 loopback 初步诊断：不可用于简历容量结论

这些输出来自 2026-10-10 的 WSL 宿主 `127.0.0.1:18109` 模板查询初测。存在大量 socket 写入错误，诊断捕获 `errno=111 Connection refused`；8 并发复测还有读取错误。内核 ListenOverflows、ListenDrops、TCPBacklogDrop、TCPReqQFullDrop 在低并发复测期间均无增长，监听端口采样持续存在。

峰值约 1,339 req/s 未重复验证，不能称为服务性能极限。CPU 短时采样约为单核的 41.8%，MySQL 容器约单核的 21%–55%，没有证据表明整机 CPU 或内存耗尽；资源样本不足以确认瓶颈。尚未查明宿主连接异常的底层原因。

正式容量测试改用独立 Docker 私网，查看 `../performance/` 下执行状态为 complete 的日志；正式记录查询接口为 `/msg/get_msg_record`，与这里的模板查询不同，不能直接将两组吞吐当作网络优化收益。
