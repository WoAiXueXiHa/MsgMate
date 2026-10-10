package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/consumer"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"github.com/WoAiXueXiHa/MsgMate/src/initialize"
	"github.com/gin-gonic/gin"
)

func main() {
	// 初始化配置
	config.Init()
	_, err := data.NewData(config.Conf)
	if err != nil {
		log.Errorf("initialize NewData err %s", err.Error())
		os.Exit(1)
	}
	// 先注册渠道工厂，再恢复遗留任务、启动消费者，避免消费时找不到处理器。
	consumer.InitMsgProc()
	if err := data.RecoverPending(data.GetData().GetDB(), config.Conf.Common.MySQLAsMq); err != nil {
		log.Errorf("recover pending: %v", err)
		os.Exit(1)
	}
	cs := consumer.NewMsgConsume()
	cs.Consume()
	var tmc consumer.TimerMsgConsume
	tmc.Consume()

	// 设置信号处理，确保在程序退出前释放分布式锁
	setupSignalHandler(cs, &tmc)

	// 创建一个web服务
	router := gin.Default()
	_ = router.SetTrustedProxies(nil)
	// 路由层只接收请求；实际发送由后台消费者完成。
	initialize.RegisterRouter(router)
	// HTTP 服务阻塞主协程，后台队列和定时任务在独立协程中推进。
	// 明确使用 IPv4，避免通配地址的双栈行为受操作系统影响。
	addr := fmt.Sprintf("0.0.0.0:%d", config.Conf.Common.Port)
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Errorf("HTTP listen %s: %v", addr, err)
		tmc.Unlock()
		cs.UnlockAll()
		data.GetData().Close()
		os.Exit(1)
	}
	log.Infof("HTTP listening on %s (IPv4)", addr)
	if err := http.Serve(listener, router); err != nil {
		log.Errorf("HTTP serve: %v", err)
		tmc.Unlock()
		cs.UnlockAll()
		data.GetData().Close()
		os.Exit(1)
	}
}

// setupSignalHandler 设置信号处理，确保在程序退出前释放锁
func setupSignalHandler(cs *consumer.MsgConsume, tmc *consumer.TimerMsgConsume) {
	c := make(chan os.Signal, 1)
	// 监听 SIGINT, SIGTERM, SIGQUIT 信号
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-c
		log.Infof("接收到系统信号: %v，准备优雅退出", sig)

		// 释放所有分布式锁
		log.Info("释放所有分布式锁...")
		tmc.Unlock()
		cs.UnlockAll()
		data.GetData().Close()

		log.Info("锁释放完成，程序退出")
		os.Exit(0)
	}()
}
