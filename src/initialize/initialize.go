package initialize

import (
	"github.com/BitofferHub/msgcenter/src/ctrl/msg"
	"github.com/gin-gonic/gin"
)

// RegisterRouter 注册路由
func RegisterRouter(router *gin.Engine) {
	{
		// 创建任务接口，前面是路径，后面是执行的函数，跳进去
		router.POST("/msg/send_msg", msg.SendMsg)
		router.GET("/msg/get_msg_record", msg.GetMsgRecord)
		router.POST("/msg/create_template", msg.CreateTemplate)
		router.GET("/msg/get_template", msg.GetTemplate)
		router.POST("/msg/update_template", msg.UpdateTemplate)
		router.POST("/msg/del_template", msg.DelTemplate)
	}
}
