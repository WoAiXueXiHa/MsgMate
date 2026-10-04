package msgpush

import (
	"crypto/tls"
	"sync"

	"github.com/BitofferHub/msgcenter/src/config"
	"github.com/BitofferHub/pkg/middlewares/log"
	"gopkg.in/gomail.v2"
)

const (
	// 端口
	port = 465
	// 邮箱服务器
	emailHost = "smtp.qq.com"
)

var (
	once = sync.Once{}
	d    *gomail.Dialer
)

// 发送给谁
func SendEmail(to string, subject string, text string) error {
	once.Do(func() {
		d = gomail.NewDialer(emailHost, port, config.Conf.Common.EmailAccount, config.Conf.Common.EmailAuthCode)
		d.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         emailHost,
		}
	})

	m := gomail.NewMessage()
	// 设置发送者
	m.SetHeader("From", config.Conf.Common.EmailAccount)
	// 设置接收者
	m.SetHeader("To", to)
	// 设置主题
	m.SetHeader("Subject", subject)
	// 设置邮件内容
	m.SetBody("text/plain", text)

	// Send the email to Bob, Cora and Dan.
	if err := d.DialAndSend(m); err != nil {
		log.Errorf("发送邮件失败: %s", err.Error())
		return err
	}
	log.Infof("发送邮件成功: %s", to)
	return nil
}
