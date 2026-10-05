package msgpush

import (
	"crypto/tls"
	"fmt"
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"gopkg.in/gomail.v2"
	"net"
	"net/smtp"
	"time"
)

const emailHost = "smtp.qq.com"

// SendEmail 分别限制连接与 SMTP 交互时间，并验证服务端 TLS 证书。
func SendEmail(to, subject, text string) error {
	account, code := config.Conf.Common.EmailAccount, config.Conf.Common.EmailAuthCode
	if account == "" || code == "" {
		return fmt.Errorf("email account and authorization code required")
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", net.JoinHostPort(emailHost, "465"), &tls.Config{ServerName: emailHost, MinVersion: tls.VersionTLS12})
	if err != nil {
		return fmt.Errorf("SMTP TLS: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	client, err := smtp.NewClient(conn, emailHost)
	if err != nil {
		return fmt.Errorf("SMTP greeting: %w", err)
	}
	defer client.Close()
	if err := client.Auth(smtp.PlainAuth("", account, code, emailHost)); err != nil {
		return fmt.Errorf("SMTP authentication: %w", err)
	}
	return sendEmail(client, account, to, subject, text)
}

func sendEmail(client *smtp.Client, account, to, subject, text string) error {
	if err := client.Mail(account); err != nil {
		return fmt.Errorf("SMTP MAIL: %w", err)
	}
	if err := client.Rcpt(to); err != nil {
		return fmt.Errorf("SMTP RCPT: %w", err)
	}
	writer, err := client.Data()
	if err != nil {
		return fmt.Errorf("SMTP DATA: %w", err)
	}
	message := gomail.NewMessage()
	message.SetHeader("From", account)
	message.SetHeader("To", to)
	message.SetHeader("Subject", subject)
	message.SetBody("text/plain", text)
	if _, err := message.WriteTo(writer); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("SMTP DATA acceptance: %w", err)
	}
	// DATA 获得肯定应答表示 SMTP 已接受邮件；之后 QUIT 失败不能触发重复发送。
	_ = client.Quit()
	return nil
}
