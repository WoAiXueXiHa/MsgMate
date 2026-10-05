package tools

import (
	"bytes"
	"text/template"
)

// TemplateReplace 先解析 Go 模板语法，再用字符串参数映射渲染正文。
// 使用 text/template，不做 HTML 转义；缺失变量沿用模板库默认行为。
// 每次创建独立模板和缓冲区，避免并发消息共享渲染状态。
func TemplateReplace(templateContent string, data map[string]string) (string, error) {
	tmpl, err := template.New("message").Parse(templateContent)
	if err != nil {
		return "", err
	}
	var result bytes.Buffer
	err = tmpl.Execute(&result, data)
	if err != nil {
		return "", err
	}
	return result.String(), nil
}
