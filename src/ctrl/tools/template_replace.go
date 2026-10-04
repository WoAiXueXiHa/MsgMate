package tools

import (
	"bytes"
	"text/template"

	"github.com/BitofferHub/pkg/middlewares/log"
)

func TemplateReplace(templateContent string, data map[string]string) (string, error) {
	log.Infof("templateContent: %s, data: %v", templateContent, data)
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
