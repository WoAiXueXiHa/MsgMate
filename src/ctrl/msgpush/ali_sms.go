// This file is auto-generated, don't edit it. Thanks.
package msgpush

import (
	"fmt"
	conf "github.com/WoAiXueXiHa/MsgMate/src/config"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	dysmsapi20170525 "github.com/alibabacloud-go/dysmsapi-20170525/v4/client"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
)

// Description:
//
// 使用AK&SK初始化账号Client
//
// @return Client
//
// @throws Exception
func CreateClient() (_result *dysmsapi20170525.Client, _err error) {
	config := &openapi.Config{
		// 必填，请确保代码运行环境设置了环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID。
		AccessKeyId: tea.String(conf.Conf.Common.AliAppID),
		// 必填，请确保代码运行环境设置了环境变量 ALIBABA_CLOUD_ACCESS_KEY_SECRET。
		AccessKeySecret: tea.String(conf.Conf.Common.AliAppSecret),
	}
	// Endpoint 请参考 https://api.aliyun.com/product/Dysmsapi
	config.Endpoint = tea.String("dysmsapi.aliyuncs.com")
	_result = &dysmsapi20170525.Client{}
	_result, _err = dysmsapi20170525.NewClient(config)
	return _result, _err
}

func SendSMS(to string, signName string, templateCode string, templateParam string) error {
	client, err := CreateClient()
	if err != nil {
		return err
	}
	request := &dysmsapi20170525.SendSmsRequest{PhoneNumbers: tea.String(to), TemplateCode: tea.String(templateCode), TemplateParam: tea.String(templateParam), SignName: tea.String(signName)}
	response, err := client.SendSmsWithOptions(request, &util.RuntimeOptions{ConnectTimeout: tea.Int(10000), ReadTimeout: tea.Int(15000)})
	if err != nil {
		return err
	}
	if response == nil || response.Body == nil {
		return fmt.Errorf("SMS empty response")
	}
	if tea.StringValue(response.Body.Code) != "OK" {
		return fmt.Errorf("SMS rejected: %s", tea.StringValue(response.Body.Code))
	}
	return nil
}
