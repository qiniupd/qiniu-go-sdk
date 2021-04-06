package client

import "fmt"

// CodeSuccess 是请求正确响应时给出的 code 值
const CodeSuccess = 0

// APIError 是接口本身报错时给出的 error 类型
type APIError struct {
	ReqID   string
	Code    int
	Message string
}

const (
	// CodeNoPredictedData 说明目标时间点没有找到对应的短期预测数据
	CodeNoPredictedData = 20111
)

func (e *APIError) Error() string {
	return fmt.Sprintf("[%s] [%d] %s", e.ReqID, e.Code, e.Message)
}

// Ensure 检查 code & message 并在响应不正确时构造错误实例
func Ensure(reqID string, code int, message string) error {
	if code != CodeSuccess {
		return &APIError{reqID, code, message}
	}
	return nil
}
