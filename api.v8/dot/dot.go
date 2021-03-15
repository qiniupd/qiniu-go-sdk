package dot

type (
	DotType string
	APIName string
	IDotter interface {
		Dot(dotType DotType, apiName APIName, success bool) error
	}
)

const (
	SDKDotType  DotType = "sdk"
	HTTPDotType DotType = "http"
)
