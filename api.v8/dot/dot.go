package dot

import "time"

type (
	DotType string
	APIName string
	IDotter interface {
		Dot(dotType DotType, apiName APIName, success bool, elapsedDuration time.Duration) error
	}
)

const (
	SDKDotType  DotType = "sdk"
	HTTPDotType DotType = "http"
)
