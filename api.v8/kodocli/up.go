package kodocli

import (
	"encoding/json"
	digest "github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
)

type FileType uint32

const (
	TypeNormal = iota
	TypeLine
)

type AuthPolicy struct {
	Scope               string   `json:"scope"`
	IsPrefixalScope     uint16   `json:"isPrefixalScope,omitempty"` // 若为非0，则Scope的key部分表示为前缀限定
	CallbackUrl         string   `json:"callbackUrl,omitempty"`
	CallbackHost        string   `json:"callbackHost,omitempty"`
	CallbackBodyType    string   `json:"callbackBodyType,omitempty"`
	CallbackBody        string   `json:"callbackBody,omitempty"`
	CallbackFetchKey    uint16   `json:"callbackFetchKey,omitempty"` // 先回调取得key再改名 https://pm.qbox.me/issues/11851
	CallbackTimeout     uint16   `json:"callbackTimeout,omitempty"`  // 允许自定义超时需求 https://pm.qbox.me/issues/21576
	Customer            string   `json:"customer,omitempty"`
	EndUser             string   `json:"endUser,omitempty"`
	Transform           string   `json:"transform,omitempty"`
	FopTimeout          uint32   `json:"fopTimeout,omitempty"`
	Deadline            int64    `json:"deadline"`         // 截止时间（以秒为单位）原来是uint32 上限为到2106年 如果用户设置过期时间超过了这个上限就会鉴权失败 请各单位如果编译不过自行调整https://pm.qbox.me/issues/25718
	Escape              uint16   `json:"escape,omitempty"` // 是否允许存在转义符号
	DetectMime          uint16   `json:"detectMime,omitempty"`
	Exclusive           uint16   `json:"exclusive,omitempty"`       // 若为非0, 即使Scope为"Bucket:key"的形式也是insert only
	InsertOnly          uint16   `json:"insertOnly,omitempty"`      // Exclusive 的别名
	ForceInsertOnly     bool     `json:"forceInsertOnly,omitempty"` // 若为true,即使上传hash相同的文件也会报文件已存在,优先级高于InsertOnly
	ReturnBody          string   `json:"returnBody,omitempty"`
	SignReturnBody      uint16   `json:"signReturnBody,omitempty"` // 默认不开启签名，需要用户的 AK SK
	ReturnURL           string   `json:"returnUrl,omitempty"`
	FsizeMin            int64    `json:"fsizeMin,omitempty"`
	FsizeLimit          int64    `json:"fsizeLimit,omitempty"`
	MimeLimit           string   `json:"mimeLimit,omitempty"`
	SaveKey             string   `json:"saveKey,omitempty"`
	ForceSaveKey        bool     `json:"forceSaveKey,omitempty"`
	PersistentOps       string   `json:"persistentOps,omitempty"`
	PersistentNotifyUrl string   `json:"persistentNotifyUrl,omitempty"`
	PersistentPipeline  string   `json:"persistentPipeline,omitempty"`
	Checksum            string   `json:"checksum,omitempty"`
	Accesses            []string `json:"accesses,omitempty"`
	DeleteAfterDays     uint32   `json:"deleteAfterDays,omitempty"`
	FileType            FileType `json:"fileType,omitempty"`
	NotifyQueue         string   `json:"notifyQueue,omitempty"`
	NotifyMessage       string   `json:"notifyMessage,omitempty"`
	NotifyMessageType   string   `json:"notifyMessageType,omitempty"`
	CustomEventMessage  string   `json:"customEventMessage,omitempty"`
	//内部参数
	OldFh   string `json:"oldFh,omitempty"`
	PutTime int64  `json:"putTime,omitempty"`
	Cond    string `json:"cond,omitempty"` //格式：condKey1=condVal1&condKey2=condVal2,支持hash、mime、fsize、putTime条件，只有条件匹配才会执行覆盖操作
}

func MakeAuthTokenString(key, secret string, auth *AuthPolicy) string {
	b, _ := json.Marshal(auth)
	mac := &digest.Mac{
		AccessKey: key,
		SecretKey: []byte(secret),
	}
	return mac.SignWithData(b)
}
