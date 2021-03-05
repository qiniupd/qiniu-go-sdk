package kodocli

import (
	"bytes"
	. "context"
	"encoding/base64"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/x/httputil.v1"
	"github.com/qiniupd/qiniu-go-sdk/x/rpc.v7"
	"github.com/qiniupd/qiniu-go-sdk/x/xlog.v8"
)

// ----------------------------------------------------------

const (
	DontCheckCrc         uint32 = 0
	CalcAndCheckCrc             = 1
	formUploadRetryTimes        = 5
)

// 上传的额外可选项
//
type PutExtra struct {
	// 可选，用户自定义参数，必须以 "x:" 开头。若不以x:开头，则忽略。
	Params map[string]string
	XMeta  map[string]string

	// 可选，当为 "" 时候，服务端自动判断。
	MimeType string

	//CRC32校验
	Crc32 uint32
	//Content-Md5 Trailer
	Md5Trailer func() []byte
	// 上传事件：进度通知。这个事件的回调函数应该尽可能快地结束。
	OnProgress func(fsize, uploaded int64)
}

// ----------------------------------------------------------

// 如果 uptoken 没有指定 ReturnBody，那么返回值是标准的 PutRet 结构
//
type PutRet struct {
	Hash         string `json:"hash"`
	PersistentId string `json:"persistentId"`
	Key          string `json:"key"`
}

// ----------------------------------------------------------

// 上传一个文件。
// 和 Put 不同的只是一个通过提供文件路径来访问文件内容，一个通过 io.Reader 来访问。
//
// ctx       是请求的上下文。
// ret       是上传成功后返回的数据。如果 uptoken 中没有设置 CallbackUrl 或 ReturnBody，那么返回的数据结构是 PutRet 结构。
// uptoken   是由业务服务器颁发的上传凭证。
// key       是要上传的文件访问路径。比如："foo/bar.jpg"。注意我们建议 key 不要以 '/' 开头。另外，key 为空字符串是合法的。
// localFile 是要上传的文件的本地路径。
// extra     是上传的一些可选项。详细见 PutExtra 结构的描述。
//
func (p Uploader) PutFile(
	ctx Context, ret interface{}, uptoken, key, localFile string, extra *PutExtra) (err error) {

	return p.putFile(ctx, ret, uptoken, key, true, localFile, extra)
}

// 上传一个文件。文件的访问路径（key）自动生成。
// 如果 uptoken 中设置了 SaveKey，那么按 SaveKey 要求的规则生成 key，否则自动以文件的 hash 做 key。
// 和 RputWithoutKey 不同的只是一个通过提供文件路径来访问文件内容，一个通过 io.Reader 来访问。
//
// ctx       是请求的上下文。
// ret       是上传成功后返回的数据。如果 uptoken 中没有设置 CallbackUrl 或 ReturnBody，那么返回的数据结构是 PutRet 结构。
// uptoken   是由业务服务器颁发的上传凭证。
// localFile 是要上传的文件的本地路径。
// extra     是上传的一些可选项。详细见 PutExtra 结构的描述。
//
func (p Uploader) PutFileWithoutKey(
	ctx Context, ret interface{}, uptoken, localFile string, extra *PutExtra) (err error) {

	return p.putFile(ctx, ret, uptoken, "", false, localFile, extra)
}

func (p Uploader) putFile(
	ctx Context, ret interface{}, uptoken string,
	key string, hasKey bool, localFile string, extra *PutExtra) (err error) {

	f, err := os.Open(localFile)
	if err != nil {
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return
	}
	fsize := fi.Size()
	return p.put(ctx, ret, uptoken, key, hasKey, f, fsize, extra, filepath.Base(localFile))
}

var defaultPutExtra PutExtra

func (p Uploader) put(
	ctx Context, ret interface{}, uptoken string,
	key string, hasKey bool, dataReaderAt io.ReaderAt, size int64, extra *PutExtra, fileName string) (err error) {

	if extra == nil {
		extra = &defaultPutExtra
	}

	tryTimes := formUploadRetryTimes
	xl := xlog.NewWith(xlog.FromContextSafe(ctx).ReqId())

lzRetry:
	var data io.Reader = io.NewSectionReader(dataReaderAt, 0, size)
	if extra.OnProgress != nil {
		data = &readerWithProgress{reader: data, fsize: size, onProgress: extra.OnProgress}
	}

	b := new(bytes.Buffer)
	writer := multipart.NewWriter(b)

	err = writeMultipart(writer, uptoken, key, hasKey, extra, fileName)
	if err != nil {
		return err
	}

	var dataReader io.Reader = data
	var mr io.Reader
	var bodyLen = int64(-1)
	var head = make(textproto.MIMEHeader)
	head.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, escapeQuotes(fileName)))
	if extra.MimeType != "" {
		head.Set("Content-Type", extra.MimeType)
	}

	lastLine := fmt.Sprintf("\r\n--%s--\r\n", writer.Boundary())
	lastLineSize := int64(len(lastLine))
	lastLineRd := strings.NewReader(lastLine)

	if extra.Crc32 == DontCheckCrc {
		_, err = writer.CreatePart(head)
		if err != nil {
			return err
		}
		if size >= 0 {
			bodyLen = int64(b.Len()) + size + lastLineSize
		}
		mr = io.MultiReader(b, dataReader, lastLineRd)
	} else if extra.Crc32 == CalcAndCheckCrc {
		h := crc32.NewIEEE()
		dataReader = io.TeeReader(data, h)
		crcReader := newCrc32Reader(writer.Boundary(), h)
		_, err = writer.CreatePart(head)
		if err != nil {
			return
		}
		if size >= 0 {
			bodyLen = int64(b.Len()) + size + lastLineSize
			bodyLen += crcReader.length()
		}
		mr = io.MultiReader(b, dataReader, crcReader, lastLineRd)
	} else {
		err = writer.WriteField("crc32", strconv.FormatInt(int64(extra.Crc32), 10))
		if err != nil {
			return err
		}
		_, err = writer.CreatePart(head)
		if err != nil {
			return err
		}
		if size >= 0 {
			bodyLen = int64(b.Len()) + size + lastLineSize
		}
		mr = io.MultiReader(b, dataReader, lastLineRd)
	}

	contentType := writer.FormDataContentType()
	var req *http.Request
	upHost := p.chooseUpHost()
	req, err = rpc.NewRequest("POST", upHost, io.MultiReader(mr, eofReaderFunc(func() {
		if extra.Md5Trailer != nil {
			if m := extra.Md5Trailer(); m != nil && req != nil {
				req.Trailer.Set("Content-Md5", base64.StdEncoding.EncodeToString(m))
			}
		}
	})))
	if err != nil {
		p.punishHost(upHost, err)
		return
	}
	req.Header.Set("Content-Type", contentType)
	req.Trailer = http.Header{"Content-Md5": nil}
	if extra.Md5Trailer == nil {
		req.ContentLength = bodyLen
	}
	resp, err := p.Conn.Do(ctx, req)
	if err != nil {
		if err == Canceled {
			return
		}
		code := httputil.DetectCode(err)
		if code == 509 {
			p.punishHost(upHost, err)
			elog.Warn(xl.ReqId(), "put:", key, "formUploadRetryLater:", err)
			time.Sleep(time.Second * time.Duration(rand.Intn(9)+1))
			goto lzRetry
		} else if tryTimes > 1 && (code == 406 || code/100 != 4) {
			p.punishHost(upHost, err)
			tryTimes--
			elog.Warn(xl.ReqId(), "put:", key, "formUploadRetry:", err)
			time.Sleep(time.Second * 3)
			goto lzRetry
		}
		return err
	}
	err = rpc.CallRet(ctx, ret, resp)
	if err != nil {
		p.punishHost(upHost, err)
	} else {
		p.rewardHost(upHost)
	}
	if extra.OnProgress != nil {
		extra.OnProgress(size, size)
	}
	return err
}

// ----------------------------------------------------------
type eofReaderFunc func()

func (f eofReaderFunc) Read(p []byte) (n int, err error) {
	f()
	return 0, io.EOF
}

// ----------------------------------------------------------

type readerWithProgress struct {
	reader     io.Reader
	uploaded   int64
	fsize      int64
	onProgress func(fsize, uploaded int64)
}

func (p *readerWithProgress) Read(b []byte) (n int, err error) {
	if p.uploaded > 0 {
		p.onProgress(p.fsize, p.uploaded)
	}

	n, err = p.reader.Read(b)
	p.uploaded += int64(n)
	return
}

// ----------------------------------------------------------

func writeMultipart(
	writer *multipart.Writer, uptoken, key string, hasKey bool, extra *PutExtra, fileName string) (err error) {

	//token
	if err = writer.WriteField("token", uptoken); err != nil {
		return
	}

	//key
	if hasKey {
		if err = writer.WriteField("key", key); err != nil {
			return
		}
	}

	//extra.Params
	if extra.Params != nil {
		for k, v := range extra.Params {
			if strings.HasPrefix(k, "x:") {
				err = writer.WriteField(k, v)
				if err != nil {
					return
				}
			}
		}
	}
	if extra.XMeta != nil {
		for k, v := range extra.XMeta {
			if err = writer.WriteField("x-qn-meta-"+k, v); err != nil {
				return
			}
		}
	}
	return err
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}

// ----------------------------------------------------------

type crc32Reader struct {
	h                hash.Hash32
	boundary         string
	r                io.Reader
	flag             bool
	nlDashBoundaryNl string
	header           string
	crc32PadLen      int64
}

func newCrc32Reader(boundary string, h hash.Hash32) *crc32Reader {
	nlDashBoundaryNl := fmt.Sprintf("\r\n--%s\r\n", boundary)
	header := `Content-Disposition: form-data; name="crc32"` + "\r\n\r\n"
	return &crc32Reader{
		h:                h,
		boundary:         boundary,
		nlDashBoundaryNl: nlDashBoundaryNl,
		header:           header,
		crc32PadLen:      10,
	}
}

func (r *crc32Reader) Read(p []byte) (int, error) {
	if r.flag == false {
		crc32Sum := r.h.Sum32()
		crc32Line := r.nlDashBoundaryNl + r.header + fmt.Sprintf("%010d", crc32Sum) //padding crc32 results to 10 digits
		r.r = strings.NewReader(crc32Line)
		r.flag = true
	}
	return r.r.Read(p)
}

func (r crc32Reader) length() (length int64) {
	return int64(len(r.nlDashBoundaryNl+r.header)) + r.crc32PadLen
}

// ----------------------------------------------------------

// 上传一个文件,普通上传。
//
// ctx     是请求的上下文。
// ret     是上传成功后返回的数据。如果 uptoken 中没有设置 CallbackUrl 或 ReturnBody，那么返回的数据结构是 PutRet 结构。
// uptoken 是由业务服务器颁发的上传凭证。
// key     是要上传的文件访问路径。比如："foo/bar.jpg"。注意我们建议 key 不要以 '/' 开头。另外，key 为空字符串是合法的。
// data    是文件内容的访问接口（io.Reader）。
// fsize   是要上传的文件大小。
// extra   是上传的一些可选项。详细见 PutExtra 结构的描述。
//
func (p Uploader) Put2(
	ctx Context, ret interface{}, uptoken, key string, data io.ReaderAt, size int64, extra *PutExtra) error {

	return p.put2(ctx, ret, uptoken, key, data, size, extra)
}

func (p Uploader) put2(ctx Context, ret interface{}, uptoken, key string, data io.ReaderAt, size int64,
	extra *PutExtra) error {

	upHost := p.chooseUpHost()
	url := upHost + "/put/" + strconv.FormatInt(size, 10)
	if extra != nil {
		if extra.MimeType != "" {
			url += "/mimeType/" + base64.URLEncoding.EncodeToString([]byte(extra.MimeType))
		}
		if extra.Crc32 != DontCheckCrc {
			url += "/crc32/" + strconv.FormatInt(int64(extra.Crc32), 10)
		}
		for k, v := range extra.Params {
			if strings.HasPrefix(k, "x:") && v != "" {
				url += "/" + k + "/" + base64.URLEncoding.EncodeToString([]byte(v))
			}
		}
	}

	if key != "" {
		url += "/key/" + base64.URLEncoding.EncodeToString([]byte(key))
	}
	elog.Debug("Put2", key, url)
	req, err := http.NewRequest("POST", url, io.NewSectionReader(data, 0, size))
	if err != nil {
		p.punishHost(upHost, err)
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "UpToken "+uptoken)
	req.ContentLength = size
	resp, err := p.Conn.Do(ctx, req)
	if err != nil {
		p.punishHost(upHost, err)
		return err
	}
	err = rpc.CallRet(ctx, ret, resp)
	if err != nil {
		p.punishHost(upHost, err)
		return err
	}
	p.rewardHost(upHost)
	return nil
}

func (p Uploader) chooseUpHost() string {
	return p.HostSelector.SelectHost()
}

func (p Uploader) punishHost(upHost string, err error) {
	p.HostSelector.PunishIfNeeded(upHost, err)
}

func (p Uploader) rewardHost(upHost string) {
	p.HostSelector.Reward(upHost)
}
