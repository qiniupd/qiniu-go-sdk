package kodocli

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/limit"
	"github.com/qiniupd/qiniu-go-sdk/x/httputil.v1"
	"github.com/qiniupd/qiniu-go-sdk/x/xlog.v8"
)

const minUploadPartSize = 1 << 22
const uploadPartRetryTimes = 5
const deletePartsRetryTimes = 10
const completePartsRetryTimes = 5

var ErrMd5NotMatch = httputil.NewError(406, "md5 not match")

//https://github.com/qbox/product/blob/master/kodo/resumable-up-v2/init_parts.md
func (p Uploader) initParts(ctx context.Context, bucket, key string) (uploadId string, suggestedPartSize int64, err error) {
	url1 := fmt.Sprintf("%s/buckets/%s/objects/%s/uploads", p.chooseUpHost(), bucket, encode(key))
	ret := struct {
		UploadId          string `json:"uploadId"`
		SuggestedPartSize int64  `json:"suggestedPartSize"`
	}{}

	err = p.Conn.Call(ctx, &ret, "POST", url1)
	uploadId = ret.UploadId
	suggestedPartSize = ret.SuggestedPartSize
	return
}

type UploadPartRet struct {
	Etag string `json:"etag"`
	Md5  string `json:"md5"`
}

//https://github.com/qbox/product/blob/master/kodo/resumable-up-v2/upload_parts.md
func (p Uploader) uploadPart(ctx context.Context, bucket, key, uploadId string, partNum int, body io.Reader, bodyLen int) (ret UploadPartRet, err error) {
	url1 := fmt.Sprintf("%s/buckets/%s/objects/%s/uploads/%s/%d", p.chooseUpHost(), bucket, encode(key), uploadId, partNum)
	h := md5.New()
	tr := io.TeeReader(body, h)

	err = p.Conn.CallWith(ctx, &ret, "PUT", url1, "application/octet-stream", tr, bodyLen)
	if err != nil {
		return
	}

	partMd5 := hex.EncodeToString(h.Sum(nil))
	if partMd5 != ret.Md5 {
		err = ErrMd5NotMatch
	}

	return
}

type CompleteMultipart struct {
	Parts      []Part            `json:"parts"`
	Fname      string            `json:"fname"`
	MimeType   string            `json:"mimeType"`
	Metadata   map[string]string `json:"metadata"`
	CustomVars map[string]string `json:"customVars"`
}

type Part struct {
	PartNumber int    `json:"partNumber"`
	Etag       string `json:"etag"`
}

//https://github.com/qbox/product/blob/master/kodo/resumable-up-v2/complete_parts.md
func (p Uploader) completeParts(ctx context.Context, ret interface{}, bucket, key string, hasKey bool, uploadId string, mPart *CompleteMultipart) error {
	key = encode(key)
	if !hasKey {
		key = "~"
	}

	metaData := make(map[string]string)
	for k, v := range mPart.Metadata {
		metaData["x-qn-meta-"+k] = v
	}
	mp := *mPart
	mp.Metadata = metaData

	url1 := fmt.Sprintf("%s/buckets/%s/objects/%s/uploads/%s", p.chooseUpHost(), bucket, key, uploadId)
	return p.Conn.CallWithJson(ctx, &ret, "POST", url1, mp)
}

type CompletePartsRet struct {
	Hash string `json:"hash"`
	Key  string `json:"key"`
}

func (p *CompleteMultipart) Len() int {
	return len(p.Parts)
}

func (p *CompleteMultipart) Swap(i, j int) {
	p.Parts[i], p.Parts[j] = p.Parts[j], p.Parts[i]
}

func (p *CompleteMultipart) Less(i, j int) bool {
	return p.Parts[i].PartNumber < p.Parts[j].PartNumber
}

func (p *CompleteMultipart) Sort() {
	sort.Sort(p)
}

//https://github.com/qbox/product/blob/master/kodo/resumable-up-v2/delete_parts.md
func (p Uploader) deleteParts(ctx context.Context, bucket, key, uploadId string) error {
	url1 := fmt.Sprintf("%s/buckets/%s/objects/%s/uploads/%s", p.chooseUpHost(), bucket, encode(key), uploadId)
	return p.Conn.Call(ctx, nil, "DELETE", url1)
}

func (p Uploader) Upload(ctx context.Context, ret interface{}, uptoken string, key string, f io.ReaderAt, fsize int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {
	return p.upload(ctx, ret, uptoken, key, true, f, fsize, mp, partNotify)
}

func (p Uploader) UploadWithoutKey(ctx context.Context, ret interface{}, uptoken string, f io.ReaderAt, fsize int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {
	return p.upload(ctx, ret, uptoken, "", false, f, fsize, mp, partNotify)
}

func (p Uploader) upload(ctx context.Context, ret interface{}, uptoken, key string, hasKey bool, f io.ReaderAt, fsize int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {

	xl := xlog.FromContextSafe(ctx)
	if fsize == 0 {
		return errors.New("can't upload empty file")
	}

	policy, err := kodo.ParseUptoken(uptoken)
	if err != nil {
		return err
	}
	bucket := strings.Split(policy.Scope, ":")[0]

	p.Conn.Client = newUptokenClient(uptoken, p.Conn.Transport)
	uploadId, suggestedPartSize, err := p.initParts(ctx, bucket, key)
	if err != nil {
		return err
	}

	var uploadParts []int64
	if suggestedPartSize > 0 && p.UploadPartSize != suggestedPartSize {
		uploadParts = p.makeUploadPartsByPartSize(fsize, suggestedPartSize)
	} else {
		uploadParts = p.makeUploadParts(fsize)
	}

	var partUpErr error
	partUpErrLock := sync.Mutex{}
	partCnt := len(uploadParts)
	parts := make([]Part, partCnt)
	partUpCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var bkLimit = limit.NewBlockingCount(p.Concurrency)
	var wg sync.WaitGroup
	var lastPartEnd int64 = 0
	for i := 0; i < partCnt; i++ {
		wg.Add(1)
		bkLimit.Acquire(nil)
		partSize := uploadParts[i]
		offset := lastPartEnd
		lastPartEnd = partSize + offset
		go func(f io.ReaderAt, offset int64, partNum int, partSize int64) {
			defer func() {
				bkLimit.Release(nil)
				wg.Done()
			}()
			select {
			case <-partUpCtx.Done():
				return
			default:
			}
			xl := xlog.NewWith(xlog.FromContextSafe(ctx).ReqId() + "." + fmt.Sprint(partNum))
			tryTimes := uploadPartRetryTimes
		lzRetry:
			var r io.Reader = io.NewSectionReader(f, offset, partSize)
			if p.UseBuffer {
				buf, err := ioutil.ReadAll(r)
				if err != nil {
					partUpErrLock.Lock()
					partUpErr = err
					partUpErrLock.Unlock()
					elog.Error(xl.ReqId(), "uploadPartErr:", partNum, err)
					cancel()
					return
				}
				r = bytes.NewReader(buf)
			}
			ret, err := p.uploadPart(partUpCtx, bucket, key, uploadId, partNum, r, int(partSize))
			if err != nil {
				if err == context.Canceled {
					return
				}

				code := httputil.DetectCode(err)
				if code == 509 { // 因为流量受限失败，不减少重试次数
					elog.Warn(xl.ReqId(), "uploadPartRetryLater:", partNum, err)
					time.Sleep(time.Second * time.Duration(rand.Intn(9)+1))
					goto lzRetry
				} else if tryTimes > 1 && (code == 406 || code/100 != 4) {
					tryTimes--
					elog.Warn(xl.ReqId(), "uploadPartRetry:", partNum, err)
					time.Sleep(time.Second * 3)
					goto lzRetry
				}

				partUpErrLock.Lock()
				partUpErr = err
				partUpErrLock.Unlock()
				elog.Error(xl.ReqId(), "uploadPartErr:", partNum, err)
				cancel()
				return
			} else {
				parts[partNum-1] = Part{partNum, ret.Etag}
				if partNotify != nil {
					partNotify(partNum, ret.Etag)
				}
			}
		}(f, offset, i+1, partSize)
	}
	wg.Wait()

	if partUpErr != nil {
		for i := 0; i < deletePartsRetryTimes; i++ {
			err = p.deleteParts(ctx, bucket, key, uploadId)
			code := httputil.DetectCode(err)
			if err == nil || code/100 == 4 {
				break
			} else {
				elog.Error(xl.ReqId(), "deleteParts:", err)
				time.Sleep(time.Second * 3)
			}
		}
		if err != nil {
			return err
		}
		return partUpErr
	}

	if mp == nil {
		mp = &CompleteMultipart{}
	}
	mp.Parts = parts

	for i := 0; i < completePartsRetryTimes; i++ {
		err = p.completeParts(ctx, ret, bucket, key, hasKey, uploadId, mp)
		code := httputil.DetectCode(err)
		if err == nil || code/100 == 4 || code == 612 || code == 614 || code == 579 {
			if code == 612 || code == 614 {
				elog.Warn(xl.ReqId(), "completeParts:", err)
				err = nil
			}
			break
		} else {
			elog.Error(xl.ReqId(), "completeParts:", err, code)
			time.Sleep(time.Second * 3)
		}
	}
	return err
}

func (p Uploader) makeUploadParts(fsize int64) []int64 {
	return p.makeUploadPartsByPartSize(fsize, p.UploadPartSize)
}

func (p Uploader) makeUploadPartsByPartSize(fsize, uploadPartSize int64) []int64 {
	partCnt := p.partNumber(fsize, uploadPartSize)
	uploadParts := make([]int64, partCnt)
	for i := 0; i < partCnt-1; i++ {
		uploadParts[i] = uploadPartSize
	}
	uploadParts[partCnt-1] = fsize - (int64(partCnt)-1)*uploadPartSize
	return uploadParts
}

func (p Uploader) partNumber(fsize, uploadPartSize int64) int {
	return int((fsize + uploadPartSize - 1) / uploadPartSize)
}

func (p Uploader) StreamUpload(ctx context.Context, ret interface{}, uptoken string, key string, f io.Reader, fsize int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {
	return p.streamUpload(ctx, ret, uptoken, key, true, f, fsize, mp, partNotify)
}

func (p Uploader) StreamUploadWithoutKey(ctx context.Context, ret interface{}, uptoken string, f io.Reader, fsize int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {
	return p.streamUpload(ctx, ret, uptoken, "", false, f, fsize, mp, partNotify)
}

func NewSectionReader(r io.Reader, n int64) *sectionReader {
	return &sectionReader{r, 0, n}
}

type sectionReader struct {
	r     io.Reader
	off   int64
	limit int64
}

func (s *sectionReader) Read(p []byte) (n int, err error) {
	if s.off >= s.limit {
		return 0, io.EOF
	}
	if max := s.limit - s.off; int64(len(p)) > max {
		p = p[0:max]
	}
	n, err = s.r.Read(p)
	s.off += int64(n)
	return
}

func (p Uploader) streamUpload(ctx context.Context, ret interface{}, uptoken, key string, hasKey bool, f io.Reader, fsize int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {

	xl := xlog.FromContextSafe(ctx)
	if fsize == 0 {
		return errors.New("can't upload empty file")
	}

	policy, err := kodo.ParseUptoken(uptoken)
	if err != nil {
		return err
	}
	bucket := strings.Split(policy.Scope, ":")[0]

	p.Conn.Client = newUptokenClient(uptoken, p.Conn.Transport)
	uploadId, suggestedPartSize, err := p.initParts(ctx, bucket, key)
	if err != nil {
		return err
	}

	var uploadParts []int64
	if suggestedPartSize > 0 && p.UploadPartSize != suggestedPartSize {
		uploadParts = p.makeUploadPartsByPartSize(fsize, suggestedPartSize)
	} else {
		uploadParts = p.makeUploadParts(fsize)
	}

	var partUpErr error
	partCnt := len(uploadParts)
	parts := make([]Part, partCnt)

	for i := 0; i < partCnt; i++ {
		partSize := uploadParts[i]
		partNum := i + 1
		xl := xlog.NewWith(xlog.FromContextSafe(ctx).ReqId() + "." + fmt.Sprint(partNum))
		r := NewSectionReader(f, partSize)
		ret, err := p.uploadPart(ctx, bucket, key, uploadId, partNum, r, int(partSize))
		if err != nil {
			partUpErr = err
			elog.Error(xl.ReqId(), "uploadPartErr:", partNum, err)
			break
		} else {
			parts[partNum-1] = Part{partNum, ret.Etag}
			if partNotify != nil {
				partNotify(partNum, ret.Etag)
			}
		}
	}

	if partUpErr != nil {
		for i := 0; i < deletePartsRetryTimes; i++ {
			err = p.deleteParts(ctx, bucket, key, uploadId)
			code := httputil.DetectCode(err)
			if err == nil || code/100 == 4 {
				break
			} else {
				elog.Error(xl.ReqId(), "deleteParts:", err)
				time.Sleep(time.Second * 3)
			}
		}
		if err != nil {
			return err
		}
		return partUpErr
	}

	if mp == nil {
		mp = &CompleteMultipart{}
	}
	mp.Parts = parts

	for i := 0; i < completePartsRetryTimes; i++ {
		err = p.completeParts(ctx, ret, bucket, key, hasKey, uploadId, mp)
		code := httputil.DetectCode(err)
		if err == nil || code/100 == 4 || code == 612 || code == 579 {
			if code == 612 {
				elog.Warn(xl.ReqId(), "completeParts:", err)
				err = nil
			}
			break
		} else {
			elog.Error(xl.ReqId(), "completeParts:", err)
			time.Sleep(time.Second * 3)
		}
	}
	return err
}
