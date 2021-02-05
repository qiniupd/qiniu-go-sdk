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
const initPartRetryTimes = 5
const uploadPartRetryTimes = 5
const deletePartsRetryTimes = 10
const completePartsRetryTimes = 5

var ErrMd5NotMatch = httputil.NewError(406, "md5 not match")

//https://github.com/qbox/product/blob/master/kodo/resumable-up-v2/init_parts.md
func (p Uploader) initParts(ctx context.Context, host, bucket, key string, hasKey bool) (uploadId string, suggestedPartSize int64, err error) {
	url1 := fmt.Sprintf("%s/buckets/%s/objects/%s/uploads", host, bucket, encodeKey(key, hasKey))
	ret := struct {
		UploadId          string `json:"uploadId"`
		SuggestedPartSize int64  `json:"suggestedPartSize,omitempty"`
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
func (p Uploader) uploadPart(ctx context.Context, host, bucket, key string, hasKey bool, uploadId string, partNum int, body io.Reader, bodyLen int) (ret UploadPartRet, err error) {
	url1 := fmt.Sprintf("%s/buckets/%s/objects/%s/uploads/%s/%d", host, bucket, encodeKey(key, hasKey), uploadId, partNum)
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
func (p Uploader) completeParts(ctx context.Context, host string, ret interface{}, bucket, key string, hasKey bool, uploadId string, mPart *CompleteMultipart) error {
	key = encodeKey(key, hasKey)

	metaData := make(map[string]string)
	for k, v := range mPart.Metadata {
		metaData["x-qn-meta-"+k] = v
	}
	mp := *mPart
	mp.Metadata = metaData

	url1 := fmt.Sprintf("%s/buckets/%s/objects/%s/uploads/%s", host, bucket, key, uploadId)
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

type PartData struct {
	Data   io.ReaderAt
	Size   int
	Finish func()
}

//https://github.com/qbox/product/blob/master/kodo/resumable-up-v2/delete_parts.md
func (p Uploader) deleteParts(ctx context.Context, host, bucket, key string, hasKey bool, uploadId string) error {
	url1 := fmt.Sprintf("%s/buckets/%s/objects/%s/uploads/%s", host, bucket, encodeKey(key, hasKey), uploadId)
	return p.Conn.Call(ctx, nil, "DELETE", url1)
}

func (p Uploader) Upload(ctx context.Context, ret interface{}, uptoken string, key string, f io.ReaderAt, fsize int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {
	uploadParts := p.makeUploadParts(fsize)
	return p.upload(ctx, ret, uptoken, key, true, f, fsize, uploadParts, mp, partNotify)
}

func (p Uploader) UploadWithParts(ctx context.Context, ret interface{}, uptoken string, key string, f io.ReaderAt, fsize int64, uploadParts []int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {
	if !p.checkUploadParts(fsize, uploadParts) {
		return errors.New("part size not equal with fsize")
	}
	return p.upload(ctx, ret, uptoken, key, true, f, fsize, uploadParts, mp, partNotify)
}

func (p Uploader) UploadWithDataChan(ctx context.Context, ret interface{}, uptoken string, key string, dataCh chan PartData,
	mp *CompleteMultipart, initNotify func(suggestedPartSize int64), partNotify func(partIdx int, etag string)) error {
	return p.uploadWithDataChan(ctx, ret, uptoken, key, true, dataCh, mp, initNotify, partNotify)
}

func (p Uploader) UploadWithoutKey(ctx context.Context, ret interface{}, uptoken string, f io.ReaderAt, fsize int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {
	uploadParts := p.makeUploadParts(fsize)
	return p.upload(ctx, ret, uptoken, "", false, f, fsize, uploadParts, mp, partNotify)
}

func (p Uploader) UploadWithoutKeyWithParts(ctx context.Context, ret interface{}, uptoken string, f io.ReaderAt, fsize int64, uploadParts []int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {
	if !p.checkUploadParts(fsize, uploadParts) {
		return errors.New("part size not equal with fsize")
	}
	return p.upload(ctx, ret, uptoken, "", false, f, fsize, uploadParts, mp, partNotify)
}

func (p Uploader) upload(ctx context.Context, ret interface{}, uptoken, key string, hasKey bool, f io.ReaderAt, fsize int64, uploadParts []int64,
	mp *CompleteMultipart, partNotify func(partIdx int, etag string)) error {

	if fsize == 0 {
		return errors.New("can't upload empty file")
	}

	policy, err := kodo.ParseUptoken(uptoken)
	if err != nil {
		return err
	}
	bucket := strings.Split(policy.Scope, ":")[0]

	p.Conn.Client = newUptokenClient(uptoken, p.Conn.Transport)
	uploadId, _, err := p.initPartsWithRetry(ctx, bucket, key, hasKey)
	if err != nil {
		return err
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
			var buf []byte = nil
			if p.UseBuffer {
				buf, err = ioutil.ReadAll(io.NewSectionReader(f, offset, partSize))
				if err != nil {
					partUpErrLock.Lock()
					partUpErr = err
					partUpErrLock.Unlock()
					elog.Error(xl.ReqId(), "uploadPartErr:", partNum, err)
					cancel()
					return
				}
			}

			getBody := func() (io.Reader, int) {
				if buf == nil {
					return io.NewSectionReader(f, offset, partSize), int(partSize)
				} else {
					return bytes.NewReader(buf), len(buf)
				}
			}
			ret, err := p.uploadPartWithRetry(partUpCtx, bucket, key, hasKey, uploadId, partNum, getBody)
			if err != nil {
				partUpErrLock.Lock()
				partUpErr = err
				partUpErrLock.Unlock()
				elog.Error(xl.ReqId(), "uploadPartErr:", partNum, err)
				cancel()
				return
			}
			parts[partNum-1] = Part{partNum, ret.Etag}
			if partNotify != nil {
				partNotify(partNum, ret.Etag)
			}
		}(f, offset, i+1, partSize)
	}
	wg.Wait()

	if partUpErr != nil {
		err = p.deletePartsWithRetry(ctx, bucket, key, hasKey, uploadId)
		if err != nil {
			return err
		}
		return partUpErr
	}

	if mp == nil {
		mp = &CompleteMultipart{}
	}
	mp.Parts = parts
	return p.completePartsWithRetry(ctx, ret, bucket, key, hasKey, uploadId, mp)
}

func (p Uploader) uploadWithDataChan(ctx context.Context, ret interface{}, uptoken, key string, hasKey bool, dataCh chan PartData,
	mp *CompleteMultipart, initNotify func(suggestedPartSize int64), partNotify func(partIdx int, etag string)) error {

	policy, err := kodo.ParseUptoken(uptoken)
	if err != nil {
		return err
	}
	bucket := strings.Split(policy.Scope, ":")[0]

	p.Conn.Client = newUptokenClient(uptoken, p.Conn.Transport)
	upHost := p.chooseUpHost()
	uploadId, partSize, err := p.initParts(ctx, upHost, bucket, key, hasKey)
	if err != nil {
		p.punishHost(upHost, err)
		return err
	}
	p.rewardHost(upHost)

	if initNotify != nil {
		initNotify(partSize)
	}

	var partUpErr error
	partUpErrLock := sync.Mutex{}
	parts := make([]Part, 0)
	partsLock := sync.Mutex{}
	partUpCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	getPartData := func() (PartData, bool) {
		var part PartData
		var ok bool = false
		select {
		case part, ok = <-dataCh:
		case <-partUpCtx.Done():
		}
		return part, ok
	}

	var bkLimit = limit.NewBlockingCount(p.Concurrency)
	var wg sync.WaitGroup
	var partNum int = 0
	for {
		part, ok := getPartData()
		if !ok {
			break
		}
		wg.Add(1)
		bkLimit.Acquire(nil)
		partNum += 1
		go func(part PartData, partNum int) {
			defer func() {
				part.Finish()
				bkLimit.Release(nil)
				wg.Done()
			}()
			xl := xlog.NewWith(xlog.FromContextSafe(ctx).ReqId() + "." + fmt.Sprint(partNum))

			getBody := func() (io.Reader, int) {
				return io.NewSectionReader(part.Data, 0, int64(part.Size)), part.Size
			}
			ret, err := p.uploadPartWithRetry(partUpCtx, bucket, key, hasKey, uploadId, partNum, getBody)
			if err != nil {
				partUpErrLock.Lock()
				partUpErr = err
				partUpErrLock.Unlock()
				elog.Error(xl.ReqId(), "uploadPartErr:", partNum, err)
				cancel()
				return
			} else {
				partsLock.Lock()
				parts = append(parts, Part{partNum, ret.Etag})
				partsLock.Unlock()
				if partNotify != nil {
					partNotify(partNum, ret.Etag)
				}
			}
		}(part, partNum)
	}
	wg.Wait()

	if partUpErr != nil {
		err = p.deletePartsWithRetry(ctx, bucket, key, hasKey, uploadId)
		if err != nil {
			return err
		}
		return partUpErr
	}

	completeMultipart := CompleteMultipart{Parts: parts}
	completeMultipart.Sort()

	return p.completePartsWithRetry(ctx, ret, bucket, key, hasKey, uploadId, mp)
}

func (p Uploader) makeUploadParts(fsize int64) []int64 {
	partCnt := p.partNumber(fsize)
	uploadParts := make([]int64, partCnt)
	for i := 0; i < partCnt-1; i++ {
		uploadParts[i] = p.UploadPartSize
	}
	uploadParts[partCnt-1] = fsize - (int64(partCnt)-1)*p.UploadPartSize
	return uploadParts
}

func (p Uploader) checkUploadParts(fsize int64, uploadParts []int64) bool {
	var partSize int64 = 0
	for _, size := range uploadParts {
		partSize += size
	}
	return fsize == partSize
}

func (p Uploader) partNumber(fsize int64) int {
	return int((fsize + p.UploadPartSize - 1) / p.UploadPartSize)
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

func (p Uploader) StreamUpload(ctx context.Context, ret interface{}, uptoken, key string, reader io.Reader, partNotify func(partIdx int, etag string)) error {
	return p.streamUpload(ctx, ret, uptoken, key, true, reader, partNotify)
}

func (p Uploader) StreamUploadWithoutKey(ctx context.Context, ret interface{}, uptoken string, reader io.Reader, partNotify func(partIdx int, etag string)) error {
	return p.streamUpload(ctx, ret, uptoken, "", false, reader, partNotify)
}

func (p Uploader) streamUpload(ctx context.Context, ret interface{}, uptoken, key string, hasKey bool, reader io.Reader, partNotify func(partIdx int, etag string)) error {
	policy, err := kodo.ParseUptoken(uptoken)
	if err != nil {
		return err
	}
	bucket := strings.Split(policy.Scope, ":")[0]

	p.Conn.Client = newUptokenClient(uptoken, p.Conn.Transport)
	upHost := p.chooseUpHost()
	uploadId, partSize, err := p.initParts(ctx, upHost, bucket, key, hasKey)
	if err != nil {
		p.punishHost(upHost, err)
		return err
	}
	p.rewardHost(upHost)

	if partSize == 0 {
		partSize = p.UploadPartSize
	}

	partUpCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var parts []Part
	var partsLock sync.Mutex

	var wg sync.WaitGroup
	type PartData struct {
		Data       []byte
		PartNumber int
	}
	partChan := make(chan PartData)
	errorChan := make(chan error, p.Concurrency)

	for i := 0; i < p.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-partUpCtx.Done():
					return
				case partData, ok := <-partChan:
					if !ok {
						return
					}
					getBody := func() (io.Reader, int) {
						return bytes.NewReader(partData.Data), len(partData.Data)
					}
					ret, err := p.uploadPartWithRetry(partUpCtx, bucket, key, hasKey, uploadId, partData.PartNumber, getBody)
					if err != nil {
						if err != context.Canceled {
							errorChan <- err
							cancel()
						}
						return
					}
					partsLock.Lock()
					parts = append(parts, Part{PartNumber: partData.PartNumber, Etag: ret.Etag})
					partsLock.Unlock()
					if partNotify != nil {
						partNotify(partData.PartNumber, ret.Etag)
					}
				}
			}
		}()
	}

	for partNum := 1; ; partNum++ {
		data, err := ioutil.ReadAll(io.LimitReader(reader, partSize))
		if err != nil {
			close(partChan)
			return err
		} else if len(data) == 0 {
			break
		} else {
			partChan <- PartData{Data: data, PartNumber: partNum}
		}
	}
	close(partChan)
	wg.Wait()
	close(errorChan)
	partUpErr := <-errorChan
	if partUpErr != nil {
		err = p.deletePartsWithRetry(ctx, bucket, key, hasKey, uploadId)
		if err != nil {
			return err
		}
		return partUpErr
	}
	completeMultipart := CompleteMultipart{Parts: parts}
	completeMultipart.Sort()

	return p.completePartsWithRetry(ctx, ret, bucket, key, hasKey, uploadId, &completeMultipart)
}

func (p Uploader) initPartsWithRetry(ctx context.Context, bucket, key string, hasKey bool) (uploadId string, suggestedPartSize int64, err error) {
	xl := xlog.FromContextSafe(ctx)

	for i := 0; i < initPartRetryTimes; i++ {
		upHost := p.chooseUpHost()
		uploadId, suggestedPartSize, err = p.initParts(ctx, upHost, bucket, key, hasKey)
		if err == context.Canceled {
			break
		}
		code := httputil.DetectCode(err)
		if err == nil || code/100 == 4 {
			p.rewardHost(upHost)
			break
		} else {
			p.punishHost(upHost, err)
			elog.Error(xl.ReqId(), "initParts:", err, code)
			time.Sleep(time.Second * 3)
		}
	}
	return
}

func (p Uploader) uploadPartWithRetry(ctx context.Context, bucket, key string, hasKey bool, uploadId string, partNum int, getBody func() (io.Reader, int)) (ret UploadPartRet, err error) {
	xl := xlog.NewWith(xlog.FromContextSafe(ctx).ReqId() + "." + fmt.Sprint(partNum))
	tryTimes := uploadPartRetryTimes

	for {
		upHost := p.chooseUpHost()
		bodyReader, bodySize := getBody()
		ret, err = p.uploadPart(ctx, upHost, bucket, key, hasKey, uploadId, partNum, bodyReader, bodySize)
		if err == context.Canceled {
			break
		}
		code := httputil.DetectCode(err)
		if code == 509 { // 因为流量受限失败，不减少重试次数
			p.punishHost(upHost, err)
			elog.Warn(xl.ReqId(), "uploadPartRetryLater:", partNum, err)
			time.Sleep(time.Second * time.Duration(rand.Intn(9)+1))
		} else if code == 406 || code/100 != 4 {
			p.punishHost(upHost, err)
			if tryTimes <= 0 {
				break
			}
			tryTimes--
			elog.Warn(xl.ReqId(), "uploadPartRetry:", partNum, err)
			time.Sleep(time.Second * 3)
		} else {
			p.rewardHost(upHost)
			break
		}
	}
	return
}

func (p Uploader) completePartsWithRetry(ctx context.Context, ret interface{}, bucket, key string, hasKey bool, uploadId string, mp *CompleteMultipart) (err error) {
	xl := xlog.FromContextSafe(ctx)

	for i := 0; i < completePartsRetryTimes; i++ {
		upHost := p.chooseUpHost()
		err = p.completeParts(ctx, upHost, ret, bucket, key, hasKey, uploadId, mp)
		if err == context.Canceled {
			break
		}
		code := httputil.DetectCode(err)
		if err == nil || code/100 == 4 || code == 612 || code == 614 || code == 579 {
			p.rewardHost(upHost)
			if code == 612 || code == 614 {
				elog.Warn(xl.ReqId(), "completeParts:", err)
				err = nil
			}
			break
		} else {
			p.punishHost(upHost, err)
			elog.Error(xl.ReqId(), "completeParts:", err, code)
			time.Sleep(time.Second * 3)
		}
	}
	return
}

func (p Uploader) deletePartsWithRetry(ctx context.Context, bucket, key string, hasKey bool, uploadId string) (err error) {
	xl := xlog.FromContextSafe(ctx)

	for i := 0; i < deletePartsRetryTimes; i++ {
		upHost := p.chooseUpHost()
		err = p.deleteParts(ctx, upHost, bucket, key, hasKey, uploadId)
		if err == context.Canceled {
			break
		}
		code := httputil.DetectCode(err)
		if err == nil || code/100 == 4 {
			p.rewardHost(upHost)
			break
		} else {
			p.punishHost(upHost, err)
			elog.Error(xl.ReqId(), "deleteParts:", err)
			time.Sleep(time.Second * 3)
		}
	}
	return
}
