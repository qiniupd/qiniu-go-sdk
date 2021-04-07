package curl

import (
	"sync"

	libcurl "github.com/andelf/go-curl"
)

type easyPoolImpl struct {
	objectsPool sync.Pool
}

func newEasyPoolImpl() *easyPoolImpl {
	return &easyPoolImpl{objectsPool: sync.Pool{
		New: func() interface{} {
			return libcurl.EasyInit()
		},
	}}
}

func (impl *easyPoolImpl) Get() *libcurl.CURL {
	curl := impl.objectsPool.Get().(*libcurl.CURL)
	return curl
}

func (impl *easyPoolImpl) Put(curl *libcurl.CURL) {
	curl.Reset()
	impl.objectsPool.Put(curl)
}
