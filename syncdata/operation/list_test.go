package operation

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

var (
	cfg      *Config
	uploader *Uploader
	lister   *Lister
)

func init() {
	cfg = newTestConfig()
	uploader = NewUploader(cfg)
	lister = NewLister(cfg)
}

func TestListRename(t *testing.T) {
	//upload file
	testKey := fmt.Sprintf("test_list_%d", rand.Int())
	err := uploader.Upload("config.go", testKey)
	assert.NoError(t, err)

	//rename
	testKeyRename := fmt.Sprintf("test_list_rename_%d", rand.Int())
	err = lister.Rename(testKey, testKeyRename)
	assert.NoError(t, err)
	defer lister.Delete(testKeyRename)

	//check rename success
	_, err = lister.Stat(testKey)
	assert.Error(t, err)
	_, err = lister.Stat(testKeyRename)
	assert.NoError(t, err)
}

func TestListPrefix1(t *testing.T) {
	//upload file
	prefix := fmt.Sprintf("%d", rand.Int())
	testKey := fmt.Sprintf("%s_test_list_prefix", prefix)
	err := uploader.Upload("config.go", testKey)
	assert.NoError(t, err)
	defer lister.Delete(testKey)

	//list prefix
	result := lister.ListPrefix(prefix)
	assert.Equal(t, testKey, result[0])
}

func TestListPrefix2(t *testing.T) {
	//upload 1500 files
	prefix := fmt.Sprintf("%d", rand.Int())
	testKey := fmt.Sprintf("%s_test_list_prefix", prefix)
	err := uploader.Upload("config.go", testKey)
	assert.NoError(t, err)
	defer lister.Delete(testKey)

	//list prefix
	result := lister.ListPrefix(prefix)
	assert.Equal(t, testKey, result[0])
}

func TestListStat(t *testing.T) {
	//upload file
	testKey := fmt.Sprintf("test_list_%d", rand.Int())
	testNonKey := fmt.Sprintf("test_list_%d", rand.Int())
	err := uploader.Upload("config.go", testKey)
	assert.NoError(t, err)
	defer lister.Delete(testKey)

	//check liststat success
	result := lister.ListStat([]string{testKey})
	assert.Equal(t, testKey, result[0].Name)

	//check liststst non-exist key
	result = lister.ListStat([]string{testNonKey})
	assert.Equal(t, int64(-1), result[0].Size)
}
