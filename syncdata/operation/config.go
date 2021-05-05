package operation

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/pelletier/go-toml"
	"github.com/qiniupd/qiniu-go-sdk/x/log.v7"
)

type Config struct {
	UpHosts       []string `json:"up_hosts" toml:"up_hosts"`
	RsHosts       []string `json:"rs_hosts" toml:"rs_hosts"`
	RsfHosts      []string `json:"rsf_hosts" toml:"rsf_hosts"`
	Bucket        string   `json:"bucket" toml:"bucket"`
	Ak            string   `json:"ak" toml:"ak"`
	Sk            string   `json:"sk" toml:"sk"`
	PartSize      int64    `json:"part" toml:"part"`
	Addr          string   `json:"addr" toml:"addr"`
	Delete        bool     `json:"delete" toml:"delete"`
	UpConcurrency int      `json:"up_concurrency" toml:"up_concurrency"`

	DownPath string `json:"down_path" toml:"down_path"`
	Sim      bool   `json:"sim" toml:"sim"`

	IoHosts []string `json:"io_hosts" toml:"io_hosts"`

	UcHosts []string `json:"uc_hosts" toml:"uc_hosts"`
}

func dupStrings(s []string) []string {
	if s == nil || len(s) == 0 {
		return s
	}
	to := make([]string, len(s))
	copy(to, s)
	return to
}

func Load(file string) (*Config, error) {
	var configuration Config
	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	ext := path.Ext(file)
	ext = strings.ToLower(ext)
	if ext == ".json" {
		err = json.Unmarshal(raw, &configuration)
	} else if ext == ".toml" {
		err = toml.Unmarshal(raw, &configuration)
	} else {
		return nil, errors.New("configuration format invalid!")
	}

	return &configuration, err
}

var g_conf *Config
var confLock sync.Mutex

func getConf() *Config {
	up := os.Getenv("QINIU")
	if up == "" {
		log.Warn("not set qiniu environment")
		return nil
	}
	confLock.Lock()
	defer confLock.Unlock()
	if g_conf != nil {
		return g_conf
	}
	c, err := Load(up)
	if err != nil {
		log.Warn("load conf failed", up, err)
		return nil
	}
	g_conf = c
	watchConfig(up)
	return c
}

func watchConfig(filename string) {
	initWG := sync.WaitGroup{}
	initWG.Add(1)
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			elog.Fatal(err)
		}
		defer watcher.Close()

		configFile := filepath.Clean(filename)
		configDir, _ := filepath.Split(configFile)
		realConfigFile, _ := filepath.EvalSymlinks(filename)

		eventsWG := sync.WaitGroup{}
		eventsWG.Add(1)
		go func() {
			for {
				select {
				case event, ok := <-watcher.Events:
					if !ok { // 'Events' channel is closed
						eventsWG.Done()
						return
					}
					currentConfigFile, _ := filepath.EvalSymlinks(filename)
					// we only care about the config file with the following cases:
					// 1 - if the config file was modified or created
					// 2 - if the real path to the config file changed (eg: k8s ConfigMap replacement)
					const writeOrCreateMask = fsnotify.Write | fsnotify.Create
					if (filepath.Clean(event.Name) == configFile &&
						event.Op&writeOrCreateMask != 0) ||
						(currentConfigFile != "" && currentConfigFile != realConfigFile) {
						realConfigFile = currentConfigFile
						c, err := Load(realConfigFile)
						if err != nil {
							elog.Error("reload config file error", err)
						} else if err == nil {
							elog.Info("reload config file")
							g_conf = c
						}
					} else if filepath.Clean(event.Name) == configFile &&
						event.Op&fsnotify.Remove&fsnotify.Remove != 0 {
						eventsWG.Done()
						return
					}

				case err, ok := <-watcher.Errors:
					if ok { // 'Errors' channel is not closed
						elog.Error("watcher error", err)
					}
					eventsWG.Done()
					return
				}
			}
		}()
		watcher.Add(configDir)
		initWG.Done()   // done initializing the watch in this go routine, so the parent routine can move on...
		eventsWG.Wait() // now, wait for event loop to end in this go-routine...
	}()
	initWG.Wait() // make sure that the go routine above fully ended before returning
}
