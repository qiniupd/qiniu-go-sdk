package operation

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"path"
	"strings"

	"github.com/pelletier/go-toml"
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
	UpConcurrency int      `json:"up_concurrency"`

	DownPath string `json:"down_path" toml:"down_path"`
	Sim      bool   `json:"sim" toml:"sim"`

	IoHosts []string `json:"io_hosts" toml:"io_hosts"`
	Uid     uint64   `json:"uid" toml:"uid"`
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
