package operation

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type server struct {
	up       *Uploader
	lister   *Lister
	del      bool
	downPath string
	sim      bool
}

type Req struct {
	Path   string `json:"path"`
	Key    string `json:"key"`
	Delete *bool  `json:"del"`
}

func renameFile(s string) string {
	s = strings.TrimPrefix(s, "/")
	s = strings.ReplaceAll(s, "/", "-")
	return s
}

func (s *server) download(res http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	fPath := s.downPath + renameFile(path)
	f, err := os.Open(fPath)
	log.Println("download", req.Method, path, req.URL.RawQuery, err)
	log.Println("ranges", req.Header.Get("Range"))
	if err != nil {
		res.WriteHeader(http.StatusNotFound)
		return
	}
	ServeContent(res, req, path, time.Now(), f)
	f.Close()
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		if r.URL.Path == "/stat" {
			s.listStat(w, r)
		} else {
			s.upload(w, r)
		}
	case http.MethodHead:
		fallthrough
	case http.MethodGet:
		if r.URL.Path == "/list" {
			s.listFiles(w, r)
		} else {
			s.download(w, r)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *server) listStat(w http.ResponseWriter, r *http.Request) {
	ret := s.lister.batchStat(r.Body)
	if ret == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	j, err := json.Marshal(ret)
	if err != nil {
		log.Println("json marshal error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func (s *server) listFiles(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	ret := s.lister.ListPrefix(prefix)
	if ret == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	j, err := json.Marshal(ret)
	if err != nil {
		log.Println("json marshal error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func (s *server) upload(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	var reqs []Req
	err := d.Decode(&reqs)
	log.Printf("receive request %+v\n", reqs)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	go func() {
		for _, req := range reqs {
			if s.sim {
				err := os.Rename(req.Path, s.downPath+renameFile(req.Path))
				log.Println("move ", req.Path, s.downPath+renameFile(req.Path), err)
			} else {
				key := req.Key
				if key == "" {
					key = req.Path
				}
				s.up.Upload(req.Path, key)
				if req.Delete == nil {
					if s.del {
						os.Remove(req.Path)
					}
				} else if *req.Delete {
					os.Remove(req.Path)
				}
			}
		}
	}()

	w.WriteHeader(http.StatusOK)
}

func StartServer(cfg *Config) (*http.Server, error) {
	s := server{
		up:       NewUploader(cfg),
		del:      cfg.Delete,
		downPath: cfg.DownPath,
		sim:      cfg.Sim,
		lister:   NewLister(cfg),
	}
	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: &s,
	}

	go func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil {
			log.Panicln("upload server failed: " + err.Error())
		}
	}()
	return srv, nil
}
