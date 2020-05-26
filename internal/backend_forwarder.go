package internal

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	. "github.com/kahing/goofys/api/common"
)

type ForwarderEmptyBodyReaderCloser struct {
}

func (*ForwarderEmptyBodyReaderCloser) Read(buf []byte) (int, error) {
	log.Infof("Empty Reader: to read size: %v", len(buf))
	for i := range buf {
		buf[i] = 0
	}
	return len(buf), nil
	// return 0, io.EOF
}
func (*ForwarderEmptyBodyReaderCloser) Close() error {
	return nil
}

type ForwarderBackendLocalItemMeta struct {
	sync.RWMutex `json:"-"`
	Key          string    `json:"key"`
	Size         uint64    `json:"size"`
	LastModified time.Time `json:"last_modified"`
	IsDir        bool      `json:"is_dir"`
	// parent's key
	ParentDir string `json:"parent_dir"`
	// alway empty. existence is for compabilites of other storage
	Etag string `json:"etag"`
	// alway empty. existence is for compabilites of other storage
	StorageClass string `json:"storage_class"`
}

type ForwarderBackend struct {
	sync.RWMutex
	flags          *FlagStorage
	config         *ForwarderBackendConfig
	cap            Capabilities
	localItemMetas map[string]*ForwarderBackendLocalItemMeta
}

func NewForwarderBackend(flags *FlagStorage, config *ForwarderBackendConfig) (*ForwarderBackend, error) {
	s := &ForwarderBackend{
		flags:  flags,
		config: config,
		cap: Capabilities{
			NoParallelMultipart: true,
			Name:                "jm-log-fs-Forwarder",
			MaxMultipartSize:    10 * 1024 * 1024,
			DirBlob:             true,
		},
		localItemMetas: make(map[string]*ForwarderBackendLocalItemMeta),
	}
	if config.PersistentFile != "" {
		log.Infof("checke metadata file: %v", config.PersistentFile)
		d, e := ioutil.ReadFile(config.PersistentFile)
		if e != nil {
			_, isPathErr := e.(*os.PathError)
			if (e == syscall.ENOENT || e == os.ErrNotExist) || isPathErr {
				e = ioutil.WriteFile(config.PersistentFile, []byte("{}"), 0644)
				if e != nil {
					log.Warnf("cannot write forwarder backend file metadata persistence file(%v), error: %v. persistence is to be disabled",
						config.PersistentFile, e)
					config.PersistentFile = ""
				}
			} else {
				log.Warnf("cannot read forwarder backend file metadata persistence file(%v), error: %v. persistence is to be disabled", config.PersistentFile, e)
				config.PersistentFile = ""
			}
		} else {
			e = json.Unmarshal(d, &s.localItemMetas)
			if e != nil {
				log.Warnf("cannot load forwarder backend file metadata persistence file(%v), error: %v.", config.PersistentFile, e)
			}
		}
	}
	if config.PersistentFile != "" {
		config.WG.Add(1)
		go s.persistFileMetadata()
	}
	return s, nil
}

func (b *ForwarderBackend) Watch(fileName string, dataChan chan<- []byte) {

}

func (b *ForwarderBackend) persistFileMetadata() {
	defer b.config.WG.Done()
	tk := time.NewTicker(time.Millisecond * 5000)
	save := func() {
		b.RLock()
		d, err := json.Marshal(b.localItemMetas)
		b.RUnlock()
		if err != nil {
			log.Warningf("failed to serialize file metadats: %v", err)
			return
		}
		err = ioutil.WriteFile(b.config.PersistentFile, d, 0644)
		if err != nil {
			log.Warnf("cannot write forwarder backend file metadata persistence file(%v), error: %v. persistence is to be disabled",
				b.config.PersistentFile, err)
		}
	}

	for {
		select {
		case <-tk.C:
			save()
		case <-b.config.CTX.Done():
			log.Infof("metadata persistent routine exiting...")
			<-tk.C // wait for a while, in case any data op in progress.
			tk.Stop()
			save()
			return
		}
	}
}

func (b *ForwarderBackend) Init(key string) error {
	log.Infof("init call : %s", key)
	return nil
}

func (b *ForwarderBackend) Capabilities() *Capabilities {
	return &b.cap
}

func (b *ForwarderBackend) Bucket() string {
	return ""
}

func (b *ForwarderBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	log.Infof("HeadBlob :%+v", *param)
	var meta *ForwarderBackendLocalItemMeta
	var ok bool
	if meta, ok = b.localItemMetas[param.Key]; !ok {
		return nil, syscall.ENOENT
	}
	o := HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key:          &param.Key,
			Size:         meta.Size,
			LastModified: &meta.LastModified,
		},
		IsDirBlob: meta.IsDir,
		Metadata:  make(map[string]*string),
	}
	log.Infof("Ret headblob: %+v", o)
	return &o, nil
}

func (b *ForwarderBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	log.Infof("ListBlobs :%+v, prefix: %v", *param, *param.Prefix)
	ots := &ListBlobsOutput{}
	b.RLock()
	defer b.RUnlock()
	for _, m := range b.localItemMetas {
		if m.Key != *param.Prefix && m.ParentDir+"/" != *param.Prefix && m.ParentDir != *param.Prefix {
			continue
		}
		item := BlobItemOutput{
			Key:          &m.Key,
			Size:         m.Size,
			LastModified: &m.LastModified,
			IsDirBlob:    m.IsDir,
		}
		ots.Items = append(ots.Items, item)
	}
	log.Infof("Ret listblobs: %+v", ots)
	return ots, nil
}

func (b *ForwarderBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	b.Lock()
	defer b.Unlock()
	if _, ok := b.localItemMetas[param.Key]; !ok {
		return nil, syscall.ENOENT
	}
	delete(b.localItemMetas, param.Key)
	return &DeleteBlobOutput{}, nil
}

func (b *ForwarderBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	b.Lock()
	defer b.Unlock()
	for _, k := range param.Items {
		delete(b.localItemMetas, k)
	}
	return &DeleteBlobsOutput{}, nil
}

func (b *ForwarderBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	log.Infof("RenameBlob %v to %v", param.Source, param.Destination)
	b.Lock()
	defer b.Unlock()
	_sname := strings.TrimRight(param.Source, "/")
	_dname := strings.TrimRight(param.Destination, "/")
	s, ok := b.localItemMetas[_sname]
	if !ok {
		return nil, syscall.ENOENT
	}
	s.Key = _dname
	b.localItemMetas[_dname] = s
	delete(b.localItemMetas, _sname)
	// process items under subdirectories
	for k, m := range b.localItemMetas {
		if strings.Index(k, param.Source) == 0 {
			m.Key = strings.Replace(m.Key, param.Source, param.Destination, 1)
			b.localItemMetas[m.Key] = m
			delete(b.localItemMetas, k)
		}
	}
	return &RenameBlobOutput{}, nil
}

func (b *ForwarderBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	log.Infof("CopyBlob %v to %v", param.Source, param.Destination)
	return &CopyBlobOutput{}, nil
}

func (b *ForwarderBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	log.Infof("GetBlob :%+v", *param)
	hbp := &HeadBlobInput{Key: param.Key}
	hb, err := b.HeadBlob(hbp)
	if err != nil {
		return nil, err
	}
	ob := GetBlobOutput{
		HeadBlobOutput: *hb,
		Body:           &ForwarderEmptyBodyReaderCloser{},
	}

	return &ob, nil
}

func (b *ForwarderBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	log.Infof("PubBlob :%+v", *param)
	if param.Size != nil {
		var data = make([]byte, int(*param.Size))
		io.ReadFull(param.Body, data)

		log.Infof("PutBlob,  size: %v", *param.Size)

	} else {

	}

	b.RLock()
	var meta *ForwarderBackendLocalItemMeta
	var ok bool
	if meta, ok = b.localItemMetas[param.Key]; !ok {
		meta = &ForwarderBackendLocalItemMeta{
			Key:          param.Key,
			LastModified: time.Now(),
			IsDir:        param.DirBlob,
			ParentDir:    "",
		}
		if param.Size != nil {
			meta.Size = *param.Size
		}
		for k := range b.localItemMetas {
			if strings.Index(param.Key, k+"/") == 0 && len(strings.Split(param.Key, "/"))-1 == len(strings.Split(k, "/")) {
				meta.ParentDir = k
				break
			}
		}
	} else {
		if param.Size != nil {
			meta.Size = *param.Size
		}
	}

	b.RUnlock()

	b.Lock()
	b.localItemMetas[param.Key] = meta
	b.Unlock()
	return &PutBlobOutput{LastModified: &meta.LastModified, ETag: &meta.Etag, StorageClass: &meta.StorageClass}, nil
}

func (b *ForwarderBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	log.Infof("MultipartBlobBegin :%+v", *param)

	b.RLock()
	var meta *ForwarderBackendLocalItemMeta
	var ok bool
	if meta, ok = b.localItemMetas[param.Key]; !ok {
		meta = &ForwarderBackendLocalItemMeta{
			Key:          param.Key,
			LastModified: time.Now(),
			ParentDir:    "",
		}
		for k := range b.localItemMetas {
			if strings.Index(param.Key, k) == 0 && len(strings.Split(param.Key, "\\"))-1 == len(strings.Split(k, "\\")) {
				meta.ParentDir = k
				break
			}
		}
	}

	b.RUnlock()
	b.Lock()
	defer b.Unlock()

	b.localItemMetas[param.Key] = meta

	return &MultipartBlobCommitInput{
		Key:      &meta.Key,
		Metadata: param.Metadata,
	}, nil
}

func (b *ForwarderBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	log.Infof("MultipartBlobAdd :%+v", *param)
	b.RLock()
	meta, ok := b.localItemMetas[*param.Commit.Key]
	if !ok {
		b.RUnlock()
		return nil, syscall.ENOENT
	}
	b.RUnlock()
	meta.Lock()
	//TODO: size cannot be exact. body has not been processed yet.
	meta.Size += param.Size
	meta.LastModified = time.Now()
	meta.Unlock()
	return &MultipartBlobAddOutput{}, nil
}

func (b *ForwarderBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	log.Infof("MultipartBlobAbort :%+v", *param)
	return &MultipartBlobAbortOutput{}, nil
}

func (b *ForwarderBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	log.Infof("MultipartBlobCommit :%+v", *param)
	b.RLock()
	b.RUnlock()
	meta, ok := b.localItemMetas[*param.Key]
	if !ok {
		return nil, syscall.ENOENT
	}
	return &MultipartBlobCommitOutput{
		ETag:         &meta.Etag,
		LastModified: &meta.LastModified,
		StorageClass: &meta.StorageClass,
	}, nil
}

func (b *ForwarderBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return &MultipartExpireOutput{}, nil
}

func (b *ForwarderBackend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	log.Infof("RemoveBucket :%+v", *param)
	return &RemoveBucketOutput{}, nil
}

func (b *ForwarderBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	log.Infof("MakeBucket :%+v", *param)
	return &MakeBucketOutput{}, nil
}

func (b *ForwarderBackend) Delegate() interface{} {
	return b
}
