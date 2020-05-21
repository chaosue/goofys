package internal

import (
	"io"
	"strings"
	"sync"
	"syscall"
	"time"

	. "github.com/kahing/goofys/api/common"
)

type KafkaEmptyBodyReaderCloser struct {
}

func (krc *KafkaEmptyBodyReaderCloser) Read(buf []byte) (int, error) {
	log.Infof("Empty Reader: to read size: %v", len(buf))
	for i := range buf {
		buf[i] = 0
	}
	return len(buf), nil
	// return 0, io.EOF
}
func (krc *KafkaEmptyBodyReaderCloser) Close() error {
	return nil
}

type KafkaLocalItemMeta struct {
	sync.RWMutex
	Key          string
	Size         uint64
	LastModified time.Time
	IsDir        bool
	// alway empty. existence is for compabilites of other storage
	Etag string
	// alway empty. existence is for compabilites of other storage
	StorageClass string
}

type KafkaGoofys struct {
	sync.RWMutex
	flags          *FlagStorage
	config         *KafkaConfig
	cap            Capabilities
	localItemMetas map[string]*KafkaLocalItemMeta
}

func NewKafkaGoofys(flags *FlagStorage, config *KafkaConfig) (*KafkaGoofys, error) {
	s := &KafkaGoofys{
		flags:  flags,
		config: config,
		cap: Capabilities{
			NoParallelMultipart: true,
			Name:                "jm-log-fs-kafka",
			MaxMultipartSize:    10 * 1024 * 1024,
			DirBlob:             true,
		},
		localItemMetas: make(map[string]*KafkaLocalItemMeta),
	}
	return s, nil
}

func (b *KafkaGoofys) Init(key string) error {
	log.Infof("init call : %s", key)
	return nil
}

func (b *KafkaGoofys) Capabilities() *Capabilities {
	return &b.cap
}

func (b *KafkaGoofys) Bucket() string {
	return ""
}

func (b *KafkaGoofys) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	log.Infof("HeadBlob :%+v", *param)
	//log.Infof("%s", debug.Stack())
	var meta *KafkaLocalItemMeta
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

func (b *KafkaGoofys) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	log.Infof("ListBlobs :%+v, prefix: %v", *param, *param.Prefix)
	ots := &ListBlobsOutput{}
	b.RLock()
	defer b.RUnlock()
	for _, m := range b.localItemMetas {
		if strings.Index(m.Key, *param.Prefix) != 0 {
			continue
		}
		ots.Items = append(ots.Items, BlobItemOutput{
			Key:          &m.Key,
			Size:         m.Size,
			LastModified: &m.LastModified,
		})
	}
	log.Infof("Ret listblobs: %+v", ots)
	return ots, nil
}

func (b *KafkaGoofys) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	b.Lock()
	defer b.Unlock()
	if _, ok := b.localItemMetas[param.Key]; !ok {
		return nil, syscall.ENOENT
	}
	delete(b.localItemMetas, param.Key)
	return &DeleteBlobOutput{}, nil
}

func (b *KafkaGoofys) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	b.Lock()
	defer b.Unlock()
	for _, k := range param.Items {
		delete(b.localItemMetas, k)
	}
	return &DeleteBlobsOutput{}, nil
}

func (b *KafkaGoofys) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
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

func (b *KafkaGoofys) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	log.Infof("CopyBlob %v to %v", param.Source, param.Destination)
	return &CopyBlobOutput{}, nil
}

func (b *KafkaGoofys) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	log.Infof("GetBlob :%+v", *param)
	hbp := &HeadBlobInput{Key: param.Key}
	hb, err := b.HeadBlob(hbp)
	if err != nil {
		return nil, err
	}
	ob := GetBlobOutput{
		HeadBlobOutput: *hb,
		Body:           &KafkaEmptyBodyReaderCloser{},
	}

	return &ob, nil
}

func (b *KafkaGoofys) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	log.Infof("PubBlob :%+v", *param)
	if param.Size != nil {
		var data = make([]byte, int(*param.Size))
		io.ReadFull(param.Body, data)

		log.Infof("PutBlob,  size: %v", *param.Size)

	} else {

	}

	b.Lock()
	defer b.Unlock()
	meta := KafkaLocalItemMeta{
		Key:          param.Key,
		LastModified: time.Now(),
		IsDir:        param.DirBlob,
	}
	if param.Size != nil {
		meta.Size = *param.Size
	}

	b.localItemMetas[param.Key] = &meta
	return &PutBlobOutput{LastModified: &meta.LastModified, ETag: &meta.Etag, StorageClass: &meta.StorageClass}, nil
}

func (b *KafkaGoofys) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	log.Infof("MultipartBlobBegin :%+v", *param)
	b.Lock()
	defer b.Unlock()
	meta := &KafkaLocalItemMeta{
		Key:          param.Key,
		Size:         0,
		LastModified: time.Now(),
	}
	b.localItemMetas[param.Key] = meta

	return &MultipartBlobCommitInput{
		Key:      &meta.Key,
		Metadata: param.Metadata,
	}, nil
}

func (b *KafkaGoofys) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
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

func (b *KafkaGoofys) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	log.Infof("MultipartBlobAbort :%+v", *param)
	return &MultipartBlobAbortOutput{}, nil
}
func (b *KafkaGoofys) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
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
func (b *KafkaGoofys) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return &MultipartExpireOutput{}, nil
}
func (b *KafkaGoofys) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	log.Infof("RemoveBucket :%+v", *param)
	return &RemoveBucketOutput{}, nil
}

func (b *KafkaGoofys) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	log.Infof("MakeBucket :%+v", *param)
	return &MakeBucketOutput{}, nil
}

func (b *KafkaGoofys) Delegate() interface{} {
	return b
}
