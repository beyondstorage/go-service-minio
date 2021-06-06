package minio

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/credential"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
	typ "github.com/beyondstorage/go-storage/v4/types"
)

// enum object type
const (
	objectTypeFile = iota
	objectTypeDir
)

// Storage is the example client.
type Storage struct {
	client     *minio.Client
	workDir    string
	bucketName string

	defaultPairs DefaultStoragePairs
	features     StorageFeatures

	types.UnimplementedStorager
}

// String implements Storager.String
func (s *Storage) String() string {
	return fmt.Sprintf(
		"Storager minio {WorkDir: %s}",
		s.workDir,
	)
}

// NewStorager will create Storager only.
func NewStorager(pairs ...types.Pair) (types.Storager, error) {
	return newStorager(pairs...)
}

// newStorager will create a new minio storager client.
func newStorager(pairs ...typ.Pair) (store *Storage, err error) {
	defer func() {
		if err != nil {
			err = services.InitError{Op: "new_storager", Type: Type, Err: formatError(err), Pairs: pairs}
		}
	}()

	opt, err := parsePairStorageNew(pairs)
	if err != nil {
		return
	}

	credentialInfo, err := credential.Parse(opt.Credential)
	if err != nil {
		return nil, err
	}

	if credentialInfo.Protocol() != credential.ProtocolHmac {
		return nil, services.PairUnsupportedError{Pair: ps.WithCredential(opt.Credential)}
	}

	accessKey, secretAccessKey := credentialInfo.Hmac()

	region := ""
	if opt.HasRegion {
		region = opt.Region
	}

	useSSL := opt.UseSsl
	opintions := minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretAccessKey, ""),
		Secure: useSSL,
		Region: region,
	}

	endpoint := opt.Endpoint
	// create new minio client
	client, err := minio.New(endpoint, &opintions)

	workDir := "/"
	if opt.HasWorkDir {
		workDir = opt.WorkDir
	}

	store = &Storage{
		client:     client,
		bucketName: opt.Name,
		workDir:    workDir,
	}

	return
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	if err == nil {
		return nil
	}

	return services.StorageError{
		Op:       op,
		Err:      formatError(err),
		Storager: s,
		Path:     path,
	}
}

// formatError format error into go-storage error
func formatError(err error) error {
	if _, ok := err.(services.InternalError); ok {
		return err
	}

	// TODO: format more minio error

	if err.Error() == "The specified key does not exist." {
		err = fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
	}

	return err
}

// newObject new a object
func (s *Storage) newObject(done bool) *typ.Object {
	return typ.NewObject(s, done)
}

// getAbsPath return absolute path
func (s *Storage) getAbsPath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}

	// join path
	absPath := filepath.Join(s.workDir, path)

	// append again
	if strings.HasSuffix(path, "/") {
		absPath += "/"
	}
	return absPath
}

func (s *Storage) formatObject(v *minio.ObjectInfo, uniquePath, dir string) *typ.Object {
	switch s.objectType(v) {
	case objectTypeFile:
		return s.formatFileObject(v, uniquePath, dir)
	case objectTypeDir:
		return s.formatFolderObject(v, uniquePath, dir)
	}

	return nil
}

// formatFolderObject format a minio folder object into go-storage object
func (s *Storage) formatFolderObject(v *minio.ObjectInfo, uniquePath, dir string) (o *typ.Object) {
	o = s.newObject(true)

	folderName := path.Base(v.Key)
	o.ID = filepath.Join(dir, folderName)
	o.Path = path.Join(dir, folderName)
	o.Mode |= typ.ModeDir

	return o
}

// formatFolderObject format a minio file object into go-storage object
func (s *Storage) formatFileObject(v *minio.ObjectInfo, uniquePath, dir string) (o *typ.Object) {
	o = s.newObject(true)

	fileName := path.Base(v.Key)
	o.ID = filepath.Join(dir, fileName)
	o.Path = path.Join(dir, fileName)
	o.Mode |= typ.ModeRead

	o.SetEtag(v.ETag)
	o.SetLastModified(v.LastModified)
	o.SetContentLength(v.Size)

	return o
}

// formatObjectKey format object key into a normal object Key eg: /a/b/c/ -> a/b/c, a/b -> a/b
func (s *Storage) formatObjectKey(str string) string {
	if strings.HasPrefix(str, "/") {
		str = str[1:]
	}
	if strings.HasSuffix(str, "/") {
		str = str[:len(str)-1]
	}

	return str
}

// compareObjectKey compares two object key
func (s *Storage) compareObjectKey(x, y string) bool {
	return s.formatObjectKey(x) == s.formatObjectKey(y)
}

// objectType return type of minio.ObjectInfo
// there are two way to judge the type of object:
// 1. using ETag -> non-empty string is file
// 2. using suffix -> has suffix '/' id dir
func (s *Storage) objectType(v *minio.ObjectInfo) int {
	if strings.HasSuffix(v.Key, "/") {
		return objectTypeDir
	}

	return objectTypeFile
}
