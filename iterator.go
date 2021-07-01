package minio


type storagePageStatus struct {}

func (i *storagePageStatus) ContinuationToken() string {
	return ""
}
