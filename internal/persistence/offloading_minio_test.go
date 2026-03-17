package persistence

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestOffloading_MinIOBackend_ReadBack(t *testing.T) {
	endpoint := os.Getenv("REDIGO_MINIO_ENDPOINT")
	accessKey := os.Getenv("REDIGO_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("REDIGO_MINIO_SECRET_KEY")
	bucket := os.Getenv("REDIGO_MINIO_BUCKET")

	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("set REDIGO_MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY/BUCKET to run")
	}

	region := os.Getenv("REDIGO_MINIO_REGION")
	if region == "" {
		region = "us-east-1"
	}

	basePrefix := os.Getenv("REDIGO_MINIO_PREFIX")
	if basePrefix != "" && basePrefix[len(basePrefix)-1] != '/' {
		basePrefix += "/"
	}
	prefix := fmt.Sprintf("%sredigo_test_%d/", basePrefix, time.Now().UnixNano())

	tmpDir := t.TempDir()

	options := DefaultOptions()
	options.EnableOffloading = true
	options.OffloadBackend = "minio"
	options.OffloadEndpoint = endpoint
	options.OffloadAccessKey = accessKey
	options.OffloadSecretKey = secretKey
	options.OffloadBucket = bucket
	options.OffloadRegion = region
	options.OffloadPrefix = prefix
	options.OffloadMinLevel = 0
	options.OffloadKeepLocal = true

	engine, err := OpenLSMEnergy(tmpDir, options)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if engine.compactor != nil {
		engine.compactor.Stop()
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k_%d", i)
		val := make([]byte, 256)
		for j := range val {
			val[j] = byte((i + j) % 251)
		}
		if err := engine.Put([]byte(key), val); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := engine.flushMemTableSync(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	version := engine.versionSet.GetCurrentVersion()
	if version == nil || len(version.Files) == 0 || len(version.Files[0]) == 0 {
		t.Fatalf("expected level-0 files")
	}
	if engine.offloader == nil || engine.offloader.store == nil {
		t.Fatalf("expected offloader initialized")
	}

	fileNum := version.Files[0][0].FileNum
	ok, err := engine.offloader.store.StatObject(engine.offloader.keyFor(fileNum))
	if err != nil {
		t.Fatalf("stat object: %v", err)
	}
	if !ok {
		t.Fatalf("expected object exists in minio")
	}

	engine.tableCache.Evict(fileNum)
	_ = os.Remove(engine.offloader.localPath(fileNum))

	_, found := engine.Get([]byte("k_1"))
	if !found {
		t.Fatalf("expected key found after offloading")
	}
}
