package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"
)

// ================== НАСТРОЙКИ ==================
const (
	BUCKET_NAME    = "31d5eb06-baket-backup-test"
	FILE_PATH      = "/mnt/dbaas/tmp/test-bigfile.bin"
	FILE_SIZE_GB   = 1   // размер тестового файла
	PART_SIZE_MB   = 20  // размер части для multipart
	REGION         = "ru-1"
	ENDPOINT_URL   = "https://s3.twcstorage.ru"
	PARALLEL_FILES = 100   // сколько файлов грузим одновременно

	ACCESS_KEY = "XA2UUWUEW2WKT3IIDZ1Z"
	SECRET_KEY = "InwYkp2JtmzW96NpMfdDZFqyQGSWfA8JsDDMG8N2"
)

// ================== ЛОГИ ==================
var (
	mainLog   *log.Logger
	errorLog  *log.Logger
	requestLog *log.Logger
)

func initLogs() {
	mainLog = log.New(os.Stdout, "[MAIN] ", log.LstdFlags|log.Lmicroseconds)
	errorFile, _ := os.OpenFile("s3_errors.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	errorLog = log.New(errorFile, "[ERROR] ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)

	reqFile, _ := os.OpenFile("s3_requests.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	requestLog = log.New(reqFile, "[REQUEST] ", log.LstdFlags|log.Lmicroseconds)
}

// ================== S3 клиент ==================
func newS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(REGION),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ACCESS_KEY, SECRET_KEY, "")),
		config.WithLogger(logging.NewStandardLogger(os.Stdout)),
		config.WithClientLogMode(aws.LogRetries|aws.LogRequest|aws.LogResponse),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ENDPOINT_URL)
	}), nil
}

// ================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==================
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, n)
	for i := range result {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		result[i] = letters[num.Int64()]
	}
	return string(result)
}

func generateFile(path string, sizeGB int) error {
	if _, err := os.Stat(path); err == nil {
		mainLog.Printf("Файл %s уже существует, пропускаем создание", path)
		return nil
	}
	sizeBytes := int64(sizeGB) * 1024 * 1024 * 1024
	mainLog.Printf("Создание файла %s размером %d ГБ", path, sizeGB)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	chunk := make([]byte, 1024*1024) // 1 MB
	_, _ = rand.Read(chunk)

	var written int64
	for written < sizeBytes {
		if _, err := f.Write(chunk); err != nil {
			return err
		}
		written += int64(len(chunk))
		if written%(1024*1024*1024) == 0 {
			mainLog.Printf("Создано %d ГБ", written/(1024*1024*1024))
		}
	}
	mainLog.Println("Файл создан")
	return nil
}

// ================== MULTIPART ==================
func multipartUpload(ctx context.Context, client *s3.Client, filePath, bucket, key string, partSizeMB int) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	mainLog.Printf("[%s] Инициализация multipart upload", key)
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return err
	}
	uploadID := createResp.UploadId

	partSize := int64(partSizeMB) * 1024 * 1024
	stat, _ := f.Stat()
	totalSize := stat.Size()
	totalParts := int(math.Ceil(float64(totalSize) / float64(partSize)))
	mainLog.Printf("[%s] Размер %d байт, частей %d", key, totalSize, totalParts)

	completedParts := make([]types.CompletedPart, 0, totalParts)
	partNumber := 1
	buf := make([]byte, partSize)

	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		mainLog.Printf("[%s] Загрузка части %d/%d (%d байт)", key, partNumber, totalParts, n)
		partResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     &bucket,
			Key:        &key,
			PartNumber: aws.Int32(int32(partNumber)),
			UploadId:   uploadID,
			Body:       strings.NewReader(string(buf[:n])),
		})
		if err != nil {
			client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   &bucket,
				Key:      &key,
				UploadId: uploadID,
			})
			return fmt.Errorf("ошибка загрузки части %d: %w", partNumber, err)
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       partResp.ETag,
			PartNumber: aws.Int32(int32(partNumber)),
		})
		partNumber++
	}

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return err
	}
	mainLog.Printf("[%s] Загрузка завершена", key)
	return nil
}

func uploadOneFile(ctx context.Context, client *s3.Client, iteration int, filePath, bucket string, partSizeMB int, idx int) {
	objectKey := fmt.Sprintf("test-bigfile-%d-%d-%s.bin", iteration, idx, randomString(6))
	mainLog.Printf("[TASK %d] Начало загрузки %s", idx, objectKey)

	err := multipartUpload(ctx, client, filePath, bucket, objectKey, partSizeMB)
	if err != nil {
		errorLog.Printf("[TASK %d] Ошибка загрузки %s: %v", idx, objectKey, err)
		return
	}

	mainLog.Printf("[TASK %d] Удаление объекта %s", idx, objectKey)
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &objectKey,
	})
	if err != nil {
		errorLog.Printf("[TASK %d] Ошибка удаления %s: %v", idx, objectKey, err)
	} else {
		mainLog.Printf("[TASK %d] Объект %s удалён", idx, objectKey)
	}
}

func main() {
	initLogs()
	ctx := context.Background()

	client, err := newS3Client(ctx)
	if err != nil {
		log.Fatalf("Ошибка создания клиента S3: %v", err)
	}

	if err := generateFile(FILE_PATH, FILE_SIZE_GB); err != nil {
		log.Fatalf("Ошибка создания файла: %v", err)
	}

	iteration := 1
	for {
		mainLog.Printf("=== Итерация %d: параллельная загрузка %d файлов ===", iteration, PARALLEL_FILES)
		var wg sync.WaitGroup
		for i := 1; i <= PARALLEL_FILES; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				uploadOneFile(ctx, client, iteration, FILE_PATH, BUCKET_NAME, PART_SIZE_MB, idx)
			}(i)
		}
		wg.Wait()
		mainLog.Printf("Итерация %d завершена, пауза 5 секунд\n", iteration)
		iteration++
		time.Sleep(5 * time.Second)
	}
}
