package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)


// ================== НАСТРОЙКИ ==================
const (
	BucketName     = "31d5eb06-baket-backup-test"
	FileSizeMB     = 100
	Region         = "ru-1"
	EndpointURL    = "https://s3.twcstorage.ru"
	ParallelFiles  = 100
	TmpDir         = "/mnt/dbaas/tmp"
	LogFileMain    = "s3_put.log"
	LogFileErrors  = "s3_errors.log"
	LogFileRequest = "s3_requests.log"
)

// ================== ГЛОБАЛЬНЫЕ ЛОГИ ==================
var (
	mainLogger   *log.Logger
	errorLogger  *log.Logger
	requestLogger *log.Logger
)

func initLogging() {
	mainLogFile, _ := os.OpenFile(LogFileMain, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	errorLogFile, _ := os.OpenFile(LogFileErrors, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	reqLogFile, _ := os.OpenFile(LogFileRequest, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	mainLogger = log.New(io.MultiWriter(os.Stdout, mainLogFile), "[MAIN] ", log.Ldate|log.Ltime|log.Lmicroseconds)
	errorLogger = log.New(errorLogFile, "[ERROR] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	requestLogger = log.New(reqLogFile, "[REQUEST] ", log.Ldate|log.Ltime|log.Lmicroseconds)
}

// ================== S3 CLIENT ==================
func newS3Client() *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"XA2UUWUEW2WKT3IIDZ1Z",
			"InwYkp2JtmzW96NpMfdDZFqyQGSWfA8JsDDMG8N2",
			"")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           EndpointURL,
					SigningRegion: Region,
				}, nil
			})),
	)
	if err != nil {
		log.Fatalf("Ошибка конфигурации AWS: %v", err)
	}
	return s3.NewFromConfig(cfg)
}

// ================== УТИЛИТЫ ==================
func generateFile(path string, sizeMB int) error {
	if _, err := os.Stat(path); err == nil {
		mainLogger.Printf("Файл %s уже существует, пропускаем", path)
		return nil
	}

	sizeBytes := int64(sizeMB) * 1024 * 1024
	mainLogger.Printf("Создаём файл %s размером %d MB (~%d байт)", path, sizeMB, sizeBytes)

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 1024*1024) // 1 MB
	var written int64
	for written < sizeBytes {
		if _, err := rand.Read(buf); err != nil {
			return err
		}
		n, err := file.Write(buf)
		if err != nil {
			return err
		}
		written += int64(n)
	}
	mainLogger.Printf("Файл создан: %s", path)
	return nil
}

func randomKey(iter, idx int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	sb := strings.Builder{}
	sb.Grow(6)
	for i := 0; i < 6; i++ {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		sb.WriteByte(letters[n.Int64()])
	}
	return fmt.Sprintf("test-file-%d-%d-%s.bin", iter, idx, sb.String())
}

func uploadOneFile(ctx context.Context, client *s3.Client, filePath string, iter, idx int, wg *sync.WaitGroup) {
	defer wg.Done()
	key := randomKey(iter, idx)

	mainLogger.Printf("[TASK %d] Начало загрузки %s", idx, key)
	f, err := os.Open(filePath)
	if err != nil {
		errorLogger.Printf("[%s] Ошибка открытия файла: %v", key, err)
		return
	}
	defer f.Close()

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		errorLogger.Printf("[%s] Ошибка загрузки: %v", key, err)
		return
	}
	mainLogger.Printf("[%s] Файл загружен", key)

	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		errorLogger.Printf("[%s] Ошибка удаления: %v", key, err)
		return
	}
	mainLogger.Printf("[%s] Файл удалён", key)
}

// ================== MAIN ==================
func main() {
	initLogging()
	client := newS3Client()

	testFile := filepath.Join(TmpDir, fmt.Sprintf("testfile_%dMB.bin", FileSizeMB))
	if err := generateFile(testFile, FileSizeMB); err != nil {
		log.Fatalf("Ошибка генерации файла: %v", err)
	}

	iteration := 1
	for {
		mainLogger.Printf("=== Итерация %d: параллельная загрузка %d файлов ===", iteration, ParallelFiles)

		var wg sync.WaitGroup
		ctx := context.Background()
		for i := 1; i <= ParallelFiles; i++ {
			wg.Add(1)
			go uploadOneFile(ctx, client, testFile, iteration, i, &wg)
		}
		wg.Wait()

		mainLogger.Printf("Итерация %d завершена, пауза 5 секунд\n", iteration)
		iteration++
		time.Sleep(5 * time.Second)
	}
}
