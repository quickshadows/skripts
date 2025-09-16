// s3_multipart.go
package main

import (
	"context"
	"crypto/rand"
	"errors"
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
	"github.com/aws/smithy-go"
)

// ================= НАСТРОЙКИ =================
var (
	BucketName    = "31d5eb06-baket-backup-test"
	FilePath      = "/mnt/dbaas/tmp/test-bigfile.bin"
	FileSizeGB    = 1   // размер тестового файла
	PartSizeMB    = 20  // размер части
	ParallelFiles = 100 // число параллельных файлов
	Region        = "ru-1"
	EndpointURL   = "https://s3.twcstorage.ru"

	AccessKey = "XA2UUWUEW2WKT3IIDZ1Z"
	SecretKey = "InwYkp2JtmzW96NpMfdDZFqyQGSWfA8JsDDMG8N2"
)

// ================= ЛОГИ =================
var (
	mainLog    *log.Logger
	errorLog   *log.Logger
	requestLog *log.Logger
)

func initLogging() {
	mainFile, _ := os.OpenFile("s3_multipart.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	errFile, _ := os.OpenFile("s3_errors.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	reqFile, _ := os.OpenFile("s3_requests.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	mainLog = log.New(io.MultiWriter(os.Stdout, mainFile), "[MAIN] ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	errorLog = log.New(errFile, "[ERROR] ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	requestLog = log.New(reqFile, "[REQ] ", log.LstdFlags|log.Lmicroseconds)
}

// ================= ВСПОМОГАТЕЛЬНЫЕ =================
func generateFile(path string, sizeGB int) error {
	if _, err := os.Stat(path); err == nil {
		mainLog.Printf("Файл %s уже существует", path)
		return nil
	}

	sizeBytes := int64(sizeGB) * 1024 * 1024 * 1024
	mainLog.Printf("Создание файла %s размером %d GB (%d байт)", path, sizeGB, sizeBytes)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	chunk := make([]byte, 1024*1024) // 1 MB
	if _, err := rand.Read(chunk); err != nil {
		return err
	}

	var written int64
	for written < sizeBytes {
		if (sizeBytes - written) < int64(len(chunk)) {
			_, err = f.Write(chunk[:sizeBytes-written])
		} else {
			_, err = f.Write(chunk)
		}
		if err != nil {
			return err
		}
		written += int64(len(chunk))
		if written%(1024*1024*1024) == 0 {
			mainLog.Printf("Создано %d GB", written/(1024*1024*1024))
		}
	}
	mainLog.Printf("Файл создан")
	return nil
}

func randSuffix(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	var sb strings.Builder
	for i := 0; i < n; i++ {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		sb.WriteByte(letters[num.Int64()])
	}
	return sb.String()
}

// ================= ЛОГИРОВАНИЕ ОШИБОК =================
func explainS3Error(code string) string {
	switch code {
	case "NoSuchBucket":
		return "Указанный bucket не существует"
	case "NoSuchKey":
		return "Указанный объект не найден"
	case "AccessDenied":
		return "Нет прав доступа (ACL/Policy)"
	case "InvalidAccessKeyId":
		return "Неверный AWS Access Key ID"
	case "SignatureDoesNotMatch":
		return "Ошибка подписи (ключ/регион)"
	case "RequestTimeTooSkewed":
		return "Время клиента отличается от сервера"
	case "SlowDown":
		return "S3 просит снизить скорость (throttling)"
	case "InternalError":
		return "Внутренняя ошибка S3"
	case "ServiceUnavailable":
		return "Сервис недоступен"
	case "ExpiredToken":
		return "Срок действия токена истёк"
	case "InvalidBucketName":
		return "Некорректное имя bucket"
	case "EntityTooSmall":
		return "Часть слишком маленькая (<5MB для multipart)"
	case "EntityTooLarge":
		return "Объект слишком большой"
	case "InvalidPart":
		return "Ошибка при сборке multipart"
	case "InvalidPartOrder":
		return "Части загружены в неправильном порядке"
	case "BucketAlreadyExists":
		return "Bucket уже существует (глобально)"
	case "BucketAlreadyOwnedByYou":
		return "Bucket уже принадлежит вам"
	default:
		return ""
	}
}

func logS3Error(key string, err error) {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code := ae.ErrorCode()
		msg := ae.ErrorMessage()
		errorLog.Printf("[%s] S3 API Error: %s - %s", key, code, msg)
		if expl := explainS3Error(code); expl != "" {
			errorLog.Printf("[%s] Пояснение: %s", key, expl)
		}
	} else {
		errorLog.Printf("[%s] Unexpected error: %T - %v", key, err, err)
	}
}

// ================= MULTIPART =================
func multipartUpload(ctx context.Context, client *s3.Client, filePath, bucket, key string, partSizeMB int) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	createOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		logS3Error(key, err)
		return err
	}
	uploadID := *createOut.UploadId
	mainLog.Printf("[%s] UploadId=%s", key, uploadID)

	partSize := int64(partSizeMB * 1024 * 1024)
	info, _ := f.Stat()
	totalSize := info.Size()
	totalParts := int(math.Ceil(float64(totalSize) / float64(partSize)))
	mainLog.Printf("[%s] Размер %d байт, частей %d", key, totalSize, totalParts)

	var completed []types.CompletedPart
	partNumber := 1
	buf := make([]byte, partSize)

	for {
		n, err := f.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			logS3Error(key, err)
			return err
		}
		mainLog.Printf("[%s] Загрузка части %d/%d, %d байт", key, partNumber, totalParts, n)
		partOut, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(partNumber),
			UploadId:   aws.String(uploadID),
			Body:       strings.NewReader(string(buf[:n])),
		})
		if err != nil {
			logS3Error(key, err)
			client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: aws.String(uploadID),
			})
			return err
		}
		mainLog.Printf("[%s] Часть %d загружена, ETag=%s", key, partNumber, *partOut.ETag)
		completed = append(completed, types.CompletedPart{
			ETag:       partOut.ETag,
			PartNumber: aws.Int32(partNumber),
		})
		partNumber++
	}

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		logS3Error(key, err)
		return err
	}

	mainLog.Printf("[%s] Загрузка завершена", key)
	return nil
}

func uploadOneFile(ctx context.Context, client *s3.Client, iteration int, filePath, bucket string, partSizeMB, idx int) error {
	objectKey := fmt.Sprintf("test-bigfile-%d-%d-%s.bin", iteration, idx, randSuffix(6))
	mainLog.Printf("[TASK %d] Начало загрузки %s", idx, objectKey)

	if err := multipartUpload(ctx, client, filePath, bucket, objectKey, partSizeMB); err != nil {
		logS3Error(objectKey, err)
		return err
	}

	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		logS3Error(objectKey, err)
		return err
	}
	mainLog.Printf("[TASK %d] Объект %s удалён", idx, objectKey)
	return nil
}

// ================= MAIN =================
func main() {
	initLogging()

	if err := generateFile(FilePath, FileSizeGB); err != nil {
		errorLog.Fatalf("Ошибка создания файла: %v", err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(AccessKey, SecretKey, "")),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: EndpointURL, SigningRegion: Region}, nil
			})),
		config.WithClientLogMode(aws.LogRequestWithBody|aws.LogResponseWithBody),
	)
	if err != nil {
		errorLog.Fatalf("Ошибка загрузки конфигурации AWS: %v", err)
	}

	// Логирование SDK → файл
	cfg.Logger = aws.LoggerFunc(func(classification aws.LogClassification, msg string) {
		requestLog.Printf("[%s] %s", classification, msg)
	})

	client := s3.NewFromConfig(cfg)
	iteration := 1

	for {
		mainLog.Printf("=== Итерация %d: параллельная загрузка %d файлов ===", iteration, ParallelFiles)
		var wg sync.WaitGroup
		for i := 1; i <= ParallelFiles; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if err := uploadOneFile(context.TODO(), client, iteration, FilePath, BucketName, PartSizeMB, idx); err != nil {
					errorLog.Printf("[TASK %d] Ошибка: %v", idx, err)
				}
			}(i)
		}
		wg.Wait()
		mainLog.Printf("Итерация %d завершена, пауза 5 секунд", iteration)
		iteration++
		time.Sleep(5 * time.Second)
	}
}


