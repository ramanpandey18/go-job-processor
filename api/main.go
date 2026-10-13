package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"job-processor/database"

	"github.com/gin-gonic/gin"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type CreateJobRequest struct {
	Type           string `json:"type" binding:"required"`
	Payload        string `json:"payload" binding:"required"`
	Duration       int    `json:"duration" binding:"required"`
	MaxRetries     int    `json:"max_retries"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

type Job struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	Payload    string `json:"payload"`
	Duration   int    `json:"duration"`
	RetryCount int    `json:"retry_count"`
	MaxRetries int    `json:"max_retries"`
}

func main() {

	ctx := context.Background()

	// -----------------------------------
	// Config from env (no hardcoded credentials)
	// -----------------------------------

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL env var not set (e.g. amqp://user:password@localhost:5672/)")
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// -----------------------------------
	// MySQL
	// -----------------------------------

	mysqlDB, err := database.NewMySQL()
	if err != nil {
		log.Fatal("MySQL connection failed:", err)
	}
	defer mysqlDB.Close()

	// -----------------------------------
	// Redis
	// -----------------------------------

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	// -----------------------------------
	// RabbitMQ
	// -----------------------------------

	rabbitConn, err := amqp091.Dial(
		rabbitURL,
	)
	if err != nil {
		log.Fatal("RabbitMQ connection failed:", err)
	}
	defer rabbitConn.Close()

	rabbitChannel, err := rabbitConn.Channel()
	if err != nil {
		log.Fatal("RabbitMQ channel failed:", err)
	}
	defer rabbitChannel.Close()

	// Declare jobs queue.
	_, err = rabbitChannel.QueueDeclare(
		"jobs",
		true,
		false,
		false,
		false,
		amqp091.Table{
			"x-dead-letter-exchange":    "jobs.dlx",
			"x-dead-letter-routing-key": "failed",
		},
	)

	if err != nil {
		log.Fatal("Jobs queue declaration failed:", err)
	}

	// -----------------------------------
	// Gin
	// -----------------------------------

	router := gin.Default()

	router.POST("/jobs", func(c *gin.Context) {

		var request CreateJobRequest

		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}

		// Default retry count.
		if request.MaxRetries == 0 {
			request.MaxRetries = 3
		}

		// -----------------------------------
		// Idempotency
		// -----------------------------------

		idempotencyRedisKey :=
			"idempotency:" + request.IdempotencyKey

		jobID := fmt.Sprintf(
			"job-%d",
			time.Now().UnixNano(),
		)

		// SETNX means:
		//
		// Set this key only if it does NOT already exist.
		//
		// true  -> first request
		// false -> duplicate request
		created, err := redisClient.SetNX(
			ctx,
			idempotencyRedisKey,
			jobID,
			24*time.Hour,
		).Result()

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "idempotency check failed",
			})
			return
		}

		if !created {

			existingJobID, err := redisClient.Get(
				ctx,
				idempotencyRedisKey,
			).Result()

			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "failed to get existing job",
				})
				return
			}

			c.JSON(http.StatusOK, gin.H{
				"id":     existingJobID,
				"status": "already_exists",
			})

			return
		}

		// -----------------------------------
		// Create Job
		// -----------------------------------

		job := Job{
			ID:         jobID,
			Type:       request.Type,
			Payload:    request.Payload,
			Duration:   request.Duration,
			RetryCount: 0,
			MaxRetries: request.MaxRetries,
		}

		createdAt := time.Now()

		// -----------------------------------
		// Save in MySQL
		// -----------------------------------

		err = mysqlDB.CreateJob(
			job.ID,
			job.Type,
			job.Payload,
			job.Duration,
			job.RetryCount,
			job.MaxRetries,
			createdAt,
		)

		if err != nil {

			// DB failed, so remove idempotency key.
			redisClient.Del(
				ctx,
				idempotencyRedisKey,
			)

			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to create job",
			})

			return
		}

		// -----------------------------------
		// Save in Redis
		// -----------------------------------

		err = redisClient.HSet(
			ctx,
			"job:"+job.ID,
			map[string]interface{}{
				"id":          job.ID,
				"status":      "pending",
				"type":        job.Type,
				"payload":     job.Payload,
				"retry_count": job.RetryCount,
				"max_retries": job.MaxRetries,
				"created_at":  createdAt.Format(time.RFC3339),
			},
		).Err()

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to save job in Redis",
			})
			return
		}

		// -----------------------------------
		// Publish to RabbitMQ
		// -----------------------------------

		body, err := json.Marshal(job)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to encode job",
			})
			return
		}

		err = rabbitChannel.Publish(
			"",
			"jobs",
			false,
			false,
			amqp091.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp091.Persistent,
				Body:         body,
			},
		)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to publish job",
			})
			return
		}

		// -----------------------------------
		// Response
		// -----------------------------------

		c.JSON(http.StatusAccepted, gin.H{
			"id":     job.ID,
			"status": "pending",
		})
	})

	router.GET("/health", func(c *gin.Context) {

		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	fmt.Println("Gin API started on :8080")

	router.Run(":8080")
}
