package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"job-processor/database"

	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

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
	// Connect to MySQL
	// -----------------------------------

	mysqlDB, err := database.NewMySQL()
	if err != nil {
		log.Fatal("MySQL connection failed:", err)
	}
	defer mysqlDB.Close()

	fmt.Println("Connected to MySQL")

	// -----------------------------------
	// Connect to RabbitMQ
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

	// -----------------------------------
	// Declare Dead Letter Exchange
	// -----------------------------------

	err = rabbitChannel.ExchangeDeclare(
		"jobs.dlx",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("DLX declaration failed:", err)
	}

	// -----------------------------------
	// Declare Dead Letter Queue
	// -----------------------------------

	_, err = rabbitChannel.QueueDeclare(
		"failed_jobs",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("DLQ declaration failed:", err)
	}

	// -----------------------------------
	// Bind DLQ
	// -----------------------------------

	err = rabbitChannel.QueueBind(
		"failed_jobs",
		"failed",
		"jobs.dlx",
		false,
		nil,
	)
	if err != nil {
		log.Fatal("DLQ binding failed:", err)
	}

	// -----------------------------------
	// Declare main jobs queue
	// -----------------------------------

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
	// Connect to Redis
	// -----------------------------------

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	fmt.Println("Producer started")

	// -----------------------------------
	// Create jobs
	// -----------------------------------

	for i := 1; i <= 10; i++ {

		jobID := fmt.Sprintf(
			"job-%d-%d",
			time.Now().UnixNano(),
			i,
		)

		payload := fmt.Sprintf(
			"image-%d.jpg",
			i,
		)

		// Deliberately create one failing job.
		if i == 3 {
			payload = "image-fail.jpg"
		}

		job := Job{
			ID:         jobID,
			Type:       "image_processing",
			Payload:    payload,
			Duration:   (i % 5) + 1,
			RetryCount: 0,
			MaxRetries: 3,
		}

		createdAt := time.Now()

		// -----------------------------------
		// Save job in MySQL
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
			log.Fatal(
				"Failed to create job in MySQL:",
				err,
			)
		}

		// -----------------------------------
		// Save initial state in Redis
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
			log.Fatal(
				"Failed to save job in Redis:",
				err,
			)
		}

		// -----------------------------------
		// Convert job to JSON
		// -----------------------------------

		body, err := json.Marshal(job)
		if err != nil {
			log.Fatal(
				"Failed to encode job:",
				err,
			)
		}

		// -----------------------------------
		// Publish to RabbitMQ
		// -----------------------------------

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
			log.Fatal(
				"Failed to publish job:",
				err,
			)
		}

		fmt.Printf(
			"Created and queued: %s\n",
			job.ID,
		)
	}

	fmt.Println("Finished publishing jobs")
}
