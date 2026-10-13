package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	// Worker ID
	// -----------------------------------

	workerID := fmt.Sprintf(
		"worker-%d",
		os.Getpid(),
	)

	// -----------------------------------
	// Connect to MySQL
	// -----------------------------------

	mysqlDB, err := database.NewMySQL()
	if err != nil {
		log.Fatal("MySQL connection failed:", err)
	}
	defer mysqlDB.Close()

	fmt.Printf(
		"%s connected to MySQL\n",
		workerID,
	)

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

	// -----------------------------------
	// Configure QoS
	// -----------------------------------

	err = rabbitChannel.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		log.Fatal("QoS configuration failed:", err)
	}

	// -----------------------------------
	// Start consuming jobs
	// -----------------------------------

	messages, err := rabbitChannel.Consume(
		"jobs",
		workerID,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Consumer creation failed:", err)
	}

	fmt.Printf(
		"%s started\n",
		workerID,
	)

	// -----------------------------------
	// Graceful shutdown
	// -----------------------------------

	stop := make(chan os.Signal, 1)

	signal.Notify(
		stop,
		os.Interrupt,
		syscall.SIGTERM,
	)

	// -----------------------------------
	// Process jobs
	// -----------------------------------

	for {

		select {

		case message, ok := <-messages:

			if !ok {
				fmt.Println(
					"RabbitMQ channel closed",
				)
				return
			}

			// -----------------------------------
			// Decode job
			// -----------------------------------

			var job Job

			err := json.Unmarshal(
				message.Body,
				&job,
			)

			if err != nil {
				log.Println(
					"Invalid job:",
					err,
				)

				message.Nack(
					false,
					false,
				)

				continue
			}

			attempt := job.RetryCount + 1

			fmt.Printf(
				"%s processing %s (%s), attempt=%d\n",
				workerID,
				job.ID,
				job.Payload,
				attempt,
			)

			startedAt := time.Now()

			// -----------------------------------
			// MySQL: Mark processing
			// -----------------------------------

			err = mysqlDB.MarkProcessing(
				job.ID,
				workerID,
				job.RetryCount,
				startedAt,
			)

			if err != nil {
				log.Println(
					"Failed to update MySQL:",
					err,
				)

				message.Nack(
					false,
					true,
				)

				continue
			}

			// -----------------------------------
			// Redis: Mark processing
			// -----------------------------------

			err = redisClient.HSet(
				ctx,
				"job:"+job.ID,
				map[string]interface{}{
					"status":      "processing",
					"worker":      workerID,
					"retry_count": job.RetryCount,
					"max_retries": job.MaxRetries,
					"started_at":  startedAt.Format(time.RFC3339),
				},
			).Err()

			if err != nil {
				log.Println(
					"Failed to update Redis:",
					err,
				)

				message.Nack(
					false,
					true,
				)

				continue
			}

			// -----------------------------------
			// Simulate actual work
			// -----------------------------------

			fmt.Printf(
				"%s working on %s for %d seconds...\n",
				workerID,
				job.ID,
				job.Duration,
			)

			time.Sleep(
				time.Duration(job.Duration) *
					time.Second,
			)

			// -----------------------------------
			// Simulate failure
			// -----------------------------------

			if job.Payload == "image-fail.jpg" {

				// -----------------------------------
				// Retry available
				// -----------------------------------

				if job.RetryCount < job.MaxRetries {

					job.RetryCount++

					// -----------------------------------
					// MySQL: Mark retrying
					// -----------------------------------

					err = mysqlDB.MarkRetrying(
						job.ID,
						job.RetryCount,
					)

					if err != nil {
						log.Println(
							"Failed to update MySQL for retry:",
							err,
						)

						message.Nack(
							false,
							true,
						)

						continue
					}

					// -----------------------------------
					// Redis: Mark retrying
					// -----------------------------------

					err = redisClient.HSet(
						ctx,
						"job:"+job.ID,
						map[string]interface{}{
							"status":      "retrying",
							"retry_count": job.RetryCount,
						},
					).Err()

					if err != nil {
						log.Println(
							"Failed to update Redis for retry:",
							err,
						)

						message.Nack(
							false,
							true,
						)

						continue
					}

					// -----------------------------------
					// Create retry message
					// -----------------------------------

					body, err := json.Marshal(job)

					if err != nil {
						log.Println(
							"Failed to encode retry job:",
							err,
						)

						message.Nack(
							false,
							false,
						)

						continue
					}

					// -----------------------------------
					// Publish retry
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
						log.Println(
							"Failed to publish retry:",
							err,
						)

						message.Nack(
							false,
							true,
						)

						continue
					}

					// -----------------------------------
					// ACK original
					// -----------------------------------

					err = message.Ack(false)

					if err != nil {
						log.Println(
							"Failed to ACK original message:",
							err,
						)

						continue
					}

					continue
				}

				// -----------------------------------
				// Maximum retries reached
				// -----------------------------------

				failedAt := time.Now()

				// -----------------------------------
				// MySQL: Mark failed
				// -----------------------------------

				err = mysqlDB.MarkFailed(
					job.ID,
					job.RetryCount,
					failedAt,
				)

				if err != nil {
					log.Println(
						"Failed to update MySQL for failed job:",
						err,
					)

					message.Nack(
						false,
						true,
					)

					continue
				}

				// -----------------------------------
				// Redis: Mark failed
				// -----------------------------------

				err = redisClient.HSet(
					ctx,
					"job:"+job.ID,
					map[string]interface{}{
						"status":      "failed",
						"retry_count": job.RetryCount,
						"failed_at":   failedAt.Format(time.RFC3339),
					},
				).Err()

				if err != nil {
					log.Println(
						"Failed to update Redis:",
						err,
					)

					message.Nack(
						false,
						true,
					)

					continue
				}

				// -----------------------------------
				// Dead Letter Queue
				// -----------------------------------

				message.Nack(
					false,
					false,
				)

				continue
			}

			// -----------------------------------
			// Mark completed
			// -----------------------------------

			completedAt := time.Now()

			// -----------------------------------
			// MySQL: Mark completed
			// -----------------------------------

			err = mysqlDB.MarkCompleted(
				job.ID,
				completedAt,
			)

			if err != nil {
				log.Println(
					"Failed to update MySQL:",
					err,
				)

				message.Nack(
					false,
					true,
				)

				continue
			}

			// -----------------------------------
			// Redis: Mark completed
			// -----------------------------------

			err = redisClient.HSet(
				ctx,
				"job:"+job.ID,
				map[string]interface{}{
					"status":       "completed",
					"completed_at": completedAt.Format(time.RFC3339),
				},
			).Err()

			if err != nil {
				log.Println(
					"Failed to update Redis:",
					err,
				)

				message.Nack(
					false,
					true,
				)

				continue
			}

			// -----------------------------------
			// ACK
			// -----------------------------------

			err = message.Ack(false)

			if err != nil {
				log.Println(
					"Failed to ACK message:",
					err,
				)

				continue
			}

			fmt.Printf(
				"%s completed %s\n",
				workerID,
				job.ID,
			)

		case <-stop:

			fmt.Printf(
				"%s shutting down\n",
				workerID,
			)

			return
		}
	}
}
