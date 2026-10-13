package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

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
	// Consume DLQ
	// -----------------------------------

	messages, err := rabbitChannel.Consume(
		"failed_jobs",
		"dlq-replayer",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("DLQ consumer creation failed:", err)
	}

	fmt.Println("DLQ replayer started")

	// -----------------------------------
	// Read ONE message from DLQ
	// -----------------------------------

	message, ok := <-messages

	if !ok {
		log.Println("DLQ channel closed")
		return
	}

	// -----------------------------------
	// Decode job
	// -----------------------------------

	var job Job

	err = json.Unmarshal(message.Body, &job)
	if err != nil {
		log.Println("Invalid DLQ message:", err)

		// This message cannot be replayed.
		// Do not put it back into the DLQ forever.
		message.Nack(false, false)

		return
	}

	fmt.Printf(
		"Replaying job %s, payload=%s, old_retry_count=%d\n",
		job.ID,
		job.Payload,
		job.RetryCount,
	)

	// -----------------------------------
	// Reset retry count
	// -----------------------------------

	job.RetryCount = 0

	// -----------------------------------
	// Update Redis
	// -----------------------------------

	err = redisClient.HSet(
		ctx,
		"job:"+job.ID,
		map[string]interface{}{
			"status":      "pending",
			"retry_count": job.RetryCount,
		},
	).Err()

	if err != nil {
		log.Println("Failed to update Redis:", err)

		// Keep the DLQ message available.
		message.Nack(false, true)

		return
	}

	// -----------------------------------
	// Convert job to JSON
	// -----------------------------------

	body, err := json.Marshal(job)
	if err != nil {
		log.Println("Failed to encode replay job:", err)

		// Keep the DLQ message available.
		message.Nack(false, true)

		return
	}

	// -----------------------------------
	// Publish back to jobs queue
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
		log.Println("Failed to publish replay job:", err)

		// Keep the DLQ message available.
		message.Nack(false, true)

		return
	}

	// -----------------------------------
	// ACK DLQ message
	// -----------------------------------

	err = message.Ack(false)

	if err != nil {
		log.Println("Failed to ACK DLQ message:", err)
		return
	}

	fmt.Printf(
		"Replayed job %s successfully\n",
		job.ID,
	)

	// -----------------------------------
	// Exit
	// -----------------------------------

	fmt.Println("DLQ replayer finished")
}
