package database

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	DB *sql.DB
}

func NewMySQL() (*MySQL, error) {

	// -----------------------------------
	// MySQL connection (via env, no hardcoded credentials)
	// -----------------------------------

	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("MYSQL_DSN env var not set (e.g. user:password@tcp(localhost:3306)/job_processor)")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// -----------------------------------
	// Verify connection
	// -----------------------------------

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return &MySQL{
		DB: db,
	}, nil
}

func (mysql *MySQL) Close() error {
	return mysql.DB.Close()
}

// -----------------------------------
// Create Job
// -----------------------------------

func (mysql *MySQL) CreateJob(
	id string,
	jobType string,
	payload string,
	duration int,
	retryCount int,
	maxRetries int,
	createdAt time.Time,
) error {

	_, err := mysql.DB.Exec(
		`
		INSERT INTO jobs (
			id,
			type,
			payload,
			duration,
			status,
			retry_count,
			max_retries,
			created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`,
		id,
		jobType,
		payload,
		duration,
		"pending",
		retryCount,
		maxRetries,
		createdAt,
	)

	return err
}

// -----------------------------------
// Mark Processing
// -----------------------------------

func (mysql *MySQL) MarkProcessing(
	id string,
	workerID string,
	retryCount int,
	startedAt time.Time,
) error {

	_, err := mysql.DB.Exec(
		`
		UPDATE jobs
		SET
			status = ?,
			worker = ?,
			retry_count = ?,
			started_at = ?
		WHERE id = ?
		`,
		"processing",
		workerID,
		retryCount,
		startedAt,
		id,
	)

	return err
}

// -----------------------------------
// Mark Retrying
// -----------------------------------

func (mysql *MySQL) MarkRetrying(
	id string,
	retryCount int,
) error {

	_, err := mysql.DB.Exec(
		`
		UPDATE jobs
		SET
			status = ?,
			retry_count = ?
		WHERE id = ?
		`,
		"retrying",
		retryCount,
		id,
	)

	return err
}

// -----------------------------------
// Mark Completed
// -----------------------------------

func (mysql *MySQL) MarkCompleted(
	id string,
	completedAt time.Time,
) error {

	_, err := mysql.DB.Exec(
		`
		UPDATE jobs
		SET
			status = ?,
			completed_at = ?
		WHERE id = ?
		`,
		"completed",
		completedAt,
		id,
	)

	return err
}

// -----------------------------------
// Mark Failed
// -----------------------------------

func (mysql *MySQL) MarkFailed(
	id string,
	retryCount int,
	failedAt time.Time,
) error {

	_, err := mysql.DB.Exec(
		`
		UPDATE jobs
		SET
			status = ?,
			retry_count = ?,
			failed_at = ?
		WHERE id = ?
		`,
		"failed",
		retryCount,
		failedAt,
		id,
	)

	return err
}

// -----------------------------------
// Reset Job For Replay
// -----------------------------------

func (mysql *MySQL) ResetForReplay(
	id string,
) error {

	_, err := mysql.DB.Exec(
		`
		UPDATE jobs
		SET
			status = ?,
			retry_count = ?,
			worker = NULL,
			started_at = NULL,
			completed_at = NULL,
			failed_at = NULL
		WHERE id = ?
		`,
		"pending",
		0,
		id,
	)

	return err
}
