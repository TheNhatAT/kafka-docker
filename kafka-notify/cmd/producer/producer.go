package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"kafka-notify/pkg/models"
	"log"
	"net/http"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	ProducerPort       = ":8082"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

// helper functions
var ErrUserNotFoundInProducer = errors.New("user not found in producer")

func findUserById(id int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.ID == id {
			return user, nil
		}
	}
	return models.User{}, ErrUserNotFoundInProducer
}

func getIDFromRequest(formValue string, ctx *gin.Context) (int, error) {
	id, err := strconv.Atoi(ctx.PostForm(formValue))
	if err != nil {
		return 0, fmt.Errorf(
			"failed to parse ID from form value %s: %w", formValue, err)
	}
	return id, nil
}

// kafka related functions
func sendKafkaMessage(producer sarama.SyncProducer, users []models.User, ctx *gin.Context, fromId, toId int) error {
	fromUser, err := findUserById(fromId, users)
	if err != nil {
		return fmt.Errorf("failed to find user by ID %d: %w", fromId, err)
	}

	toUser, err := findUserById(toId, users)
	if err != nil {
		return fmt.Errorf("failed to find user by ID %d: %w", toId, err)
	}

	notification := models.Notification{
		From:    fromUser,
		To:      toUser,
		Message: ctx.PostForm("message"),
	}

	notificationJson, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Value: sarama.StringEncoder(notificationJson),
		Key:   sarama.StringEncoder(strconv.Itoa(toId)),
	}
	fmt.Println("should be sending message: ", msg)

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	} else {
		log.Printf("message sent: partition=%d, offset=%d, message=%s", partition, offset, notificationJson)
	}
	return err
}

func sendMessageHandler(producer sarama.SyncProducer, users []models.User) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		fromID, err := getIDFromRequest("fromID", ctx)
		if err != nil {
			ctx.JSON((http.StatusBadRequest), gin.H{"error": err.Error()})
			return
		}

		toID, err := getIDFromRequest("toID", ctx)
		if err != nil {
			ctx.JSON((http.StatusBadRequest), gin.H{"error": err.Error()})
			return
		}

		err = sendKafkaMessage(producer, users, ctx, fromID, toID)
		if errors.Is(err, ErrUserNotFoundInProducer) {
			ctx.JSON((http.StatusBadRequest), gin.H{"error": err.Error()})
			return
		}

		if err != nil {
			ctx.JSON((http.StatusInternalServerError), gin.H{"error": err.Error()})
			return
		}

		ctx.JSON(http.StatusOK, gin.H{"message": "message sent"})
	}
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return producer, nil
}

func main() {
	users := []models.User{
		{ID: 1, Name: "John"},
		{ID: 2, Name: "Jane"},
		{ID: 3, Name: "Bob"},
		{ID: 4, Name: "Alice"},
	}

	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to setup producer: %v", err)
	}
	defer producer.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.POST("/send", sendMessageHandler(producer, users))

	fmt.Printf("Kafka PRODUCER 📨 started at http://localhost%s\n",
		ProducerPort)

	if err := router.Run(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
