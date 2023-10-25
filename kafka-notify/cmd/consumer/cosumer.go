package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"kafka-notify/pkg/models"
	"log"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	ConsumerGroup      = "notifications-group"
	ConsumerTopic      = "notifications"
	ConsumerPort       = ":8081"
	KafkaServerAddress = "localhost:9092"
)

// helper functions
var ErrNoMessageFound = errors.New("no message found")

func getUserIdFromRequest(ctx *gin.Context) (string, error) {
	userID := ctx.Param("userID")
	if userID == "" {
		return "", errors.New("no user id found in request")
	}

	return userID, nil
}

// notification storage
type UserNotifications map[string][]models.Notification

type NotificationStore struct {
	data UserNotifications
	mu   sync.RWMutex
}

func (ns *NotificationStore) Add(userID string,
	notification models.Notification) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.data[userID] = append(ns.data[userID], notification)
}

func (ns *NotificationStore) Get(userID string) ([]models.Notification, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	notifications, ok := ns.data[userID]
	if !ok {
		return nil, ErrNoMessageFound
	}

	return notifications, nil
}

// kafka related functions
type Consumer struct {
	store *NotificationStore
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (*Consumer) ConsumeClaim(sarama.ConsumerGroupSession,
	sarama.ConsumerGroupClaim) error {
	return nil
}

func (consumer *Consumer) ConsumerClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		userID := string(msg.Key)
		var notification models.Notification
		err := json.Unmarshal(msg.Value, &notification)
		if err != nil {
			log.Printf("failed to unmarshal notification: %v", err)
			continue
		}
		consumer.store.Add(userID, notification)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumerGroup([]string{KafkaServerAddress},
		ConsumerGroup, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func setupConsumerGroup(ctx context.Context, store *NotificationStore) {
	consumerGroup, err := initializeConsumerGroup()
	if err != nil {
		log.Fatalf("failed to initialize consumer group: %v", err)
	}
	defer consumerGroup.Close()

	consumer := &Consumer{store: store}

	for {
		err = consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
		if err != nil {
			log.Printf("failed to consume: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func handleNotifications(ctx *gin.Context, store *NotificationStore) {
	userID, err := getUserIdFromRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	notes, _ := store.Get(userID)
	if len(notes) == 0 {
		ctx.JSON(http.StatusNotFound, gin.H{
			"error":         "no notifications found for user",
			"notifications": []models.Notification{},
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"notifications": notes,
	})
}

func main() {
	store := &NotificationStore{
		data: make(UserNotifications),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go setupConsumerGroup(ctx, store)
	defer cancel()

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/notifications/:userID", func(ctx *gin.Context) {
		handleNotifications(ctx, store)
	})

	fmt.Printf("Kafka CONSUMER (Group: %s) 👥📥 "+
		"started at http://localhost%s\n", ConsumerGroup, ConsumerPort)

	if err := router.Run(ConsumerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
