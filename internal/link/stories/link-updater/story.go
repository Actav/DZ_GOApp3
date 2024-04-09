package link_updater

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/link"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/scrape"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/utils"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func New(repository repository, consumer amqpConsumer) *Story {
	return &Story{repository: repository, consumer: consumer}
}

type Story struct {
	repository repository
	consumer   amqpConsumer
}

func (s *Story) Run(ctx context.Context) error {
	msgs, err := s.consumer.Consume(
		link.QueueName, // Имя очереди, из которой мы хотим получать сообщения
		"",             // consumer tag - идентификатор потребителя
		false,          // auto-ack - автоматическое подтверждение получения сообщения
		false,          // exclusive - исключительный доступ к очереди только для этого потребителя
		false,          // no-local - сервер не будет отправлять сообщения соединениям, которые сами их опубликовали
		false,          // no-wait - сервер не будет ожидать ответа от потребителя
		nil,            // arguments - дополнительные параметры
	)
	if err != nil {
		return fmt.Errorf("s.consumer.Consume: %w", err)
	}

	go s.listenAndProcessMessages(ctx, msgs)

	return nil
}

func (s *Story) listenAndProcessMessages(ctx context.Context, msgs <-chan amqp.Delivery) {
	for {
		select {
		case msg := <-msgs:
			if err := s.handleLinkMessage(ctx, msg); err != nil {
				// Здесь добавить сбор ошибок в лог
				// log.Printf("Error update link: %v", err)

				// Отклонение сообщения без повторной обработки
				_ = msg.Nack(false, false)
			} else {
				_ = msg.Ack(false)
			}
		case <-ctx.Done():
			return // Завершаем цикл, если контекст был отменен
		}
	}
}

func (s *Story) handleLinkMessage(ctx context.Context, msg amqp.Delivery) error {
	var m link.Message

	if err := json.Unmarshal(msg.Body, &m); err != nil {
		return fmt.Errorf("error unmarshalling message: %v", err)
	}

	id, err := primitive.ObjectIDFromHex(m.ID)
	if err != nil {
		return fmt.Errorf("error primitive.ObjectIDFromHex ID %s: %v", m.ID, err)
	}

	linkData, err := s.repository.FindByID(ctx, id)
	if err != nil {
		return fmt.Errorf("error s.repository.FindByID %s: %v", m.ID, err)
	}

	lReq := database.UpdateLinkReq{
		ID:     id,
		URL:    linkData.URL,
		Title:  linkData.Title,
		Tags:   linkData.Tags,
		Images: linkData.Images,
		UserID: linkData.UserID,
	}

	linkParsData, err := scrape.Parse(ctx, linkData.URL)
	if err != nil {
		return fmt.Errorf("scrape Parse: %w", err)
	}

	if len(linkParsData.Title) > 0 {
		lReq.Title = linkParsData.Title
	}

	if len(linkParsData.Tags) > 0 {
		lReq.Tags = utils.UniqueTags(linkParsData.Tags, linkData.Tags)
	}

	if _, err := s.repository.Update(ctx, lReq); err != nil {
		return fmt.Errorf("error saving updated link with ID %s to the database: %v", lReq.ID, err)
	}

	return nil
}
