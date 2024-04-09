package link

const (
	QueueName = "linksQueue"
)

type Message struct {
	ID string `json:"id"`
}
