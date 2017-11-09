package handler

type MessageHandler interface {
	HandleMessage(data []byte) error
}
