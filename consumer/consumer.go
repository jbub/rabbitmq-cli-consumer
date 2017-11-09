package consumer

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"

	"sync"

	"github.com/jbub/rabbitmq-cli-consumer/config"
	"github.com/jbub/rabbitmq-cli-consumer/handler"
	"github.com/streadway/amqp"
)

const (
	EmptyString = "<empty>"
)

func New(cfg *config.Config, msgHandler handler.MessageHandler, debugLogger *log.Logger, errLogger *log.Logger, infLogger *log.Logger) (*Consumer, error) {
	uri := fmt.Sprintf(
		"amqp://%s:%s@%s:%s%s",
		url.QueryEscape(cfg.RabbitMq.Username),
		url.QueryEscape(cfg.RabbitMq.Password),
		cfg.RabbitMq.Host,
		cfg.RabbitMq.Port,
		cfg.RabbitMq.Vhost,
	)

	infLogger.Println("connecting to RabbitMQ ...")
	conn, err := amqp.Dial(uri)
	if nil != err {
		return nil, errors.New(fmt.Sprintf("failed connecting RabbitMQ: %s", err.Error()))
	}
	infLogger.Println("connected")

	infLogger.Println("opening channel ...")
	ch, err := conn.Channel()
	if nil != err {
		return nil, errors.New(fmt.Sprintf("failed to open a channel: %s", err.Error()))
	}
	infLogger.Println("channel opened")

	infLogger.Println("setting QoS ...")
	// Attempt to preserve BC here
	if cfg.Prefetch.Count == 0 {
		cfg.Prefetch.Count = 3
	}
	if err := ch.Qos(cfg.Prefetch.Count, 0, cfg.Prefetch.Global); err != nil {
		return nil, errors.New(fmt.Sprintf("failed to set QoS: %s", err.Error()))
	}
	infLogger.Println("QoS set")

	infLogger.Printf("declaring queue \"%s\"...", cfg.RabbitMq.Queue)
	_, err = ch.QueueDeclare(cfg.RabbitMq.Queue, true, false, false, false, sanitizeQueueArgs(cfg))

	if err != nil {
		return nil, errors.New(fmt.Sprintf("failed to declare queue: %s", err.Error()))
	}

	// Check for missing exchange settings to preserve BC
	if "" == cfg.Exchange.Name && "" == cfg.Exchange.Type && !cfg.Exchange.Durable && !cfg.Exchange.Autodelete {
		cfg.Exchange.Type = "direct"
	}

	// Empty Exchange name means default, no need to declare
	if "" != cfg.Exchange.Name {
		infLogger.Printf("declaring exchange \"%s\"...", cfg.Exchange.Name)
		err = ch.ExchangeDeclare(cfg.Exchange.Name, cfg.Exchange.Type, cfg.Exchange.Durable, cfg.Exchange.Autodelete, false, false, amqp.Table{})
		if err != nil {
			return nil, errors.New(fmt.Sprintf("failed to declare exchange: %s", err.Error()))
		}

		// Bind queue
		infLogger.Printf("binding queue \"%s\" to exchange \"%s\"...", cfg.RabbitMq.Queue, cfg.Exchange.Name)
		err = ch.QueueBind(cfg.RabbitMq.Queue, transformToStringValue(cfg.QueueSettings.Routingkey), transformToStringValue(cfg.Exchange.Name), false, nil)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("failed to bind queue to exchange: %s", err.Error()))
		}
	}

	return &Consumer{
		Channel:     ch,
		Connection:  conn,
		Queue:       cfg.RabbitMq.Queue,
		MsgHandler:  msgHandler,
		DebugLogger: debugLogger,
		ErrLogger:   errLogger,
		InfLogger:   infLogger,
	}, nil
}

type Consumer struct {
	Channel     *amqp.Channel
	Connection  *amqp.Connection
	Queue       string
	DebugLogger *log.Logger
	ErrLogger   *log.Logger
	InfLogger   *log.Logger
	MsgHandler  handler.MessageHandler
}

func ConnectionCloseHandler(closeErr chan *amqp.Error, c *Consumer) {
	err := <-closeErr
	c.ErrLogger.Fatalf("connection closed: %v", err)
	os.Exit(10)
}

func (c *Consumer) Consume() {
	c.InfLogger.Println("registering consumer... ")
	msgs, err := c.Channel.Consume(c.Queue, "", false, false, false, false, nil)
	if err != nil {
		c.ErrLogger.Fatalf("failed to register a consumer: %s", err)
	}
	c.InfLogger.Println("succeeded registering consumer.")

	defer c.Connection.Close()
	defer c.Channel.Close()

	closeErr := make(chan *amqp.Error)
	closeErr = c.Connection.NotifyClose(closeErr)

	go ConnectionCloseHandler(closeErr, c)

	var wg sync.WaitGroup
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			go func(wg *sync.WaitGroup, d amqp.Delivery) {
				wg.Add(1)
				defer wg.Done()

				c.handleMsg(d)
			}(&wg, d)
		}
	}()

	c.InfLogger.Println("waiting for messages...")
	<-forever
	wg.Wait()
}

func (c *Consumer) handleMsg(d amqp.Delivery) {
	if c.DebugLogger != nil {
		c.DebugLogger.Printf("received message: %v", string(d.Body))
	}

	if err := c.MsgHandler.HandleMessage(d.Body); err != nil {
		c.ErrLogger.Printf("could not handle message: %v", err)
	}

	if err := d.Ack(true); err != nil {
		c.ErrLogger.Printf("could not acknowledge message: %v", err)
	}
}

func sanitizeQueueArgs(cfg *config.Config) amqp.Table {
	args := make(amqp.Table)

	if cfg.QueueSettings.MessageTTL > 0 {
		args["x-message-ttl"] = int32(cfg.QueueSettings.MessageTTL)
	}

	if cfg.QueueSettings.DeadLetterExchange != "" {
		args["x-dead-letter-exchange"] = transformToStringValue(cfg.QueueSettings.DeadLetterExchange)

		if cfg.QueueSettings.DeadLetterRoutingKey != "" {
			args["x-dead-letter-routing-key"] = transformToStringValue(cfg.QueueSettings.DeadLetterRoutingKey)
		}
	}

	if len(args) > 0 {
		return args
	}
	return nil
}

func transformToStringValue(val string) string {
	if val == EmptyString {
		return ""
	}
	return val
}
