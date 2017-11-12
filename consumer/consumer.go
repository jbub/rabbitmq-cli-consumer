package consumer

import (
	"fmt"
	"log"
	"net/url"

	"time"

	"github.com/jbub/rabbitmq-cli-consumer/config"
	"github.com/jbub/rabbitmq-cli-consumer/domain"
	"github.com/streadway/amqp"
)

const (
	EmptyString = "<empty>"
)

func New(cfg *config.Config, jb domain.JobBuilder, httpTimeout time.Duration, debugLogger *log.Logger, errLogger *log.Logger, infLogger *log.Logger) (*Consumer, error) {
	uri := fmt.Sprintf(
		"amqp://%s:%s@%s:%s%s",
		url.QueryEscape(cfg.RabbitMq.Username),
		url.QueryEscape(cfg.RabbitMq.Password),
		cfg.RabbitMq.Host,
		cfg.RabbitMq.Port,
		cfg.RabbitMq.Vhost,
	)

	conn, err := amqp.Dial(uri)
	if nil != err {
		return nil, fmt.Errorf("failed connecting RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if nil != err {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}

	// Attempt to preserve BC here
	if cfg.Prefetch.Count == 0 {
		cfg.Prefetch.Count = 3
	}
	if err := ch.Qos(cfg.Prefetch.Count, 0, cfg.Prefetch.Global); err != nil {
		return nil, fmt.Errorf("failed to set QoS: %v", err)
	}

	if _, err := ch.QueueDeclare(cfg.RabbitMq.Queue, true, false, false, false, sanitizeQueueArgs(cfg)); err != nil {
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	// Check for missing exchange settings to preserve BC
	if "" == cfg.Exchange.Name && "" == cfg.Exchange.Type && !cfg.Exchange.Durable && !cfg.Exchange.Autodelete {
		cfg.Exchange.Type = "direct"
	}

	// Empty Exchange name means default, no need to declare
	if "" != cfg.Exchange.Name {
		if err := ch.ExchangeDeclare(cfg.Exchange.Name, cfg.Exchange.Type, cfg.Exchange.Durable, cfg.Exchange.Autodelete, false, false, amqp.Table{}); err != nil {
			return nil, fmt.Errorf("failed to declare exchange: %v", err)
		}

		// Bind queue
		if err := ch.QueueBind(cfg.RabbitMq.Queue, transformToStringValue(cfg.QueueSettings.Routingkey), transformToStringValue(cfg.Exchange.Name), false, nil); err != nil {
			return nil, fmt.Errorf("failed to bind queue to exchange: %v", err)
		}
	}

	return &Consumer{
		Cfg:         cfg,
		Channel:     ch,
		Connection:  conn,
		Queue:       cfg.RabbitMq.Queue,
		JobBuilder:  jb,
		HttpTimeout: httpTimeout,
		DebugLogger: debugLogger,
		ErrLogger:   errLogger,
		InfLogger:   infLogger,
	}, nil
}

type Consumer struct {
	Cfg         *config.Config
	Channel     *amqp.Channel
	Connection  *amqp.Connection
	Queue       string
	DebugLogger *log.Logger
	ErrLogger   *log.Logger
	InfLogger   *log.Logger
	JobBuilder  domain.JobBuilder
	HttpTimeout time.Duration
}

func ConnectionCloseHandler(closeErr chan *amqp.Error, c *Consumer) {
	err := <-closeErr
	c.ErrLogger.Fatalf("connection closed: %v", err)
}

func (c *Consumer) Consume() {
	msgs, err := c.Channel.Consume(c.Queue, "", true, false, false, false, nil)
	if err != nil {
		c.ErrLogger.Fatalf("failed to register a consumer: %s", err)
	}

	defer c.Connection.Close()
	defer c.Channel.Close()

	closeErr := make(chan *amqp.Error)
	closeErr = c.Connection.NotifyClose(closeErr)

	go ConnectionCloseHandler(closeErr, c)

	c.InfLogger.Printf("using %v workers ...", c.Cfg.Workers.Count)
	c.InfLogger.Printf("using worker queue of length %v ...", c.Cfg.Workers.Queue)
	c.InfLogger.Printf("using http timeout %v ...", c.HttpTimeout)
	c.InfLogger.Printf("waiting for messages ...")

	pool := NewPool(c.Cfg.Workers.Count, c.Cfg.Workers.Queue, c.InfLogger, c.ErrLogger)
	defer pool.Release()

	forever := make(chan bool)

	for d := range msgs {
		if c.DebugLogger != nil {
			c.DebugLogger.Printf("received message: %v", string(d.Body))
		}

		job, err := c.JobBuilder.BuildJob(d.Body)
		if err != nil {
			c.ErrLogger.Printf("could not build job: %v", err)
		}

		pool.AddJob(job)
	}

	pool.WaitAll()
	<-forever
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
