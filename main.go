package main

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/codegangsta/cli"

	"github.com/jbub/rabbitmq-cli-consumer/config"
	"github.com/jbub/rabbitmq-cli-consumer/consumer"
	"github.com/jbub/rabbitmq-cli-consumer/handler"
)

func main() {
	app := cli.NewApp()
	app.Name = "rabbitmq-cli-consumer"
	app.Usage = "Consume RabbitMQ easily to any cli program"
	app.Author = "Richard van den Brand"
	app.Email = "richard@vandenbrand.org"
	app.Version = "1.4.2"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "configuration, c",
			Usage: "Location of configuration file",
		},
		cli.BoolFlag{
			Name:  "verbose, V",
			Usage: "Enable verbose mode (logs to stdout and stderr)",
		},
		cli.StringFlag{
			Name:  "queue-name, q",
			Usage: "Optional queue name to which can be passed in, without needing to define it in config, if set will override config queue name",
		},
		cli.DurationFlag{
			Name:  "http-timeout, t",
			Value: time.Second * 30,
		},
	}
	app.Action = func(c *cli.Context) {
		if c.String("configuration") == "" {
			cli.ShowAppHelp(c)
			os.Exit(1)
		}

		verbose := c.Bool("verbose")
		debugLogger := getDebugLogger(verbose)

		cfg, err := config.LoadAndParse(c.String("configuration"))
		if err != nil {
			log.Fatalf("failed parsing configuration: %s\n", err)
		}

		errLogger, err := createLogger(cfg.Logs.Error, verbose, os.Stderr)
		if err != nil {
			log.Fatalf("failed creating error log: %s", err)
		}

		infLogger, err := createLogger(cfg.Logs.Info, verbose, os.Stdout)
		if err != nil {
			log.Fatalf("failed creating info log: %s", err)
		}

		if c.String("queue-name") != "" {
			cfg.RabbitMq.Queue = c.String("queue-name")
		}

		httpTimeout := c.Duration("http-timeout")
		msgHandler := handler.NewHTTPMessagerHandler(httpTimeout, infLogger)
		cons, err := consumer.New(cfg, msgHandler, debugLogger, errLogger, infLogger)
		if err != nil {
			errLogger.Fatalf("failed creating consumer: %s", err)
		}

		cons.Consume()
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func createLogger(filename string, verbose bool, out io.Writer) (*log.Logger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}

	var writers = []io.Writer{
		file,
	}
	if verbose {
		writers = append(writers, out)
	}
	return log.New(io.MultiWriter(writers...), "", log.Ldate|log.Ltime), nil
}

func getDebugLogger(verbose bool) *log.Logger {
	if verbose {
		return log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}
	return nil
}
