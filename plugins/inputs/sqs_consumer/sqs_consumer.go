package sqs_consumer

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/influxdata/telegraf"
	internalaws "github.com/influxdata/telegraf/internal/config/aws"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
)

type empty struct{}

type (
	SqsConsumer struct {
		Region                 string `toml:"region"`
		AccessKey              string `toml:"access_key"`
		SecretKey              string `toml:"secret_key"`
		RoleARN                string `toml:"role_arn"`
		Profile                string `toml:"profile"`
		Filename               string `toml:"shared_credential_file"`
		Token                  string `toml:"token"`
		QueueUrl               string `toml:"queue_url"`
		MaxUndeliveredMessages int    `toml:"max_undelivered_messages"`

		Log telegraf.Logger

		sqsConn *sqs.SQS

		parser  parsers.Parser
		cancel  context.CancelFunc
		ctx     context.Context

		records map[telegraf.TrackingID]*string
		recMux  sync.Mutex
		wg      sync.WaitGroup
	}
)

const (
	defaultMaxUndeliveredMessages       = 1000
	sqsMaxMsgs                    int64 = 10 // max per API
	sqsPollWaitSecs               int64 = 20 // max per API
)

var sampleConfig = `
  ## Amazon REGION of sqs queue
  region = "us-west-2"

  ## queueurl provided when creating the queue
  ## must exist prior to starting telegraf.
  queue_url = "https://sqs.us-west-2.amazonaws.com/__xxx__/QUEUENAME.fifo"

  ## Amazon Credentials
  ## Credentials are loaded in the following order
  ## 1) Assumed credentials via STS if role_arn is specified
  ## 2) explicit credentials from 'access_key' and 'secret_key'
  ## 3) shared profile from 'profile'
  ## 4) environment variables
  ## 5) shared credentials file
  ## 6) EC2 Instance Profile
  # access_key = ""
  # secret_key = ""
  # token = ""
  # role_arn = ""
  # profile = ""
  # shared_credential_file = ""

  ## Maximum messages to read from the broker that have not been written by an
  ## output.  For best throughput set based on the number of metrics within
  ## each message and the size of the output's metric_batch_size.
  ##
  ## For example, if each message from the queue contains 10 metrics and the
  ## output metric_batch_size is 1000, setting this to 100 will ensure that a
  ## full batch is collected and the write is triggered immediately without
  ## waiting until the next flush_interval.
  # max_undelivered_messages = 1000

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`

func init() {
	inputs.Add("sqs_consumer", func() telegraf.Input {
		return &SqsConsumer{
			MaxUndeliveredMessages: defaultMaxUndeliveredMessages,
		}
	})
}

func (sq *SqsConsumer) SampleConfig() string {
	return sampleConfig
}

func (sq *SqsConsumer) Description() string {
	return "Configuration for the AWS SQS input."
}

func (sq *SqsConsumer) SetParser(parser parsers.Parser) {
	sq.parser = parser
}

func (sq *SqsConsumer) setupSqsConn() {
	credentialConfig := &internalaws.CredentialConfig{
		Region:    sq.Region,
		AccessKey: sq.AccessKey,
		SecretKey: sq.SecretKey,
		RoleARN:   sq.RoleARN,
		Profile:   sq.Profile,
		Filename:  sq.Filename,
		Token:     sq.Token,
	}
	configProvider := credentialConfig.Credentials()
	sq.sqsConn = sqs.New(configProvider)
}

func (sq *SqsConsumer) Start(ac telegraf.Accumulator) error {
	sq.records = make(
		map[telegraf.TrackingID]*string,
		sq.MaxUndeliveredMessages,
	)
	sq.ctx, sq.cancel = context.WithCancel(context.Background())
	sq.setupSqsConn()
    tracker := ac.WithTracking(sq.MaxUndeliveredMessages)

	// start our deliveries goroutine
	sq.wg.Add(1)
	go sq.handleDeliveries(tracker)

	// now start our polling queue
	sq.wg.Add(1)
	go sq.pollSqs(tracker)

	return nil
}

func (sq *SqsConsumer) Stop() {
	sq.cancel()
	sq.wg.Wait()
}

func (sq *SqsConsumer) Gather(acc telegraf.Accumulator) error {
	return nil
}

func (sq *SqsConsumer) pollSqs(tracker telegraf.TrackingAccumulator) {
	defer sq.wg.Done()
	pollch := make(chan *sqs.ReceiveMessageOutput, sq.MaxUndeliveredMessages)

    // purposely not adding a wait on this.  kill this with fire if we exit
    // since it's a long poll against sqs
    // run double of these, should be tunable via configs?
	go func() {
		for {
			msgs, err := sq.sqsConn.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:            &sq.QueueUrl,
				MaxNumberOfMessages: aws.Int64(sqsMaxMsgs),
				WaitTimeSeconds:     aws.Int64(sqsPollWaitSecs),
			})
			if err != nil {
				sq.Log.Warnf("failed to fetch sqs message %v", err)
				continue
			}
            pollch <-msgs
		}
	}()

	for {
		select {
		case <-sq.ctx.Done():
			return
		case msgs := <-pollch:
			for _, message := range msgs.Messages {
			    sq.handleMsg(message, tracker)
			}
		}
	}
}

func (sq *SqsConsumer) handleMsg(
        msg *sqs.Message,
        tracker telegraf.TrackingAccumulator,
    ) error {
	metrics, err := sq.parser.Parse([]byte(*msg.Body))
	if err != nil {
		tracker.AddError(err)
		return err
	}
	if len(metrics) == 0 {
		return nil
	}
	id := tracker.AddTrackingMetricGroup(metrics)
	sq.recMux.Lock()
	sq.records[id] = msg.ReceiptHandle
	sq.recMux.Unlock()
	return nil
}

func (sq *SqsConsumer) handleDeliveries(tracker telegraf.TrackingAccumulator) {
	defer sq.wg.Done()
	for {
		// ok to block here, either get done or a delivered signal to handle
		select {
		case <-sq.ctx.Done():
			return
		case info := <-tracker.Delivered():
			sq.recMux.Lock()
			rcptH, ok := sq.records[info.ID()]
			if !ok {
				sq.recMux.Unlock()
				continue
			}
			delete(sq.records, info.ID())
			sq.recMux.Unlock()
			if info.Delivered() {
				deleteOpts := &sqs.DeleteMessageInput{
					QueueUrl:      &sq.QueueUrl,
					ReceiptHandle: rcptH,
				}
				_, err := sq.sqsConn.DeleteMessage(deleteOpts)
				if err != nil {
				}
				// what to do with this err?
				// XXX TODO research delete batching
			} else {
				sq.Log.Warn("Metric Group FAILED to process")
			}
		}
	}
}
