package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/loveason/cony"
)

const (
	addr = "amqp://test:test@47.96.11.241:5672/"
)

var cli *cony.Client
var consumer *cony.Consumer

func main() {
	r := gin.New()
	r.Use(gin.Logger())
	initMQ()
	r.GET("/consumer", handler)
	r.Run(":4000")
}

func initMQ() {
	cli = cony.NewClient(
		cony.URL(addr),
		cony.Backoff(cony.DefaultBackoff),
	)

	// Declarations
	// The queue name will be supplied by the AMQP server
	que := &cony.Queue{
		Name:       "cony_test",
		AutoDelete: false,
	}
	exc := cony.Exchange{
		Name:       "web_direct",
		Kind:       "direct",
		AutoDelete: false,
	}
	bnd := cony.Binding{
		Queue:    que,
		Exchange: exc,
		Key:      "",
	}
	cli.Declare([]cony.Declaration{
		cony.DeclareQueue(que),
		cony.DeclareExchange(exc),
		cony.DeclareBinding(bnd),
	})

	consumer = cony.NewConsumer(
		que,
		cony.Qos(1),
	)
	cli.Consume(consumer)

	go func() {
		for cli.Loop() {
			select {
			case err := <-cli.Errors():
				fmt.Println(err)
			}
		}
	}()
}

func handler(c *gin.Context) {
	// msg, ok, err := cli.Get("cony_test", false)
	// fmt.Println("result:", msg, ok, err)
	// if err != nil {
	// 	c.String(http.StatusOK, "err:%s", err.Error())
	// } else {
	// 	c.String(http.StatusOK, "msg:%s", msg.Body)
	// }
	msg := <-consumer.Deliveries()
	body := string(msg.Body)
	fmt.Println("msg:", body)
	if body == "loveason" {
		msg.Reject(true)
	} else {
		msg.Ack(false)
	}
	c.String(http.StatusOK, "msg:%s", body)
}
