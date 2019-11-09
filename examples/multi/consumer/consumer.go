package main

import (
	"fmt"
	"sync"

	"github.com/loveason/cony"
)

var (
	consumer1 *cony.Consumer
	consumer2 *cony.Consumer
	consumer3 *cony.Consumer
	wg        sync.WaitGroup
)

func main() {
	cli := cony.NewClient(
		cony.URL("amqp://test:test@47.96.11.241:5672/"),
		cony.Backoff(cony.DefaultBackoff),
	)
	queue1 := &cony.Queue{
		Name:    "cony_queue_1",
		Durable: true,
	}
	queue2 := &cony.Queue{
		Name:    "cony_queue_2",
		Durable: true,
	}
	queue3 := &cony.Queue{
		Name:    "cony_queue_3",
		Durable: true,
	}
	cli.Declare([]cony.Declaration{
		cony.DeclareQueue(queue1),
		cony.DeclareQueue(queue2),
		cony.DeclareQueue(queue3),
	})
	consumer1 = cony.NewConsumer(
		queue1,
		cony.Qos(1),
	)
	consumer2 = cony.NewConsumer(
		queue2,
		cony.Qos(1),
	)
	consumer3 = cony.NewConsumer(
		queue3,
		cony.Qos(1),
	)
	cli.Consume(consumer1)
	cli.Consume(consumer2)
	cli.Consume(consumer3)
	wg.Add(3)
	go func() {
		for cli.Loop() {
			select {
			case msg := <-consumer1.Deliveries():
				fmt.Printf("consumer1 msg:%s\n", msg.Body)
				msg.Ack(false)
			case err := <-consumer1.Errors():
				fmt.Printf("consumer1 err:%s\n", err.Error())
			case err := <-cli.Errors():
				fmt.Printf("client err:%s\n", err.Error())
			}
		}
	}()

	go func() {
		for cli.Loop() {
			select {
			case msg := <-consumer2.Deliveries():
				fmt.Printf("consumer2 msg:%s\n", msg.Body)
				msg.Ack(false)
			case err := <-consumer2.Errors():
				fmt.Printf("consumer2 err:%s\n", err.Error())
			case err := <-cli.Errors():
				fmt.Printf("client err:%s\n", err.Error())
			}
		}
	}()

	go func() {
		for cli.Loop() {
			select {
			case msg := <-consumer3.Deliveries():
				fmt.Printf("consumer3 msg:%s\n", msg.Body)
				msg.Ack(false)
			case err := <-consumer3.Errors():
				fmt.Printf("consumer3 err:%s\n", err.Error())
			case err := <-cli.Errors():
				fmt.Printf("client err:%s\n", err.Error())
			}
		}
	}()
	fmt.Println("wait task done.")
	wg.Wait()
	fmt.Println("task done.")
}
