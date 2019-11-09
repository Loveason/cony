package main

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"

	"github.com/loveason/cony"
)

var (
	wg sync.WaitGroup
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

	p1 := cony.NewPublisher("", queue1.Name)
	p2 := cony.NewPublisher("", queue2.Name)
	p3 := cony.NewPublisher("", queue3.Name)
	cli.Publish(p1)
	cli.Publish(p2)
	cli.Publish(p3)

	go func() {
		for cli.Loop() {
			select {
			case err := <-cli.Errors():
				fmt.Println(err)
			}
		}
	}()
	wg.Add(3)
	go publish(p1, "a")
	go publish(p2, "b")
	go publish(p3, "c")
	wg.Wait()
	fmt.Println("all done.")
}

func publish(p *cony.Publisher, flag string) {
	defer wg.Done()
	for i := 0; i < 100; i++ {
		body := fmt.Sprintf("msg_%s_%d", flag, i)
		err := p.Publish(amqp.Publishing{
			Body: []byte(body),
		})
		if err != nil {
			fmt.Printf("publish err:%s, body:%s\n", err.Error(), body)
		} else {
			fmt.Printf("%s publish success\n", body)
		}
	}
}
