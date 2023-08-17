package grpcstreamack

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientStreamOption struct {
	Name           string
	QOS            int
	MaxSizeBuff    int
	WaitAckTimeout int
}

/*
getInitAck func() {
	return &insiteexpert.StreamAck{}
}
*/

func RunClientStream(wg *sync.WaitGroup, ctx context.Context, getInitAck func() interface{}, connect func(context.Context) (grpc.ClientStream, error), getInitData func() interface{}, setData func(interface{}), option ClientStreamOption) error {
	count := 0
	defer wg.Done()
	fmt.Println("RunStreamNofTag")
	for {
		select {
		case <-ctx.Done(): // if cancel() execute
			return nil
		default:
		}
		count = 0
		QOS := option.QOS
		time.Sleep(time.Second * 5)
		ctx = metadata.NewOutgoingContext(ctx,
			metadata.Pairs(
				"qos", fmt.Sprintf("%d", option.QOS),
				"name", option.Name,
				"max_size_buff", fmt.Sprintf("%d", option.MaxSizeBuff),
				"wait_ack_timeout", fmt.Sprintf("%d", option.WaitAckTimeout),
			),
		)
		client, err := connect(ctx)
		//client, err := sppd.StreamNofTagZone(ctx)
		if err != nil {
			fmt.Println("error RunStreamNofTag", err)
			continue
		} else {
			fmt.Println("Connect to sppd StreamNofTagZone Ok")
		}

		if md, err := client.Header(); err == nil {
			if v, err := strconv.ParseInt(md["qos"][0], 10, 32); err == nil {
				QOS = int(v)
				fmt.Printf("QOS:%d\n", QOS)
			}
		}
		for {
			select {
			case <-ctx.Done(): // if cancel() execute
				return nil
			default:
			}

			if count >= QOS {
				log.Println("ACK")
				if err := client.SendMsg(getInitAck()); err != nil {
					if err == io.EOF {
						fmt.Println("\tstream closed")
						break
					} else if err != nil {
						fmt.Println("\terror happed", err)
						break
					}
				}
				count = 0
			}
			data := getInitData()
			err := client.RecvMsg(data)
			count++
			if err == io.EOF {
				fmt.Println("\tstream closed")
				break
			} else if err != nil {
				fmt.Println("\terror happed", err)
				break
			}
			setData(data)
		}
	}
}
