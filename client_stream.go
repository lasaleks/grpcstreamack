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

type ClientStream struct {
	NameConnect    string
	NameStream     string
	QOS            int
	MaxSizeBuff    int
	WaitAckTimeout int
	Connect        func(context.Context) (grpc.ClientStream, error)
	InitAck        func() interface{}
	InitData       func() interface{}
	SetData        func(interface{})
}

func (s *ClientStream) GetDesc() string {
	return fmt.Sprintf("ClientStream %s Name:%s", s.NameStream, s.NameConnect)
}

func (s *ClientStream) Run(wg *sync.WaitGroup, ctx context.Context) error {
	count := 0
	defer wg.Done()
	log.Printf("Run %s", s.GetDesc())
	for {
		select {
		case <-ctx.Done(): // if cancel() execute
			return nil
		default:
		}
		count = 0
		QOS := s.QOS
		time.Sleep(time.Second * 5)
		ctx = metadata.NewOutgoingContext(ctx,
			metadata.Pairs(
				"qos", fmt.Sprintf("%d", s.QOS),
				"name", s.NameConnect,
				"max_size_buff", fmt.Sprintf("%d", s.MaxSizeBuff),
				"wait_ack_timeout", fmt.Sprintf("%d", s.WaitAckTimeout),
			),
		)
		client, err := s.Connect(ctx)
		//client, err := sppd.StreamNofTagZone(ctx)
		if err != nil {
			log.Printf("Connect %s error %s", s.GetDesc(), err)
			continue
		} else {
			log.Printf("Connect %s is Ok", s.GetDesc())
		}

		if md, err := client.Header(); err == nil {
			if len(md["qos"]) > 0 {
				if v, err := strconv.ParseInt(md["qos"][0], 10, 32); err == nil {
					QOS = int(v)
					log.Printf("%s server.QOS:%d\n", s.GetDesc(), QOS)
				}
			} else {
				log.Println("error QOS empty")
				QOS = 1
			}
		}
		for {
			select {
			case <-ctx.Done(): // if cancel() execute
				return nil
			default:
			}

			if count >= QOS {
				if err := client.SendMsg(s.InitAck()); err != nil {
					if err == io.EOF {
						log.Printf("%s stream closed", s.GetDesc())
						break
					} else if err != nil {
						log.Printf("%s %s error happed", s.GetDesc(), err)
						break
					}
				}
				count = 0
			}
			data := s.InitData()
			err := client.RecvMsg(data)
			count++
			if err == io.EOF {
				log.Printf("%s stream closed", s.GetDesc())
				break
			} else if err != nil {
				log.Printf("%s %s error happed", s.GetDesc(), err)
				break
			}
			s.SetData(data)
		}
	}
}
