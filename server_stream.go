package grpcstreamack

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	DEFAULT_GRPC_STREAM_QOS             = 50
	DEFAULT_GRPC_STREAM_MAX_SIZE_BUFF   = 4000
	DEFAULT_GRPC_STREAM_WAIT_ACK_TIMOUT = 60
)

type OptionStream struct {
	Name             string
	QOS              int
	MAX_SIZE_BUFF    int
	WAIT_ACK_TIMEOUT int
}

type ServerStream struct {
	Id         int32
	TypeStream string
	ch_send    chan interface{}
	finished   chan bool
	option     OptionStream
}

func NewSubStream(id int32, typeStream string, ctx context.Context) *ServerStream {
	sub := ServerStream{
		Id:         id,
		finished:   make(chan bool),
		TypeStream: typeStream,
	}
	sub.option = sub.GetOptionsFromContext(ctx)
	sub.ch_send = make(chan interface{}, sub.option.MAX_SIZE_BUFF)
	return &sub
}

func (s *ServerStream) GetOptionsFromContext(ctx context.Context) OptionStream {
	opt := OptionStream{
		Name:             "",
		QOS:              DEFAULT_GRPC_STREAM_QOS,
		MAX_SIZE_BUFF:    DEFAULT_GRPC_STREAM_MAX_SIZE_BUFF,
		WAIT_ACK_TIMEOUT: DEFAULT_GRPC_STREAM_WAIT_ACK_TIMOUT,
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if value, ok := md["name"]; ok {
			if len(value) > 0 {
				opt.Name = value[0]
			}
		}
		if value, ok := md["qos"]; ok {
			if v, err := strconv.ParseInt(value[0], 10, 32); err == nil {
				if v > 0 {
					opt.QOS = int(v)
				}
			}
		}
		if value, ok := md["max_size_buff"]; ok {
			if v, err := strconv.ParseInt(value[0], 10, 32); err == nil {
				if v > 0 {
					opt.MAX_SIZE_BUFF = int(v)
				}
			}
		}
		if value, ok := md["wait_ack_timeout"]; ok {
			if v, err := strconv.ParseInt(value[0], 10, 32); err == nil {
				if v > 0 {
					opt.WAIT_ACK_TIMEOUT = int(v)
				}
			}
		}
	}
	return opt
}

func (s *ServerStream) Send(value interface{}) {
	select {
	case s.ch_send <- value:
	default:
		// Default case is to avoid blocking in case client has already unsubscribed
		log.Printf("WARRNING max size exceeded %d Stream Id:%d Name:%s\n", len(s.ch_send), s.Id, s.option.Name)
		select {
		case s.finished <- true:
		default:
			// Default case is to avoid blocking in case client has already unsubscribed
		}
	}
}

/*
get_init_ack func() interface{} {
	return &insiteexpert.StreamAck{}
}
*/

func (s *ServerStream) Stream(stream grpc.ServerStream, get_init_ack func() interface{}) error {
	stream.SendHeader(metadata.Pairs("qos", fmt.Sprintf("%d", s.option.QOS)))
	fin := s.finished
	count := 0
	ack := make(chan bool, 2)
	go func() {
		defer log.Printf("Stream %s RecvEnd Client %s ID:%d", s.TypeStream, s.option.Name, s.Id)
		for {
			err := stream.RecvMsg(get_init_ack()) //&insiteexpert.StreamAck{}
			if err == io.EOF {
				select {
				case fin <- true:
				default:
					// Default case is to avoid blocking in case client has already unsubscribed
				}
				return
			}
			if err != nil {
				select {
				case fin <- true:
				default:
					// Default case is to avoid blocking in case client has already unsubscribed
				}
				return
			}
			ack <- true
		}
	}()

	for {
		if count >= s.option.QOS {
			select {
			case <-stream.Context().Done():
				return nil
			case <-ack:
				count = 0
			case <-s.finished:
				log.Printf("Stream %s Closing for client %s ID:%d", s.TypeStream, s.option.Name, s.Id)
				return nil
			case <-time.After(time.Second * time.Duration(s.option.WAIT_ACK_TIMEOUT)):
				log.Printf("Stream %s Wait ACK Timeout %d client %s ID:%d", s.TypeStream, s.option.WAIT_ACK_TIMEOUT, s.option.Name, s.Id)
				return nil
			}
		} else {
			select {
			case <-stream.Context().Done():
				return nil
			case <-fin:
				log.Printf("Closing stream %s for client %s ID:%d", s.TypeStream, s.option.Name, s.Id)
				return nil
			case value := <-s.ch_send:
				// копируем в память перед отправкой
				count++
				if err := stream.SendMsg(value); err != nil {
					return err
				}
			}
		}
	}
}
