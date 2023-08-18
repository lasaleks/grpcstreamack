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
	NameConnect      string
	QOS              int
	MAX_SIZE_BUFF    int
	WAIT_ACK_TIMEOUT int
}

type ServerStream struct {
	Id         int32
	NameStream string
	ch_send    chan interface{}
	finished   chan bool
	Option     OptionStream
}

func NewSubStream(id int32, nameStream string, ctx context.Context) *ServerStream {
	sub := ServerStream{
		Id:         id,
		finished:   make(chan bool),
		NameStream: nameStream,
	}
	sub.Option = sub.GetOptionsFromContext(ctx)
	sub.ch_send = make(chan interface{}, sub.Option.MAX_SIZE_BUFF)
	return &sub
}

func (s *ServerStream) GetOptionsFromContext(ctx context.Context) OptionStream {
	opt := OptionStream{
		NameConnect:      "",
		QOS:              DEFAULT_GRPC_STREAM_QOS,
		MAX_SIZE_BUFF:    DEFAULT_GRPC_STREAM_MAX_SIZE_BUFF,
		WAIT_ACK_TIMEOUT: DEFAULT_GRPC_STREAM_WAIT_ACK_TIMOUT,
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if value, ok := md["name"]; ok {
			if len(value) > 0 {
				opt.NameConnect = value[0]
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

func (s *ServerStream) GetDesc() string {
	return fmt.Sprintf("Stream %s Id:%d Name:%s", s.NameStream, s.Id, s.Option.NameConnect)
}

func (s *ServerStream) Send(value interface{}) {
	select {
	case s.ch_send <- value:
	default:
		// Default case is to avoid blocking in case client has already unsubscribed
		log.Printf("%s WARRNING max size exceeded %d\n", s.GetDesc(), len(s.ch_send))
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
	log.Printf("Received subscribe %s", s.GetDesc())
	defer log.Printf("Client %s has disconnected", s.GetDesc())

	stream.SendHeader(metadata.Pairs("qos", fmt.Sprintf("%d", s.Option.QOS)))
	fin := s.finished
	count := 0
	ack := make(chan bool, 2)
	go func() {
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
		if count >= s.Option.QOS {
			select {
			case <-stream.Context().Done():
				return nil
			case <-ack:
				count = 0
			case <-s.finished:
				log.Printf("%s closing client", s.GetDesc())
				return nil
			case <-time.After(time.Second * time.Duration(s.Option.WAIT_ACK_TIMEOUT)):
				log.Printf("%s wait ACK timeout %d", s.GetDesc(), s.Option.WAIT_ACK_TIMEOUT)
				return nil
			}
		} else {
			select {
			case <-stream.Context().Done():
				return nil
			case <-fin:
				log.Printf("%s closing", s.GetDesc())
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
