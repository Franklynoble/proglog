package server

import (
	"context"

	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
	"github.com/google/martian/v3/api"

api "github.com/Franklynoble/proglog/api/v1"
)










/*
directory, we’ll implement our server in a file called server.go and a package
named server. The first order of business is to define our server type and a
factory function to create an instance of the server.

*/

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct{
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config)(srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil 
}

/*
the API you saw in log_grpc.pb.go, we need to implement the Con-
sume() and Produce() handlers. Our gRPC layer is thin because it defers to our
log library, so to implement these methods, you call down to the library and
handle any error

*/
func (s *grpcServer) Produce (ctx context.Context, req *api.ProduceRequest) (

	*api.ProduceResponse, error) {
		offset, err := s.CommitLog.Append(req.Record)
	
		if err != nil {
			return nil, err
		}
		return &api.ProduceResponse{Offset: offset}, nil 
	}




func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
		record, err := s.CommitLog.Read(req.Offset)

		if err != nil {
			return nil, err 
		}
		return &api.ConsumeResponse{Record: record}, nil 
	}


	func(s *grpcServer) ProduceStream(
		stream api.logProduceStreamServer,
	)error{
		for{
			req, err := stream.Recv()
			if err != nil {
				return err 
			}
			res, err := s.Produce(stream.Context())
		  
		    if err != nil {
			return err
		    }
		    if err = stream.Send(re); err != nil {
			return err
		    }
		
		}
	}
}
 func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest, 
	stream api.Log_ConsumeStreamServer,
 ) error {
	for{
		Select {
			case <- stream.Context().Done();
			return nil 

		default: 
			res, err := s.Consume(stream.Context())
		}
	}
 }
