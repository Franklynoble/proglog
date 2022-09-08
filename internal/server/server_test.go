package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	api "github.com/Franklynoble/proglog/api/v1"
	"github.com/Franklynoble/proglog/internal/log"
)

/*
TestServer(*testing.T) defines our list of test cases and then runs a subtest for
each case. Add the following setupTest(*testing.T, func(*Config)) function below Test-
Server():
*/

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream suceeds":                     testProduceConsume,
		"consume past log boundry fails":                     testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

/*
setupTest(*testing.T, func(*Config)) is a helper function to set up each test case. Our
test setup begins by creating a listener on the local network address that our
server will run on. The 0 port is useful for when we don’t care what port we
use since 0 will automatically assign us a free port. We then make an insecure
connection to our listener and, with it, a client we’ll use to hit our server with.
Next we create our server and start serving requests in a goroutine because
*/
func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient,
	cfg *Config, teardown func(),
) {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NEWGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()
	client = api.NewLogClient(cc)
	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}

}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}
	produce, err := client.Produce(
		ctx, &api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello-world"),
		},
	})
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v, got", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {

	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	},
		{
			Value:  []byte("second message"),
			Offset: 1,
		}}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d",
					res.Offset, offset)
			}
		}

	}
	{
		stream, err := client.ConsumeStream(
			ctx, &api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()

			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}

}
