package gs_client

import (
	"testing"
	"net"
	"model/pkg/lockpb"
	"google.golang.org/grpc/reflection"
	"util/log"
	"google.golang.org/grpc"
)

//mock gs rpc server
func TestGSRpcServer_Lock(t *testing.T) {
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	lockrpcpb.RegisterDLockServiceServer(s, &LockServer{})
	reflection.Register(s)

	if err = s.Serve(lis); err != nil {
		log.Fatal("failed to server: %v", err)
	}
}