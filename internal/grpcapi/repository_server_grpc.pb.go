// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.3
// source: repository_server.proto

package grpcapi

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	KopiaRepository_Session_FullMethodName = "/kopia_repository.KopiaRepository/Session"
)

// KopiaRepositoryClient is the client API for KopiaRepository service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type KopiaRepositoryClient interface {
	// Session starts a long-running repository session.
	Session(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SessionRequest, SessionResponse], error)
}

type kopiaRepositoryClient struct {
	cc grpc.ClientConnInterface
}

func NewKopiaRepositoryClient(cc grpc.ClientConnInterface) KopiaRepositoryClient {
	return &kopiaRepositoryClient{cc}
}

func (c *kopiaRepositoryClient) Session(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SessionRequest, SessionResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &KopiaRepository_ServiceDesc.Streams[0], KopiaRepository_Session_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SessionRequest, SessionResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type KopiaRepository_SessionClient = grpc.BidiStreamingClient[SessionRequest, SessionResponse]

// KopiaRepositoryServer is the server API for KopiaRepository service.
// All implementations must embed UnimplementedKopiaRepositoryServer
// for forward compatibility.
type KopiaRepositoryServer interface {
	// Session starts a long-running repository session.
	Session(grpc.BidiStreamingServer[SessionRequest, SessionResponse]) error
	mustEmbedUnimplementedKopiaRepositoryServer()
}

// UnimplementedKopiaRepositoryServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedKopiaRepositoryServer struct{}

func (UnimplementedKopiaRepositoryServer) Session(grpc.BidiStreamingServer[SessionRequest, SessionResponse]) error {
	return status.Errorf(codes.Unimplemented, "method Session not implemented")
}
func (UnimplementedKopiaRepositoryServer) mustEmbedUnimplementedKopiaRepositoryServer() {}
func (UnimplementedKopiaRepositoryServer) testEmbeddedByValue()                         {}

// UnsafeKopiaRepositoryServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to KopiaRepositoryServer will
// result in compilation errors.
type UnsafeKopiaRepositoryServer interface {
	mustEmbedUnimplementedKopiaRepositoryServer()
}

func RegisterKopiaRepositoryServer(s grpc.ServiceRegistrar, srv KopiaRepositoryServer) {
	// If the following call pancis, it indicates UnimplementedKopiaRepositoryServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&KopiaRepository_ServiceDesc, srv)
}

func _KopiaRepository_Session_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(KopiaRepositoryServer).Session(&grpc.GenericServerStream[SessionRequest, SessionResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type KopiaRepository_SessionServer = grpc.BidiStreamingServer[SessionRequest, SessionResponse]

// KopiaRepository_ServiceDesc is the grpc.ServiceDesc for KopiaRepository service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var KopiaRepository_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "kopia_repository.KopiaRepository",
	HandlerType: (*KopiaRepositoryServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Session",
			Handler:       _KopiaRepository_Session_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "repository_server.proto",
}
