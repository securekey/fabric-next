// Code generated by protoc-gen-go.
// source: snap.proto
// DO NOT EDIT!

/*
Package protos is a generated protocol buffer package.

It is generated from these files:
	snap.proto

It has these top-level messages:
	Request
	Response
*/
package protos

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Status int32

const (
	Status_SUCCESS Status = 0
	Status_FAILED  Status = 1
)

var Status_name = map[int32]string{
	0: "SUCCESS",
	1: "FAILED",
}
var Status_value = map[string]int32{
	"SUCCESS": 0,
	"FAILED":  1,
}

func (x Status) String() string {
	return proto.EnumName(Status_name, int32(x))
}
func (Status) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type Request struct {
	SnapName string   `protobuf:"bytes,1,opt,name=snapName" json:"snapName,omitempty"`
	Args     [][]byte `protobuf:"bytes,2,rep,name=args,proto3" json:"args,omitempty"`
}

func (m *Request) Reset()                    { *m = Request{} }
func (m *Request) String() string            { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()               {}
func (*Request) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type Response struct {
	Error   string   `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
	Status  Status   `protobuf:"varint,2,opt,name=status,enum=snapprotos.Status" json:"status,omitempty"`
	Payload [][]byte `protobuf:"bytes,3,rep,name=payload,proto3" json:"payload,omitempty"`
}

func (m *Response) Reset()                    { *m = Response{} }
func (m *Response) String() string            { return proto.CompactTextString(m) }
func (*Response) ProtoMessage()               {}
func (*Response) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func init() {
	proto.RegisterType((*Request)(nil), "snapprotos.Request")
	proto.RegisterType((*Response)(nil), "snapprotos.Response")
	proto.RegisterEnum("snapprotos.Status", Status_name, Status_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// BobS - Temp Hack
const _ = grpc.SupportPackageIsVersion4

// Client API for Snap service

type SnapClient interface {
	Invoke(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error)
}

type snapClient struct {
	cc *grpc.ClientConn
}

func NewSnapClient(cc *grpc.ClientConn) SnapClient {
	return &snapClient{cc}
}

func (c *snapClient) Invoke(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := grpc.Invoke(ctx, "/snapprotos.Snap/Invoke", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Snap service

type SnapServer interface {
	Invoke(context.Context, *Request) (*Response, error)
}

func RegisterSnapServer(s *grpc.Server, srv SnapServer) {
	s.RegisterService(&_Snap_serviceDesc, srv)
}

func _Snap_Invoke_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SnapServer).Invoke(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/snapprotos.Snap/Invoke",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SnapServer).Invoke(ctx, req.(*Request))
	}
	return interceptor(ctx, in, info, handler)
}

var _Snap_serviceDesc = grpc.ServiceDesc{
	ServiceName: "snapprotos.Snap",
	HandlerType: (*SnapServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Invoke",
			Handler:    _Snap_Invoke_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: fileDescriptor0,
}

func init() { proto.RegisterFile("snap.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 261 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x54, 0x8f, 0xbf, 0x4f, 0xfb, 0x30,
	0x14, 0xc4, 0x9b, 0x6f, 0xfb, 0x4d, 0xca, 0x03, 0xa1, 0xea, 0xd1, 0x21, 0xea, 0x54, 0x32, 0x45,
	0x15, 0x38, 0x52, 0x11, 0x03, 0x03, 0x03, 0x94, 0x22, 0x55, 0x42, 0x0c, 0xb6, 0x58, 0xd8, 0x9c,
	0xf0, 0x5a, 0xa2, 0xd2, 0xd8, 0xd8, 0x0e, 0x52, 0xff, 0x7b, 0x14, 0x27, 0xfc, 0x9a, 0x7c, 0xe7,
	0xbb, 0x8f, 0xce, 0x06, 0xb0, 0x95, 0xd4, 0x4c, 0x1b, 0xe5, 0x14, 0x7a, 0xed, 0xa5, 0x4d, 0xae,
	0x20, 0xe2, 0xf4, 0x5e, 0x93, 0x75, 0x38, 0x81, 0x61, 0x13, 0x3c, 0xca, 0x1d, 0xc5, 0xc1, 0x34,
	0x48, 0x0f, 0xf8, 0xb7, 0x47, 0x84, 0x81, 0x34, 0x1b, 0x1b, 0xff, 0x9b, 0xf6, 0xd3, 0x23, 0xee,
	0x75, 0xb2, 0x86, 0x21, 0x27, 0xab, 0x55, 0x65, 0x09, 0xc7, 0xf0, 0x9f, 0x8c, 0x51, 0xa6, 0x03,
	0x5b, 0x83, 0x33, 0x08, 0xad, 0x93, 0xae, 0x6e, 0xb8, 0x20, 0x3d, 0x9e, 0x23, 0xfb, 0x59, 0x66,
	0xc2, 0x27, 0xbc, 0x6b, 0x60, 0x0c, 0x91, 0x96, 0xfb, 0x37, 0x25, 0x5f, 0xe2, 0xbe, 0x1f, 0xf9,
	0xb2, 0xb3, 0x53, 0x08, 0xdb, 0x2e, 0x1e, 0x42, 0x24, 0x9e, 0x16, 0x8b, 0xa5, 0x10, 0xa3, 0x1e,
	0x02, 0x84, 0xf7, 0x37, 0xab, 0x87, 0xe5, 0xdd, 0x28, 0x98, 0x5f, 0xc3, 0x40, 0x54, 0x52, 0xe3,
	0x25, 0x84, 0xab, 0xea, 0x43, 0x6d, 0x09, 0x4f, 0x7e, 0x4f, 0x75, 0x3f, 0x9c, 0x8c, 0xff, 0x5e,
	0xb6, 0x6f, 0x4f, 0x7a, 0xb7, 0xec, 0xf9, 0x6c, 0x53, 0xba, 0xd7, 0x3a, 0x67, 0x85, 0xda, 0x65,
	0x96, 0x8a, 0xda, 0xd0, 0x96, 0xf6, 0xd9, 0x5a, 0xe6, 0xa6, 0x2c, 0xce, 0x1b, 0xc8, 0x66, 0x52,
	0x97, 0x59, 0x8b, 0xe6, 0xa1, 0x3f, 0x2f, 0x3e, 0x03, 0x00, 0x00, 0xff, 0xff, 0x28, 0x05, 0x22,
	0xe4, 0x55, 0x01, 0x00, 0x00,
}
