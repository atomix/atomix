// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: atomix/runtime/v1/runtime.proto

package v1

import (
	context "context"
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type RuntimeInfo struct {
	Version string `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
}

func (m *RuntimeInfo) Reset()         { *m = RuntimeInfo{} }
func (m *RuntimeInfo) String() string { return proto.CompactTextString(m) }
func (*RuntimeInfo) ProtoMessage()    {}
func (*RuntimeInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_d426e124a0fc8e61, []int{0}
}
func (m *RuntimeInfo) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RuntimeInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RuntimeInfo.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RuntimeInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RuntimeInfo.Merge(m, src)
}
func (m *RuntimeInfo) XXX_Size() int {
	return m.Size()
}
func (m *RuntimeInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_RuntimeInfo.DiscardUnknown(m)
}

var xxx_messageInfo_RuntimeInfo proto.InternalMessageInfo

func (m *RuntimeInfo) GetVersion() string {
	if m != nil {
		return m.Version
	}
	return ""
}

type GetRuntimeInfoRequest struct {
}

func (m *GetRuntimeInfoRequest) Reset()         { *m = GetRuntimeInfoRequest{} }
func (m *GetRuntimeInfoRequest) String() string { return proto.CompactTextString(m) }
func (*GetRuntimeInfoRequest) ProtoMessage()    {}
func (*GetRuntimeInfoRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_d426e124a0fc8e61, []int{1}
}
func (m *GetRuntimeInfoRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *GetRuntimeInfoRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_GetRuntimeInfoRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *GetRuntimeInfoRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetRuntimeInfoRequest.Merge(m, src)
}
func (m *GetRuntimeInfoRequest) XXX_Size() int {
	return m.Size()
}
func (m *GetRuntimeInfoRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetRuntimeInfoRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetRuntimeInfoRequest proto.InternalMessageInfo

type GetRuntimeInfoResponse struct {
	RuntimeInfo RuntimeInfo `protobuf:"bytes,1,opt,name=runtime_info,json=runtimeInfo,proto3" json:"runtime_info"`
}

func (m *GetRuntimeInfoResponse) Reset()         { *m = GetRuntimeInfoResponse{} }
func (m *GetRuntimeInfoResponse) String() string { return proto.CompactTextString(m) }
func (*GetRuntimeInfoResponse) ProtoMessage()    {}
func (*GetRuntimeInfoResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_d426e124a0fc8e61, []int{2}
}
func (m *GetRuntimeInfoResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *GetRuntimeInfoResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_GetRuntimeInfoResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *GetRuntimeInfoResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetRuntimeInfoResponse.Merge(m, src)
}
func (m *GetRuntimeInfoResponse) XXX_Size() int {
	return m.Size()
}
func (m *GetRuntimeInfoResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetRuntimeInfoResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetRuntimeInfoResponse proto.InternalMessageInfo

func (m *GetRuntimeInfoResponse) GetRuntimeInfo() RuntimeInfo {
	if m != nil {
		return m.RuntimeInfo
	}
	return RuntimeInfo{}
}

func init() {
	proto.RegisterType((*RuntimeInfo)(nil), "atomix.runtime.v1.RuntimeInfo")
	proto.RegisterType((*GetRuntimeInfoRequest)(nil), "atomix.runtime.v1.GetRuntimeInfoRequest")
	proto.RegisterType((*GetRuntimeInfoResponse)(nil), "atomix.runtime.v1.GetRuntimeInfoResponse")
}

func init() { proto.RegisterFile("atomix/runtime/v1/runtime.proto", fileDescriptor_d426e124a0fc8e61) }

var fileDescriptor_d426e124a0fc8e61 = []byte{
	// 224 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x4f, 0x2c, 0xc9, 0xcf,
	0xcd, 0xac, 0xd0, 0x2f, 0x2a, 0xcd, 0x2b, 0xc9, 0xcc, 0x4d, 0xd5, 0x2f, 0x33, 0x84, 0x31, 0xf5,
	0x0a, 0x8a, 0xf2, 0x4b, 0xf2, 0x85, 0x04, 0x21, 0x0a, 0xf4, 0x60, 0xa2, 0x65, 0x86, 0x52, 0x22,
	0xe9, 0xf9, 0xe9, 0xf9, 0x60, 0x59, 0x7d, 0x10, 0x0b, 0xa2, 0x50, 0x49, 0x9d, 0x8b, 0x3b, 0x08,
	0xa2, 0xc6, 0x33, 0x2f, 0x2d, 0x5f, 0x48, 0x82, 0x8b, 0xbd, 0x2c, 0xb5, 0xa8, 0x38, 0x33, 0x3f,
	0x4f, 0x82, 0x51, 0x81, 0x51, 0x83, 0x33, 0x08, 0xc6, 0x55, 0x12, 0xe7, 0x12, 0x75, 0x4f, 0x2d,
	0x41, 0x52, 0x1b, 0x94, 0x5a, 0x58, 0x9a, 0x5a, 0x5c, 0xa2, 0x94, 0xc8, 0x25, 0x86, 0x2e, 0x51,
	0x5c, 0x90, 0x9f, 0x57, 0x9c, 0x2a, 0xe4, 0xce, 0xc5, 0x03, 0xb5, 0x3f, 0x3e, 0x33, 0x2f, 0x2d,
	0x1f, 0x6c, 0x22, 0xb7, 0x91, 0x9c, 0x1e, 0x86, 0xdb, 0xf4, 0x90, 0x74, 0x3b, 0xb1, 0x9c, 0xb8,
	0x27, 0xcf, 0x10, 0xc4, 0x5d, 0x84, 0x10, 0x32, 0x2a, 0xe0, 0x62, 0x87, 0xaa, 0x10, 0x4a, 0xe5,
	0xe2, 0x43, 0xb5, 0x4d, 0x48, 0x03, 0x8b, 0x79, 0x58, 0x5d, 0x2a, 0xa5, 0x49, 0x84, 0x4a, 0x88,
	0xd3, 0x9d, 0x24, 0x4e, 0x3c, 0x92, 0x63, 0xbc, 0xf0, 0x48, 0x8e, 0xf1, 0xc1, 0x23, 0x39, 0xc6,
	0x09, 0x8f, 0xe5, 0x18, 0x2e, 0x3c, 0x96, 0x63, 0xb8, 0xf1, 0x58, 0x8e, 0x21, 0x89, 0x0d, 0x1c,
	0x6e, 0xc6, 0x80, 0x00, 0x00, 0x00, 0xff, 0xff, 0x8f, 0x65, 0xba, 0x7f, 0x83, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// RuntimeClient is the client API for Runtime service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type RuntimeClient interface {
	GetRuntimeInfo(ctx context.Context, in *GetRuntimeInfoRequest, opts ...grpc.CallOption) (*GetRuntimeInfoResponse, error)
}

type runtimeClient struct {
	cc *grpc.ClientConn
}

func NewRuntimeClient(cc *grpc.ClientConn) RuntimeClient {
	return &runtimeClient{cc}
}

func (c *runtimeClient) GetRuntimeInfo(ctx context.Context, in *GetRuntimeInfoRequest, opts ...grpc.CallOption) (*GetRuntimeInfoResponse, error) {
	out := new(GetRuntimeInfoResponse)
	err := c.cc.Invoke(ctx, "/atomix.runtime.v1.Runtime/GetRuntimeInfo", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RuntimeServer is the server API for Runtime service.
type RuntimeServer interface {
	GetRuntimeInfo(context.Context, *GetRuntimeInfoRequest) (*GetRuntimeInfoResponse, error)
}

// UnimplementedRuntimeServer can be embedded to have forward compatible implementations.
type UnimplementedRuntimeServer struct {
}

func (*UnimplementedRuntimeServer) GetRuntimeInfo(ctx context.Context, req *GetRuntimeInfoRequest) (*GetRuntimeInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetRuntimeInfo not implemented")
}

func RegisterRuntimeServer(s *grpc.Server, srv RuntimeServer) {
	s.RegisterService(&_Runtime_serviceDesc, srv)
}

func _Runtime_GetRuntimeInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRuntimeInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RuntimeServer).GetRuntimeInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/atomix.runtime.v1.Runtime/GetRuntimeInfo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RuntimeServer).GetRuntimeInfo(ctx, req.(*GetRuntimeInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Runtime_serviceDesc = grpc.ServiceDesc{
	ServiceName: "atomix.runtime.v1.Runtime",
	HandlerType: (*RuntimeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetRuntimeInfo",
			Handler:    _Runtime_GetRuntimeInfo_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "atomix/runtime/v1/runtime.proto",
}

func (m *RuntimeInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RuntimeInfo) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RuntimeInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Version) > 0 {
		i -= len(m.Version)
		copy(dAtA[i:], m.Version)
		i = encodeVarintRuntime(dAtA, i, uint64(len(m.Version)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *GetRuntimeInfoRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *GetRuntimeInfoRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *GetRuntimeInfoRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	return len(dAtA) - i, nil
}

func (m *GetRuntimeInfoResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *GetRuntimeInfoResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *GetRuntimeInfoResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	{
		size, err := m.RuntimeInfo.MarshalToSizedBuffer(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarintRuntime(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0xa
	return len(dAtA) - i, nil
}

func encodeVarintRuntime(dAtA []byte, offset int, v uint64) int {
	offset -= sovRuntime(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *RuntimeInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Version)
	if l > 0 {
		n += 1 + l + sovRuntime(uint64(l))
	}
	return n
}

func (m *GetRuntimeInfoRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	return n
}

func (m *GetRuntimeInfoResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = m.RuntimeInfo.Size()
	n += 1 + l + sovRuntime(uint64(l))
	return n
}

func sovRuntime(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozRuntime(x uint64) (n int) {
	return sovRuntime(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *RuntimeInfo) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRuntime
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RuntimeInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RuntimeInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRuntime
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthRuntime
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthRuntime
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Version = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRuntime(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthRuntime
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *GetRuntimeInfoRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRuntime
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: GetRuntimeInfoRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: GetRuntimeInfoRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipRuntime(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthRuntime
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *GetRuntimeInfoResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRuntime
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: GetRuntimeInfoResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: GetRuntimeInfoResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RuntimeInfo", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRuntime
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRuntime
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRuntime
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.RuntimeInfo.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRuntime(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthRuntime
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipRuntime(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowRuntime
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRuntime
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRuntime
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthRuntime
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupRuntime
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthRuntime
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthRuntime        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowRuntime          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupRuntime = fmt.Errorf("proto: unexpected end of group")
)
