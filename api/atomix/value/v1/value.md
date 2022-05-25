# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/value/v1/value.proto](#atomix_value_v1_value-proto)
    - [Event](#atomix-value-v1-Event)
    - [EventsInput](#atomix-value-v1-EventsInput)
    - [EventsOutput](#atomix-value-v1-EventsOutput)
    - [EventsRequest](#atomix-value-v1-EventsRequest)
    - [EventsResponse](#atomix-value-v1-EventsResponse)
    - [GetInput](#atomix-value-v1-GetInput)
    - [GetOutput](#atomix-value-v1-GetOutput)
    - [GetRequest](#atomix-value-v1-GetRequest)
    - [GetResponse](#atomix-value-v1-GetResponse)
    - [SetInput](#atomix-value-v1-SetInput)
    - [SetOutput](#atomix-value-v1-SetOutput)
    - [SetRequest](#atomix-value-v1-SetRequest)
    - [SetResponse](#atomix-value-v1-SetResponse)
    - [ValueConfig](#atomix-value-v1-ValueConfig)
  
    - [Event.Type](#atomix-value-v1-Event-Type)
  
    - [Value](#atomix-value-v1-Value)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_value_v1_value-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/value/v1/value.proto



<a name="atomix-value-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#atomix-value-v1-Event-Type) |  |  |
| value | [bytes](#bytes) |  |  |
| timestamp | [atomix.time.v1.Timestamp](#atomix-time-v1-Timestamp) |  |  |






<a name="atomix-value-v1-EventsInput"></a>

### EventsInput







<a name="atomix-value-v1-EventsOutput"></a>

### EventsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-value-v1-Event) |  |  |






<a name="atomix-value-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [EventsInput](#atomix-value-v1-EventsInput) |  |  |






<a name="atomix-value-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [EventsOutput](#atomix-value-v1-EventsOutput) |  |  |






<a name="atomix-value-v1-GetInput"></a>

### GetInput







<a name="atomix-value-v1-GetOutput"></a>

### GetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |
| timestamp | [atomix.time.v1.Timestamp](#atomix-time-v1-Timestamp) |  |  |






<a name="atomix-value-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [GetInput](#atomix-value-v1-GetInput) |  |  |






<a name="atomix-value-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [GetOutput](#atomix-value-v1-GetOutput) |  |  |






<a name="atomix-value-v1-SetInput"></a>

### SetInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |
| timestamp | [atomix.time.v1.Timestamp](#atomix-time-v1-Timestamp) |  |  |






<a name="atomix-value-v1-SetOutput"></a>

### SetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |
| timestamp | [atomix.time.v1.Timestamp](#atomix-time-v1-Timestamp) |  |  |






<a name="atomix-value-v1-SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [SetInput](#atomix-value-v1-SetInput) |  |  |






<a name="atomix-value-v1-SetResponse"></a>

### SetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [SetOutput](#atomix-value-v1-SetOutput) |  |  |






<a name="atomix-value-v1-ValueConfig"></a>

### ValueConfig






 


<a name="atomix-value-v1-Event-Type"></a>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| UPDATE | 1 |  |


 

 


<a name="atomix-value-v1-Value"></a>

### Value
Value is a service for a value primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Set | [SetRequest](#atomix-value-v1-SetRequest) | [SetResponse](#atomix-value-v1-SetResponse) | Set sets the value |
| Get | [GetRequest](#atomix-value-v1-GetRequest) | [GetResponse](#atomix-value-v1-GetResponse) | Get gets the value |
| Events | [EventsRequest](#atomix-value-v1-EventsRequest) | [EventsResponse](#atomix-value-v1-EventsResponse) stream | Events listens for value change events |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

