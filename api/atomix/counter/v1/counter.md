# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/counter/v1/counter.proto](#atomix_counter_v1_counter-proto)
    - [CounterConfig](#atomix-counter-v1-CounterConfig)
    - [DecrementInput](#atomix-counter-v1-DecrementInput)
    - [DecrementOutput](#atomix-counter-v1-DecrementOutput)
    - [DecrementRequest](#atomix-counter-v1-DecrementRequest)
    - [DecrementResponse](#atomix-counter-v1-DecrementResponse)
    - [GetInput](#atomix-counter-v1-GetInput)
    - [GetOutput](#atomix-counter-v1-GetOutput)
    - [GetRequest](#atomix-counter-v1-GetRequest)
    - [GetResponse](#atomix-counter-v1-GetResponse)
    - [IncrementInput](#atomix-counter-v1-IncrementInput)
    - [IncrementOutput](#atomix-counter-v1-IncrementOutput)
    - [IncrementRequest](#atomix-counter-v1-IncrementRequest)
    - [IncrementResponse](#atomix-counter-v1-IncrementResponse)
    - [Precondition](#atomix-counter-v1-Precondition)
    - [SetInput](#atomix-counter-v1-SetInput)
    - [SetOutput](#atomix-counter-v1-SetOutput)
    - [SetRequest](#atomix-counter-v1-SetRequest)
    - [SetResponse](#atomix-counter-v1-SetResponse)
    - [Value](#atomix-counter-v1-Value)
  
    - [Counter](#atomix-counter-v1-Counter)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_counter_v1_counter-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/counter/v1/counter.proto



<a name="atomix-counter-v1-CounterConfig"></a>

### CounterConfig







<a name="atomix-counter-v1-DecrementInput"></a>

### DecrementInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| delta | [int64](#int64) |  |  |






<a name="atomix-counter-v1-DecrementOutput"></a>

### DecrementOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-counter-v1-DecrementRequest"></a>

### DecrementRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [DecrementInput](#atomix-counter-v1-DecrementInput) |  |  |






<a name="atomix-counter-v1-DecrementResponse"></a>

### DecrementResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [DecrementOutput](#atomix-counter-v1-DecrementOutput) |  |  |






<a name="atomix-counter-v1-GetInput"></a>

### GetInput







<a name="atomix-counter-v1-GetOutput"></a>

### GetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-counter-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [GetInput](#atomix-counter-v1-GetInput) |  |  |






<a name="atomix-counter-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [GetOutput](#atomix-counter-v1-GetOutput) |  |  |






<a name="atomix-counter-v1-IncrementInput"></a>

### IncrementInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| delta | [int64](#int64) |  |  |






<a name="atomix-counter-v1-IncrementOutput"></a>

### IncrementOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-counter-v1-IncrementRequest"></a>

### IncrementRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [IncrementInput](#atomix-counter-v1-IncrementInput) |  |  |






<a name="atomix-counter-v1-IncrementResponse"></a>

### IncrementResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [IncrementOutput](#atomix-counter-v1-IncrementOutput) |  |  |






<a name="atomix-counter-v1-Precondition"></a>

### Precondition



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-counter-v1-SetInput"></a>

### SetInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |
| preconditions | [Precondition](#atomix-counter-v1-Precondition) | repeated |  |






<a name="atomix-counter-v1-SetOutput"></a>

### SetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-counter-v1-SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [SetInput](#atomix-counter-v1-SetInput) |  |  |






<a name="atomix-counter-v1-SetResponse"></a>

### SetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [SetOutput](#atomix-counter-v1-SetOutput) |  |  |






<a name="atomix-counter-v1-Value"></a>

### Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |





 

 

 


<a name="atomix-counter-v1-Counter"></a>

### Counter
Counter is a service for a counter primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Set | [SetRequest](#atomix-counter-v1-SetRequest) | [SetResponse](#atomix-counter-v1-SetResponse) | Set sets the counter value |
| Get | [GetRequest](#atomix-counter-v1-GetRequest) | [GetResponse](#atomix-counter-v1-GetResponse) | Get gets the current counter value |
| Increment | [IncrementRequest](#atomix-counter-v1-IncrementRequest) | [IncrementResponse](#atomix-counter-v1-IncrementResponse) | Increment increments the counter value |
| Decrement | [DecrementRequest](#atomix-counter-v1-DecrementRequest) | [DecrementResponse](#atomix-counter-v1-DecrementResponse) | Decrement decrements the counter value |

 



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

