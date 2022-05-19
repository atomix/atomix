# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/set/v1/set.proto](#atomix_set_v1_set-proto)
    - [AddInput](#atomix-set-v1-AddInput)
    - [AddOutput](#atomix-set-v1-AddOutput)
    - [AddRequest](#atomix-set-v1-AddRequest)
    - [AddResponse](#atomix-set-v1-AddResponse)
    - [ClearInput](#atomix-set-v1-ClearInput)
    - [ClearOutput](#atomix-set-v1-ClearOutput)
    - [ClearRequest](#atomix-set-v1-ClearRequest)
    - [ClearResponse](#atomix-set-v1-ClearResponse)
    - [ContainsInput](#atomix-set-v1-ContainsInput)
    - [ContainsOutput](#atomix-set-v1-ContainsOutput)
    - [ContainsRequest](#atomix-set-v1-ContainsRequest)
    - [ContainsResponse](#atomix-set-v1-ContainsResponse)
    - [Element](#atomix-set-v1-Element)
    - [ElementsInput](#atomix-set-v1-ElementsInput)
    - [ElementsOutput](#atomix-set-v1-ElementsOutput)
    - [ElementsRequest](#atomix-set-v1-ElementsRequest)
    - [ElementsResponse](#atomix-set-v1-ElementsResponse)
    - [Event](#atomix-set-v1-Event)
    - [EventsInput](#atomix-set-v1-EventsInput)
    - [EventsOutput](#atomix-set-v1-EventsOutput)
    - [EventsRequest](#atomix-set-v1-EventsRequest)
    - [EventsResponse](#atomix-set-v1-EventsResponse)
    - [RemoveInput](#atomix-set-v1-RemoveInput)
    - [RemoveOutput](#atomix-set-v1-RemoveOutput)
    - [RemoveRequest](#atomix-set-v1-RemoveRequest)
    - [RemoveResponse](#atomix-set-v1-RemoveResponse)
    - [SizeInput](#atomix-set-v1-SizeInput)
    - [SizeOutput](#atomix-set-v1-SizeOutput)
    - [SizeRequest](#atomix-set-v1-SizeRequest)
    - [SizeResponse](#atomix-set-v1-SizeResponse)
  
    - [Event.Type](#atomix-set-v1-Event-Type)
  
    - [Set](#atomix-set-v1-Set)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_set_v1_set-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/set/v1/set.proto



<a name="atomix-set-v1-AddInput"></a>

### AddInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-AddOutput"></a>

### AddOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-AddRequest"></a>

### AddRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.RequestHeaders](#atomix-runtime-v1-RequestHeaders) |  |  |
| input | [AddInput](#atomix-set-v1-AddInput) |  |  |






<a name="atomix-set-v1-AddResponse"></a>

### AddResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.ResponseHeaders](#atomix-runtime-v1-ResponseHeaders) |  |  |
| output | [AddOutput](#atomix-set-v1-AddOutput) |  |  |






<a name="atomix-set-v1-ClearInput"></a>

### ClearInput







<a name="atomix-set-v1-ClearOutput"></a>

### ClearOutput







<a name="atomix-set-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.RequestHeaders](#atomix-runtime-v1-RequestHeaders) |  |  |
| input | [ClearInput](#atomix-set-v1-ClearInput) |  |  |






<a name="atomix-set-v1-ClearResponse"></a>

### ClearResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.ResponseHeaders](#atomix-runtime-v1-ResponseHeaders) |  |  |
| output | [ClearOutput](#atomix-set-v1-ClearOutput) |  |  |






<a name="atomix-set-v1-ContainsInput"></a>

### ContainsInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-ContainsOutput"></a>

### ContainsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| contains | [bool](#bool) |  |  |






<a name="atomix-set-v1-ContainsRequest"></a>

### ContainsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.RequestHeaders](#atomix-runtime-v1-RequestHeaders) |  |  |
| input | [ContainsInput](#atomix-set-v1-ContainsInput) |  |  |






<a name="atomix-set-v1-ContainsResponse"></a>

### ContainsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.ResponseHeaders](#atomix-runtime-v1-ResponseHeaders) |  |  |
| output | [ContainsOutput](#atomix-set-v1-ContainsOutput) |  |  |






<a name="atomix-set-v1-Element"></a>

### Element



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meta | [atomix.runtime.v1.ObjectMeta](#atomix-runtime-v1-ObjectMeta) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-set-v1-ElementsInput"></a>

### ElementsInput







<a name="atomix-set-v1-ElementsOutput"></a>

### ElementsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-ElementsRequest"></a>

### ElementsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.RequestHeaders](#atomix-runtime-v1-RequestHeaders) |  |  |
| input | [ElementsInput](#atomix-set-v1-ElementsInput) |  |  |






<a name="atomix-set-v1-ElementsResponse"></a>

### ElementsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.ResponseHeaders](#atomix-runtime-v1-ResponseHeaders) |  |  |
| output | [ElementsOutput](#atomix-set-v1-ElementsOutput) |  |  |






<a name="atomix-set-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#atomix-set-v1-Event-Type) |  |  |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-EventsInput"></a>

### EventsInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| replay | [bool](#bool) |  |  |






<a name="atomix-set-v1-EventsOutput"></a>

### EventsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-set-v1-Event) |  |  |






<a name="atomix-set-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.RequestHeaders](#atomix-runtime-v1-RequestHeaders) |  |  |
| input | [EventsInput](#atomix-set-v1-EventsInput) |  |  |






<a name="atomix-set-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.ResponseHeaders](#atomix-runtime-v1-ResponseHeaders) |  |  |
| output | [EventsOutput](#atomix-set-v1-EventsOutput) |  |  |






<a name="atomix-set-v1-RemoveInput"></a>

### RemoveInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-RemoveOutput"></a>

### RemoveOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.RequestHeaders](#atomix-runtime-v1-RequestHeaders) |  |  |
| input | [RemoveInput](#atomix-set-v1-RemoveInput) |  |  |






<a name="atomix-set-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.ResponseHeaders](#atomix-runtime-v1-ResponseHeaders) |  |  |
| output | [RemoveOutput](#atomix-set-v1-RemoveOutput) |  |  |






<a name="atomix-set-v1-SizeInput"></a>

### SizeInput







<a name="atomix-set-v1-SizeOutput"></a>

### SizeOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-set-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.RequestHeaders](#atomix-runtime-v1-RequestHeaders) |  |  |
| input | [SizeInput](#atomix-set-v1-SizeInput) |  |  |






<a name="atomix-set-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.ResponseHeaders](#atomix-runtime-v1-ResponseHeaders) |  |  |
| output | [SizeOutput](#atomix-set-v1-SizeOutput) |  |  |





 


<a name="atomix-set-v1-Event-Type"></a>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| ADD | 1 |  |
| REMOVE | 2 |  |
| REPLAY | 3 |  |


 

 


<a name="atomix-set-v1-Set"></a>

### Set
Set is a service for a set primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Size | [SizeRequest](#atomix-set-v1-SizeRequest) | [SizeResponse](#atomix-set-v1-SizeResponse) | Size gets the number of elements in the set |
| Contains | [ContainsRequest](#atomix-set-v1-ContainsRequest) | [ContainsResponse](#atomix-set-v1-ContainsResponse) | Contains returns whether the set contains a value |
| Add | [AddRequest](#atomix-set-v1-AddRequest) | [AddResponse](#atomix-set-v1-AddResponse) | Add adds a value to the set |
| Remove | [RemoveRequest](#atomix-set-v1-RemoveRequest) | [RemoveResponse](#atomix-set-v1-RemoveResponse) | Remove removes a value from the set |
| Clear | [ClearRequest](#atomix-set-v1-ClearRequest) | [ClearResponse](#atomix-set-v1-ClearResponse) | Clear removes all values from the set |
| Events | [EventsRequest](#atomix-set-v1-EventsRequest) | [EventsResponse](#atomix-set-v1-EventsResponse) stream | Events listens for set change events |
| Elements | [ElementsRequest](#atomix-set-v1-ElementsRequest) | [ElementsResponse](#atomix-set-v1-ElementsResponse) stream | Elements lists all elements in the set |

 



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

