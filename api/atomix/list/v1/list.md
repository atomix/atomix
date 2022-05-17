# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/list/v1/list.proto](#atomix_list_v1_list-proto)
    - [AppendInput](#atomix-list-v1-AppendInput)
    - [AppendOutput](#atomix-list-v1-AppendOutput)
    - [AppendRequest](#atomix-list-v1-AppendRequest)
    - [AppendResponse](#atomix-list-v1-AppendResponse)
    - [ClearInput](#atomix-list-v1-ClearInput)
    - [ClearOutput](#atomix-list-v1-ClearOutput)
    - [ClearRequest](#atomix-list-v1-ClearRequest)
    - [ClearResponse](#atomix-list-v1-ClearResponse)
    - [ContainsInput](#atomix-list-v1-ContainsInput)
    - [ContainsOutput](#atomix-list-v1-ContainsOutput)
    - [ContainsRequest](#atomix-list-v1-ContainsRequest)
    - [ContainsResponse](#atomix-list-v1-ContainsResponse)
    - [ElementsInput](#atomix-list-v1-ElementsInput)
    - [ElementsOutput](#atomix-list-v1-ElementsOutput)
    - [ElementsRequest](#atomix-list-v1-ElementsRequest)
    - [ElementsResponse](#atomix-list-v1-ElementsResponse)
    - [Event](#atomix-list-v1-Event)
    - [EventsInput](#atomix-list-v1-EventsInput)
    - [EventsOutput](#atomix-list-v1-EventsOutput)
    - [EventsRequest](#atomix-list-v1-EventsRequest)
    - [EventsResponse](#atomix-list-v1-EventsResponse)
    - [GetInput](#atomix-list-v1-GetInput)
    - [GetOutput](#atomix-list-v1-GetOutput)
    - [GetRequest](#atomix-list-v1-GetRequest)
    - [GetResponse](#atomix-list-v1-GetResponse)
    - [InsertInput](#atomix-list-v1-InsertInput)
    - [InsertOutput](#atomix-list-v1-InsertOutput)
    - [InsertRequest](#atomix-list-v1-InsertRequest)
    - [InsertResponse](#atomix-list-v1-InsertResponse)
    - [Item](#atomix-list-v1-Item)
    - [Precondition](#atomix-list-v1-Precondition)
    - [RemoveInput](#atomix-list-v1-RemoveInput)
    - [RemoveOutput](#atomix-list-v1-RemoveOutput)
    - [RemoveRequest](#atomix-list-v1-RemoveRequest)
    - [RemoveResponse](#atomix-list-v1-RemoveResponse)
    - [SetInput](#atomix-list-v1-SetInput)
    - [SetOutput](#atomix-list-v1-SetOutput)
    - [SetRequest](#atomix-list-v1-SetRequest)
    - [SetResponse](#atomix-list-v1-SetResponse)
    - [SizeInput](#atomix-list-v1-SizeInput)
    - [SizeOutput](#atomix-list-v1-SizeOutput)
    - [SizeRequest](#atomix-list-v1-SizeRequest)
    - [SizeResponse](#atomix-list-v1-SizeResponse)
    - [Value](#atomix-list-v1-Value)
  
    - [Event.Type](#atomix-list-v1-Event-Type)
  
    - [List](#atomix-list-v1-List)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_list_v1_list-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/list/v1/list.proto



<a name="atomix-list-v1-AppendInput"></a>

### AppendInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-AppendOutput"></a>

### AppendOutput







<a name="atomix-list-v1-AppendRequest"></a>

### AppendRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [AppendInput](#atomix-list-v1-AppendInput) |  |  |






<a name="atomix-list-v1-AppendResponse"></a>

### AppendResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [AppendOutput](#atomix-list-v1-AppendOutput) |  |  |






<a name="atomix-list-v1-ClearInput"></a>

### ClearInput







<a name="atomix-list-v1-ClearOutput"></a>

### ClearOutput







<a name="atomix-list-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [ClearInput](#atomix-list-v1-ClearInput) |  |  |






<a name="atomix-list-v1-ClearResponse"></a>

### ClearResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [ClearOutput](#atomix-list-v1-ClearOutput) |  |  |






<a name="atomix-list-v1-ContainsInput"></a>

### ContainsInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-ContainsOutput"></a>

### ContainsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| contains | [bool](#bool) |  |  |






<a name="atomix-list-v1-ContainsRequest"></a>

### ContainsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [ContainsInput](#atomix-list-v1-ContainsInput) |  |  |






<a name="atomix-list-v1-ContainsResponse"></a>

### ContainsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [ContainsOutput](#atomix-list-v1-ContainsOutput) |  |  |






<a name="atomix-list-v1-ElementsInput"></a>

### ElementsInput







<a name="atomix-list-v1-ElementsOutput"></a>

### ElementsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-ElementsRequest"></a>

### ElementsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [ElementsInput](#atomix-list-v1-ElementsInput) |  |  |






<a name="atomix-list-v1-ElementsResponse"></a>

### ElementsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [ElementsOutput](#atomix-list-v1-ElementsOutput) |  |  |






<a name="atomix-list-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#atomix-list-v1-Event-Type) |  |  |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-EventsInput"></a>

### EventsInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| replay | [bool](#bool) |  |  |






<a name="atomix-list-v1-EventsOutput"></a>

### EventsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-list-v1-Event) |  |  |






<a name="atomix-list-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [EventsInput](#atomix-list-v1-EventsInput) |  |  |






<a name="atomix-list-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [EventsOutput](#atomix-list-v1-EventsOutput) |  |  |






<a name="atomix-list-v1-GetInput"></a>

### GetInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |






<a name="atomix-list-v1-GetOutput"></a>

### GetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [GetInput](#atomix-list-v1-GetInput) |  |  |






<a name="atomix-list-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [GetOutput](#atomix-list-v1-GetOutput) |  |  |






<a name="atomix-list-v1-InsertInput"></a>

### InsertInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |
| preconditions | [Precondition](#atomix-list-v1-Precondition) | repeated |  |






<a name="atomix-list-v1-InsertOutput"></a>

### InsertOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-InsertRequest"></a>

### InsertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [InsertInput](#atomix-list-v1-InsertInput) |  |  |






<a name="atomix-list-v1-InsertResponse"></a>

### InsertResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [InsertOutput](#atomix-list-v1-InsertOutput) |  |  |






<a name="atomix-list-v1-Item"></a>

### Item



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-Precondition"></a>

### Precondition



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [atomix.runtime.v1.ObjectMeta](#atomix-runtime-v1-ObjectMeta) |  |  |






<a name="atomix-list-v1-RemoveInput"></a>

### RemoveInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |
| preconditions | [Precondition](#atomix-list-v1-Precondition) | repeated |  |






<a name="atomix-list-v1-RemoveOutput"></a>

### RemoveOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [RemoveInput](#atomix-list-v1-RemoveInput) |  |  |






<a name="atomix-list-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [RemoveOutput](#atomix-list-v1-RemoveOutput) |  |  |






<a name="atomix-list-v1-SetInput"></a>

### SetInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |
| preconditions | [Precondition](#atomix-list-v1-Precondition) | repeated |  |






<a name="atomix-list-v1-SetOutput"></a>

### SetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [SetInput](#atomix-list-v1-SetInput) |  |  |






<a name="atomix-list-v1-SetResponse"></a>

### SetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [SetOutput](#atomix-list-v1-SetOutput) |  |  |






<a name="atomix-list-v1-SizeInput"></a>

### SizeInput







<a name="atomix-list-v1-SizeOutput"></a>

### SizeOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-list-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveRequestHeaders](#atomix-runtime-v1-PrimitiveRequestHeaders) |  |  |
| input | [SizeInput](#atomix-list-v1-SizeInput) |  |  |






<a name="atomix-list-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.runtime.v1.PrimitiveResponseHeaders](#atomix-runtime-v1-PrimitiveResponseHeaders) |  |  |
| output | [SizeOutput](#atomix-list-v1-SizeOutput) |  |  |






<a name="atomix-list-v1-Value"></a>

### Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meta | [atomix.runtime.v1.ObjectMeta](#atomix-runtime-v1-ObjectMeta) |  |  |
| value | [string](#string) |  |  |





 


<a name="atomix-list-v1-Event-Type"></a>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| ADD | 1 |  |
| REMOVE | 2 |  |
| REPLAY | 3 |  |


 

 


<a name="atomix-list-v1-List"></a>

### List
List is a service for a list primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Size | [SizeRequest](#atomix-list-v1-SizeRequest) | [SizeResponse](#atomix-list-v1-SizeResponse) | Size gets the number of elements in the list |
| Append | [AppendRequest](#atomix-list-v1-AppendRequest) | [AppendResponse](#atomix-list-v1-AppendResponse) | Append appends a value to the list |
| Insert | [InsertRequest](#atomix-list-v1-InsertRequest) | [InsertResponse](#atomix-list-v1-InsertResponse) | Insert inserts a value at a specific index in the list |
| Get | [GetRequest](#atomix-list-v1-GetRequest) | [GetResponse](#atomix-list-v1-GetResponse) | Get gets the value at an index in the list |
| Set | [SetRequest](#atomix-list-v1-SetRequest) | [SetResponse](#atomix-list-v1-SetResponse) | Set sets the value at an index in the list |
| Remove | [RemoveRequest](#atomix-list-v1-RemoveRequest) | [RemoveResponse](#atomix-list-v1-RemoveResponse) | Remove removes an element from the list |
| Clear | [ClearRequest](#atomix-list-v1-ClearRequest) | [ClearResponse](#atomix-list-v1-ClearResponse) | Clear removes all elements from the list |
| Events | [EventsRequest](#atomix-list-v1-EventsRequest) | [EventsResponse](#atomix-list-v1-EventsResponse) stream | Events listens for change events |
| Elements | [ElementsRequest](#atomix-list-v1-ElementsRequest) | [ElementsResponse](#atomix-list-v1-ElementsResponse) stream | Elements streams all elements in the list |

 



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

