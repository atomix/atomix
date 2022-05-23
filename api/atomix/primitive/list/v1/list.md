# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/primitive/list/v1/list.proto](#atomix_primitive_list_v1_list-proto)
    - [AppendInput](#atomix-primitive-list-v1-AppendInput)
    - [AppendOutput](#atomix-primitive-list-v1-AppendOutput)
    - [AppendRequest](#atomix-primitive-list-v1-AppendRequest)
    - [AppendResponse](#atomix-primitive-list-v1-AppendResponse)
    - [ClearInput](#atomix-primitive-list-v1-ClearInput)
    - [ClearOutput](#atomix-primitive-list-v1-ClearOutput)
    - [ClearRequest](#atomix-primitive-list-v1-ClearRequest)
    - [ClearResponse](#atomix-primitive-list-v1-ClearResponse)
    - [ContainsInput](#atomix-primitive-list-v1-ContainsInput)
    - [ContainsOutput](#atomix-primitive-list-v1-ContainsOutput)
    - [ContainsRequest](#atomix-primitive-list-v1-ContainsRequest)
    - [ContainsResponse](#atomix-primitive-list-v1-ContainsResponse)
    - [ElementsInput](#atomix-primitive-list-v1-ElementsInput)
    - [ElementsOutput](#atomix-primitive-list-v1-ElementsOutput)
    - [ElementsRequest](#atomix-primitive-list-v1-ElementsRequest)
    - [ElementsResponse](#atomix-primitive-list-v1-ElementsResponse)
    - [Event](#atomix-primitive-list-v1-Event)
    - [EventsInput](#atomix-primitive-list-v1-EventsInput)
    - [EventsOutput](#atomix-primitive-list-v1-EventsOutput)
    - [EventsRequest](#atomix-primitive-list-v1-EventsRequest)
    - [EventsResponse](#atomix-primitive-list-v1-EventsResponse)
    - [GetInput](#atomix-primitive-list-v1-GetInput)
    - [GetOutput](#atomix-primitive-list-v1-GetOutput)
    - [GetRequest](#atomix-primitive-list-v1-GetRequest)
    - [GetResponse](#atomix-primitive-list-v1-GetResponse)
    - [InsertInput](#atomix-primitive-list-v1-InsertInput)
    - [InsertOutput](#atomix-primitive-list-v1-InsertOutput)
    - [InsertRequest](#atomix-primitive-list-v1-InsertRequest)
    - [InsertResponse](#atomix-primitive-list-v1-InsertResponse)
    - [Item](#atomix-primitive-list-v1-Item)
    - [ListConfig](#atomix-primitive-list-v1-ListConfig)
    - [RemoveInput](#atomix-primitive-list-v1-RemoveInput)
    - [RemoveOutput](#atomix-primitive-list-v1-RemoveOutput)
    - [RemoveRequest](#atomix-primitive-list-v1-RemoveRequest)
    - [RemoveResponse](#atomix-primitive-list-v1-RemoveResponse)
    - [SetInput](#atomix-primitive-list-v1-SetInput)
    - [SetOutput](#atomix-primitive-list-v1-SetOutput)
    - [SetRequest](#atomix-primitive-list-v1-SetRequest)
    - [SetResponse](#atomix-primitive-list-v1-SetResponse)
    - [SizeInput](#atomix-primitive-list-v1-SizeInput)
    - [SizeOutput](#atomix-primitive-list-v1-SizeOutput)
    - [SizeRequest](#atomix-primitive-list-v1-SizeRequest)
    - [SizeResponse](#atomix-primitive-list-v1-SizeResponse)
    - [Value](#atomix-primitive-list-v1-Value)
  
    - [Event.Type](#atomix-primitive-list-v1-Event-Type)
  
    - [List](#atomix-primitive-list-v1-List)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_primitive_list_v1_list-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/primitive/list/v1/list.proto



<a name="atomix-primitive-list-v1-AppendInput"></a>

### AppendInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Value](#atomix-primitive-list-v1-Value) |  |  |






<a name="atomix-primitive-list-v1-AppendOutput"></a>

### AppendOutput







<a name="atomix-primitive-list-v1-AppendRequest"></a>

### AppendRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [AppendInput](#atomix-primitive-list-v1-AppendInput) |  |  |






<a name="atomix-primitive-list-v1-AppendResponse"></a>

### AppendResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [AppendOutput](#atomix-primitive-list-v1-AppendOutput) |  |  |






<a name="atomix-primitive-list-v1-ClearInput"></a>

### ClearInput







<a name="atomix-primitive-list-v1-ClearOutput"></a>

### ClearOutput







<a name="atomix-primitive-list-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [ClearInput](#atomix-primitive-list-v1-ClearInput) |  |  |






<a name="atomix-primitive-list-v1-ClearResponse"></a>

### ClearResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [ClearOutput](#atomix-primitive-list-v1-ClearOutput) |  |  |






<a name="atomix-primitive-list-v1-ContainsInput"></a>

### ContainsInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Value](#atomix-primitive-list-v1-Value) |  |  |






<a name="atomix-primitive-list-v1-ContainsOutput"></a>

### ContainsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| contains | [bool](#bool) |  |  |






<a name="atomix-primitive-list-v1-ContainsRequest"></a>

### ContainsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [ContainsInput](#atomix-primitive-list-v1-ContainsInput) |  |  |






<a name="atomix-primitive-list-v1-ContainsResponse"></a>

### ContainsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [ContainsOutput](#atomix-primitive-list-v1-ContainsOutput) |  |  |






<a name="atomix-primitive-list-v1-ElementsInput"></a>

### ElementsInput







<a name="atomix-primitive-list-v1-ElementsOutput"></a>

### ElementsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-primitive-list-v1-Item) |  |  |






<a name="atomix-primitive-list-v1-ElementsRequest"></a>

### ElementsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [ElementsInput](#atomix-primitive-list-v1-ElementsInput) |  |  |






<a name="atomix-primitive-list-v1-ElementsResponse"></a>

### ElementsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [ElementsOutput](#atomix-primitive-list-v1-ElementsOutput) |  |  |






<a name="atomix-primitive-list-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#atomix-primitive-list-v1-Event-Type) |  |  |
| item | [Item](#atomix-primitive-list-v1-Item) |  |  |






<a name="atomix-primitive-list-v1-EventsInput"></a>

### EventsInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| replay | [bool](#bool) |  |  |






<a name="atomix-primitive-list-v1-EventsOutput"></a>

### EventsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-primitive-list-v1-Event) |  |  |






<a name="atomix-primitive-list-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [EventsInput](#atomix-primitive-list-v1-EventsInput) |  |  |






<a name="atomix-primitive-list-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [EventsOutput](#atomix-primitive-list-v1-EventsOutput) |  |  |






<a name="atomix-primitive-list-v1-GetInput"></a>

### GetInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |






<a name="atomix-primitive-list-v1-GetOutput"></a>

### GetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-primitive-list-v1-Item) |  |  |






<a name="atomix-primitive-list-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [GetInput](#atomix-primitive-list-v1-GetInput) |  |  |






<a name="atomix-primitive-list-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [GetOutput](#atomix-primitive-list-v1-GetOutput) |  |  |






<a name="atomix-primitive-list-v1-InsertInput"></a>

### InsertInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |
| value | [Value](#atomix-primitive-list-v1-Value) |  |  |






<a name="atomix-primitive-list-v1-InsertOutput"></a>

### InsertOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-primitive-list-v1-Item) |  |  |






<a name="atomix-primitive-list-v1-InsertRequest"></a>

### InsertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [InsertInput](#atomix-primitive-list-v1-InsertInput) |  |  |






<a name="atomix-primitive-list-v1-InsertResponse"></a>

### InsertResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [InsertOutput](#atomix-primitive-list-v1-InsertOutput) |  |  |






<a name="atomix-primitive-list-v1-Item"></a>

### Item



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |
| value | [Value](#atomix-primitive-list-v1-Value) |  |  |
| timestamp | [atomix.primitive.meta.v1.Timestamp](#atomix-primitive-meta-v1-Timestamp) |  |  |






<a name="atomix-primitive-list-v1-ListConfig"></a>

### ListConfig







<a name="atomix-primitive-list-v1-RemoveInput"></a>

### RemoveInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |
| timestamp | [atomix.primitive.meta.v1.Timestamp](#atomix-primitive-meta-v1-Timestamp) |  |  |






<a name="atomix-primitive-list-v1-RemoveOutput"></a>

### RemoveOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-primitive-list-v1-Item) |  |  |






<a name="atomix-primitive-list-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [RemoveInput](#atomix-primitive-list-v1-RemoveInput) |  |  |






<a name="atomix-primitive-list-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [RemoveOutput](#atomix-primitive-list-v1-RemoveOutput) |  |  |






<a name="atomix-primitive-list-v1-SetInput"></a>

### SetInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |
| value | [Value](#atomix-primitive-list-v1-Value) |  |  |
| timestamp | [atomix.primitive.meta.v1.Timestamp](#atomix-primitive-meta-v1-Timestamp) |  |  |






<a name="atomix-primitive-list-v1-SetOutput"></a>

### SetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-primitive-list-v1-Item) |  |  |






<a name="atomix-primitive-list-v1-SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [SetInput](#atomix-primitive-list-v1-SetInput) |  |  |






<a name="atomix-primitive-list-v1-SetResponse"></a>

### SetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [SetOutput](#atomix-primitive-list-v1-SetOutput) |  |  |






<a name="atomix-primitive-list-v1-SizeInput"></a>

### SizeInput







<a name="atomix-primitive-list-v1-SizeOutput"></a>

### SizeOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-primitive-list-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [SizeInput](#atomix-primitive-list-v1-SizeInput) |  |  |






<a name="atomix-primitive-list-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [SizeOutput](#atomix-primitive-list-v1-SizeOutput) |  |  |






<a name="atomix-primitive-list-v1-Value"></a>

### Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |





 


<a name="atomix-primitive-list-v1-Event-Type"></a>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| ADD | 1 |  |
| REMOVE | 2 |  |
| REPLAY | 3 |  |


 

 


<a name="atomix-primitive-list-v1-List"></a>

### List
List is a service for a list primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Size | [SizeRequest](#atomix-primitive-list-v1-SizeRequest) | [SizeResponse](#atomix-primitive-list-v1-SizeResponse) | Size gets the number of elements in the list |
| Append | [AppendRequest](#atomix-primitive-list-v1-AppendRequest) | [AppendResponse](#atomix-primitive-list-v1-AppendResponse) | Append appends a value to the list |
| Insert | [InsertRequest](#atomix-primitive-list-v1-InsertRequest) | [InsertResponse](#atomix-primitive-list-v1-InsertResponse) | Insert inserts a value at a specific index in the list |
| Get | [GetRequest](#atomix-primitive-list-v1-GetRequest) | [GetResponse](#atomix-primitive-list-v1-GetResponse) | Get gets the value at an index in the list |
| Set | [SetRequest](#atomix-primitive-list-v1-SetRequest) | [SetResponse](#atomix-primitive-list-v1-SetResponse) | Set sets the value at an index in the list |
| Remove | [RemoveRequest](#atomix-primitive-list-v1-RemoveRequest) | [RemoveResponse](#atomix-primitive-list-v1-RemoveResponse) | Remove removes an element from the list |
| Clear | [ClearRequest](#atomix-primitive-list-v1-ClearRequest) | [ClearResponse](#atomix-primitive-list-v1-ClearResponse) | Clear removes all elements from the list |
| Events | [EventsRequest](#atomix-primitive-list-v1-EventsRequest) | [EventsResponse](#atomix-primitive-list-v1-EventsResponse) stream | Events listens for change events |
| Elements | [ElementsRequest](#atomix-primitive-list-v1-ElementsRequest) | [ElementsResponse](#atomix-primitive-list-v1-ElementsResponse) stream | Elements streams all elements in the list |

 



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

