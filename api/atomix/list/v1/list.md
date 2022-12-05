# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/list/v1/list.proto](#atomix_list_v1_list-proto)
    - [AppendRequest](#atomix-list-v1-AppendRequest)
    - [AppendResponse](#atomix-list-v1-AppendResponse)
    - [ClearRequest](#atomix-list-v1-ClearRequest)
    - [ClearResponse](#atomix-list-v1-ClearResponse)
    - [CloseRequest](#atomix-list-v1-CloseRequest)
    - [CloseResponse](#atomix-list-v1-CloseResponse)
    - [ContainsRequest](#atomix-list-v1-ContainsRequest)
    - [ContainsResponse](#atomix-list-v1-ContainsResponse)
    - [CreateRequest](#atomix-list-v1-CreateRequest)
    - [CreateResponse](#atomix-list-v1-CreateResponse)
    - [Event](#atomix-list-v1-Event)
    - [Event.Appended](#atomix-list-v1-Event-Appended)
    - [Event.Inserted](#atomix-list-v1-Event-Inserted)
    - [Event.Removed](#atomix-list-v1-Event-Removed)
    - [Event.Updated](#atomix-list-v1-Event-Updated)
    - [EventsRequest](#atomix-list-v1-EventsRequest)
    - [EventsResponse](#atomix-list-v1-EventsResponse)
    - [GetRequest](#atomix-list-v1-GetRequest)
    - [GetResponse](#atomix-list-v1-GetResponse)
    - [InsertRequest](#atomix-list-v1-InsertRequest)
    - [InsertResponse](#atomix-list-v1-InsertResponse)
    - [Item](#atomix-list-v1-Item)
    - [ItemsRequest](#atomix-list-v1-ItemsRequest)
    - [ItemsResponse](#atomix-list-v1-ItemsResponse)
    - [RemoveRequest](#atomix-list-v1-RemoveRequest)
    - [RemoveResponse](#atomix-list-v1-RemoveResponse)
    - [SetRequest](#atomix-list-v1-SetRequest)
    - [SetResponse](#atomix-list-v1-SetResponse)
    - [SizeRequest](#atomix-list-v1-SizeRequest)
    - [SizeResponse](#atomix-list-v1-SizeResponse)
    - [Value](#atomix-list-v1-Value)
  
    - [List](#atomix-list-v1-List)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_list_v1_list-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/list/v1/list.proto



<a name="atomix-list-v1-AppendRequest"></a>

### AppendRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-AppendResponse"></a>

### AppendResponse







<a name="atomix-list-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-list-v1-ClearResponse"></a>

### ClearResponse







<a name="atomix-list-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-list-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-list-v1-ContainsRequest"></a>

### ContainsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-ContainsResponse"></a>

### ContainsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| contains | [bool](#bool) |  |  |






<a name="atomix-list-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-list-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-list-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |
| appended | [Event.Appended](#atomix-list-v1-Event-Appended) |  |  |
| inserted | [Event.Inserted](#atomix-list-v1-Event-Inserted) |  |  |
| updated | [Event.Updated](#atomix-list-v1-Event-Updated) |  |  |
| removed | [Event.Removed](#atomix-list-v1-Event-Removed) |  |  |






<a name="atomix-list-v1-Event-Appended"></a>

### Event.Appended



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-Event-Inserted"></a>

### Event.Inserted



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-Event-Removed"></a>

### Event.Removed



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-Event-Updated"></a>

### Event.Updated



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Value](#atomix-list-v1-Value) |  |  |
| prev_value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| replay | [bool](#bool) |  |  |






<a name="atomix-list-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-list-v1-Event) |  |  |






<a name="atomix-list-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| index | [uint32](#uint32) |  |  |






<a name="atomix-list-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-InsertRequest"></a>

### InsertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| index | [uint32](#uint32) |  |  |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-InsertResponse"></a>

### InsertResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-Item"></a>

### Item



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint32](#uint32) |  |  |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-ItemsRequest"></a>

### ItemsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| watch | [bool](#bool) |  |  |






<a name="atomix-list-v1-ItemsResponse"></a>

### ItemsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| index | [uint32](#uint32) |  |  |






<a name="atomix-list-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| index | [uint32](#uint32) |  |  |
| value | [Value](#atomix-list-v1-Value) |  |  |






<a name="atomix-list-v1-SetResponse"></a>

### SetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#atomix-list-v1-Item) |  |  |






<a name="atomix-list-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-list-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-list-v1-Value"></a>

### Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |





 

 

 


<a name="atomix-list-v1-List"></a>

### List
List is a service for a list primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-list-v1-CreateRequest) | [CreateResponse](#atomix-list-v1-CreateResponse) | Create creates the list |
| Close | [CloseRequest](#atomix-list-v1-CloseRequest) | [CloseResponse](#atomix-list-v1-CloseResponse) | Close closes the list |
| Size | [SizeRequest](#atomix-list-v1-SizeRequest) | [SizeResponse](#atomix-list-v1-SizeResponse) | Size gets the number of elements in the list |
| Append | [AppendRequest](#atomix-list-v1-AppendRequest) | [AppendResponse](#atomix-list-v1-AppendResponse) | Append appends a value to the list |
| Insert | [InsertRequest](#atomix-list-v1-InsertRequest) | [InsertResponse](#atomix-list-v1-InsertResponse) | Insert inserts a value at a specific index in the list |
| Get | [GetRequest](#atomix-list-v1-GetRequest) | [GetResponse](#atomix-list-v1-GetResponse) | Get gets the value at an index in the list |
| Set | [SetRequest](#atomix-list-v1-SetRequest) | [SetResponse](#atomix-list-v1-SetResponse) | Set sets the value at an index in the list |
| Remove | [RemoveRequest](#atomix-list-v1-RemoveRequest) | [RemoveResponse](#atomix-list-v1-RemoveResponse) | Remove removes an element from the list |
| Clear | [ClearRequest](#atomix-list-v1-ClearRequest) | [ClearResponse](#atomix-list-v1-ClearResponse) | Clear removes all elements from the list |
| Events | [EventsRequest](#atomix-list-v1-EventsRequest) | [EventsResponse](#atomix-list-v1-EventsResponse) stream | Events listens for change events |
| Items | [ItemsRequest](#atomix-list-v1-ItemsRequest) | [ItemsResponse](#atomix-list-v1-ItemsResponse) stream | Items streams all items in the list |

 



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

