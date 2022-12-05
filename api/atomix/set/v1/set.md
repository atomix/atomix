# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/set/v1/set.proto](#atomix_set_v1_set-proto)
    - [AddRequest](#atomix-set-v1-AddRequest)
    - [AddResponse](#atomix-set-v1-AddResponse)
    - [ClearRequest](#atomix-set-v1-ClearRequest)
    - [ClearResponse](#atomix-set-v1-ClearResponse)
    - [CloseRequest](#atomix-set-v1-CloseRequest)
    - [CloseResponse](#atomix-set-v1-CloseResponse)
    - [ContainsRequest](#atomix-set-v1-ContainsRequest)
    - [ContainsResponse](#atomix-set-v1-ContainsResponse)
    - [CreateRequest](#atomix-set-v1-CreateRequest)
    - [CreateResponse](#atomix-set-v1-CreateResponse)
    - [Element](#atomix-set-v1-Element)
    - [ElementsRequest](#atomix-set-v1-ElementsRequest)
    - [ElementsResponse](#atomix-set-v1-ElementsResponse)
    - [Event](#atomix-set-v1-Event)
    - [Event.Added](#atomix-set-v1-Event-Added)
    - [Event.Removed](#atomix-set-v1-Event-Removed)
    - [EventsRequest](#atomix-set-v1-EventsRequest)
    - [EventsResponse](#atomix-set-v1-EventsResponse)
    - [RemoveRequest](#atomix-set-v1-RemoveRequest)
    - [RemoveResponse](#atomix-set-v1-RemoveResponse)
    - [SizeRequest](#atomix-set-v1-SizeRequest)
    - [SizeResponse](#atomix-set-v1-SizeResponse)
  
    - [Set](#atomix-set-v1-Set)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_set_v1_set-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/set/v1/set.proto



<a name="atomix-set-v1-AddRequest"></a>

### AddRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| element | [Element](#atomix-set-v1-Element) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |






<a name="atomix-set-v1-AddResponse"></a>

### AddResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-set-v1-ClearResponse"></a>

### ClearResponse







<a name="atomix-set-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-set-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-set-v1-ContainsRequest"></a>

### ContainsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-ContainsResponse"></a>

### ContainsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| contains | [bool](#bool) |  |  |






<a name="atomix-set-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-set-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-set-v1-Element"></a>

### Element



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="atomix-set-v1-ElementsRequest"></a>

### ElementsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| watch | [bool](#bool) |  |  |






<a name="atomix-set-v1-ElementsResponse"></a>

### ElementsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| added | [Event.Added](#atomix-set-v1-Event-Added) |  |  |
| removed | [Event.Removed](#atomix-set-v1-Event-Removed) |  |  |






<a name="atomix-set-v1-Event-Added"></a>

### Event.Added



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-Event-Removed"></a>

### Event.Removed



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |
| expired | [bool](#bool) |  |  |






<a name="atomix-set-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-set-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-set-v1-Event) |  |  |






<a name="atomix-set-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-set-v1-Element) |  |  |






<a name="atomix-set-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-set-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |





 

 

 


<a name="atomix-set-v1-Set"></a>

### Set
Set is a service for a set primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-set-v1-CreateRequest) | [CreateResponse](#atomix-set-v1-CreateResponse) | Create creates the set |
| Close | [CloseRequest](#atomix-set-v1-CloseRequest) | [CloseResponse](#atomix-set-v1-CloseResponse) | Close closes the set |
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

