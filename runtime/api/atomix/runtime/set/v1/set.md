# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/runtime/set/v1/set.proto](#atomix_runtime_set_v1_set-proto)
    - [AddRequest](#atomix-runtime-set-v1-AddRequest)
    - [AddResponse](#atomix-runtime-set-v1-AddResponse)
    - [ClearRequest](#atomix-runtime-set-v1-ClearRequest)
    - [ClearResponse](#atomix-runtime-set-v1-ClearResponse)
    - [CloseRequest](#atomix-runtime-set-v1-CloseRequest)
    - [CloseResponse](#atomix-runtime-set-v1-CloseResponse)
    - [ContainsRequest](#atomix-runtime-set-v1-ContainsRequest)
    - [ContainsResponse](#atomix-runtime-set-v1-ContainsResponse)
    - [CreateRequest](#atomix-runtime-set-v1-CreateRequest)
    - [CreateResponse](#atomix-runtime-set-v1-CreateResponse)
    - [Element](#atomix-runtime-set-v1-Element)
    - [ElementsRequest](#atomix-runtime-set-v1-ElementsRequest)
    - [ElementsResponse](#atomix-runtime-set-v1-ElementsResponse)
    - [Event](#atomix-runtime-set-v1-Event)
    - [Event.Added](#atomix-runtime-set-v1-Event-Added)
    - [Event.Removed](#atomix-runtime-set-v1-Event-Removed)
    - [EventsRequest](#atomix-runtime-set-v1-EventsRequest)
    - [EventsResponse](#atomix-runtime-set-v1-EventsResponse)
    - [RemoveRequest](#atomix-runtime-set-v1-RemoveRequest)
    - [RemoveResponse](#atomix-runtime-set-v1-RemoveResponse)
    - [SizeRequest](#atomix-runtime-set-v1-SizeRequest)
    - [SizeResponse](#atomix-runtime-set-v1-SizeResponse)
  
    - [Set](#atomix-runtime-set-v1-Set)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_runtime_set_v1_set-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/runtime/set/v1/set.proto



<a name="atomix-runtime-set-v1-AddRequest"></a>

### AddRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| element | [Element](#atomix-runtime-set-v1-Element) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |






<a name="atomix-runtime-set-v1-AddResponse"></a>

### AddResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-runtime-set-v1-Element) |  |  |






<a name="atomix-runtime-set-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-set-v1-ClearResponse"></a>

### ClearResponse







<a name="atomix-runtime-set-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-set-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-runtime-set-v1-ContainsRequest"></a>

### ContainsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| element | [Element](#atomix-runtime-set-v1-Element) |  |  |






<a name="atomix-runtime-set-v1-ContainsResponse"></a>

### ContainsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| contains | [bool](#bool) |  |  |






<a name="atomix-runtime-set-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-runtime-set-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-runtime-set-v1-Element"></a>

### Element



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="atomix-runtime-set-v1-ElementsRequest"></a>

### ElementsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| watch | [bool](#bool) |  |  |






<a name="atomix-runtime-set-v1-ElementsResponse"></a>

### ElementsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-runtime-set-v1-Element) |  |  |






<a name="atomix-runtime-set-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| added | [Event.Added](#atomix-runtime-set-v1-Event-Added) |  |  |
| removed | [Event.Removed](#atomix-runtime-set-v1-Event-Removed) |  |  |






<a name="atomix-runtime-set-v1-Event-Added"></a>

### Event.Added



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-runtime-set-v1-Element) |  |  |






<a name="atomix-runtime-set-v1-Event-Removed"></a>

### Event.Removed



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-runtime-set-v1-Element) |  |  |
| expired | [bool](#bool) |  |  |






<a name="atomix-runtime-set-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-set-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-runtime-set-v1-Event) |  |  |






<a name="atomix-runtime-set-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| element | [Element](#atomix-runtime-set-v1-Element) |  |  |






<a name="atomix-runtime-set-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [Element](#atomix-runtime-set-v1-Element) |  |  |






<a name="atomix-runtime-set-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-set-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |





 

 

 


<a name="atomix-runtime-set-v1-Set"></a>

### Set
Set is a service for a set primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-runtime-set-v1-CreateRequest) | [CreateResponse](#atomix-runtime-set-v1-CreateResponse) | Create creates the set |
| Close | [CloseRequest](#atomix-runtime-set-v1-CloseRequest) | [CloseResponse](#atomix-runtime-set-v1-CloseResponse) | Close closes the set |
| Size | [SizeRequest](#atomix-runtime-set-v1-SizeRequest) | [SizeResponse](#atomix-runtime-set-v1-SizeResponse) | Size gets the number of elements in the set |
| Contains | [ContainsRequest](#atomix-runtime-set-v1-ContainsRequest) | [ContainsResponse](#atomix-runtime-set-v1-ContainsResponse) | Contains returns whether the set contains a value |
| Add | [AddRequest](#atomix-runtime-set-v1-AddRequest) | [AddResponse](#atomix-runtime-set-v1-AddResponse) | Add adds a value to the set |
| Remove | [RemoveRequest](#atomix-runtime-set-v1-RemoveRequest) | [RemoveResponse](#atomix-runtime-set-v1-RemoveResponse) | Remove removes a value from the set |
| Clear | [ClearRequest](#atomix-runtime-set-v1-ClearRequest) | [ClearResponse](#atomix-runtime-set-v1-ClearResponse) | Clear removes all values from the set |
| Events | [EventsRequest](#atomix-runtime-set-v1-EventsRequest) | [EventsResponse](#atomix-runtime-set-v1-EventsResponse) stream | Events listens for set change events |
| Elements | [ElementsRequest](#atomix-runtime-set-v1-ElementsRequest) | [ElementsResponse](#atomix-runtime-set-v1-ElementsResponse) stream | Elements lists all elements in the set |

 



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

