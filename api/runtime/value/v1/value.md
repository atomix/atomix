# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [runtime/value/v1/value.proto](#runtime_value_v1_value-proto)
    - [CloseRequest](#atomix-runtime-value-v1-CloseRequest)
    - [CloseResponse](#atomix-runtime-value-v1-CloseResponse)
    - [CreateRequest](#atomix-runtime-value-v1-CreateRequest)
    - [CreateResponse](#atomix-runtime-value-v1-CreateResponse)
    - [DeleteRequest](#atomix-runtime-value-v1-DeleteRequest)
    - [DeleteResponse](#atomix-runtime-value-v1-DeleteResponse)
    - [Event](#atomix-runtime-value-v1-Event)
    - [Event.Created](#atomix-runtime-value-v1-Event-Created)
    - [Event.Deleted](#atomix-runtime-value-v1-Event-Deleted)
    - [Event.Updated](#atomix-runtime-value-v1-Event-Updated)
    - [EventsRequest](#atomix-runtime-value-v1-EventsRequest)
    - [EventsResponse](#atomix-runtime-value-v1-EventsResponse)
    - [GetRequest](#atomix-runtime-value-v1-GetRequest)
    - [GetResponse](#atomix-runtime-value-v1-GetResponse)
    - [InsertRequest](#atomix-runtime-value-v1-InsertRequest)
    - [InsertResponse](#atomix-runtime-value-v1-InsertResponse)
    - [SetRequest](#atomix-runtime-value-v1-SetRequest)
    - [SetResponse](#atomix-runtime-value-v1-SetResponse)
    - [UpdateRequest](#atomix-runtime-value-v1-UpdateRequest)
    - [UpdateResponse](#atomix-runtime-value-v1-UpdateResponse)
    - [VersionedValue](#atomix-runtime-value-v1-VersionedValue)
    - [WatchRequest](#atomix-runtime-value-v1-WatchRequest)
    - [WatchResponse](#atomix-runtime-value-v1-WatchResponse)
  
    - [Value](#atomix-runtime-value-v1-Value)
  
- [Scalar Value Types](#scalar-value-types)



<a name="runtime_value_v1_value-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## runtime/value/v1/value.proto



<a name="atomix-runtime-value-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |






<a name="atomix-runtime-value-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-runtime-value-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-runtime-value-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-runtime-value-v1-DeleteRequest"></a>

### DeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| prev_version | [uint64](#uint64) |  |  |






<a name="atomix-runtime-value-v1-DeleteResponse"></a>

### DeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |






<a name="atomix-runtime-value-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| created | [Event.Created](#atomix-runtime-value-v1-Event-Created) |  |  |
| updated | [Event.Updated](#atomix-runtime-value-v1-Event-Updated) |  |  |
| deleted | [Event.Deleted](#atomix-runtime-value-v1-Event-Deleted) |  |  |






<a name="atomix-runtime-value-v1-Event-Created"></a>

### Event.Created



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |






<a name="atomix-runtime-value-v1-Event-Deleted"></a>

### Event.Deleted



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |
| expired | [bool](#bool) |  |  |






<a name="atomix-runtime-value-v1-Event-Updated"></a>

### Event.Updated



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |
| prev_value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |






<a name="atomix-runtime-value-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |






<a name="atomix-runtime-value-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-runtime-value-v1-Event) |  |  |






<a name="atomix-runtime-value-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |






<a name="atomix-runtime-value-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |






<a name="atomix-runtime-value-v1-InsertRequest"></a>

### InsertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| value | [bytes](#bytes) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |






<a name="atomix-runtime-value-v1-InsertResponse"></a>

### InsertResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  |  |






<a name="atomix-runtime-value-v1-SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| value | [bytes](#bytes) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |






<a name="atomix-runtime-value-v1-SetResponse"></a>

### SetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  |  |
| prev_value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |






<a name="atomix-runtime-value-v1-UpdateRequest"></a>

### UpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| value | [bytes](#bytes) |  |  |
| prev_version | [uint64](#uint64) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |






<a name="atomix-runtime-value-v1-UpdateResponse"></a>

### UpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  |  |
| prev_value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |






<a name="atomix-runtime-value-v1-VersionedValue"></a>

### VersionedValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |
| version | [uint64](#uint64) |  |  |






<a name="atomix-runtime-value-v1-WatchRequest"></a>

### WatchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |






<a name="atomix-runtime-value-v1-WatchResponse"></a>

### WatchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-runtime-value-v1-VersionedValue) |  |  |





 

 

 


<a name="atomix-runtime-value-v1-Value"></a>

### Value
Value is a service for a value primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-runtime-value-v1-CreateRequest) | [CreateResponse](#atomix-runtime-value-v1-CreateResponse) | Create creates the value |
| Close | [CloseRequest](#atomix-runtime-value-v1-CloseRequest) | [CloseResponse](#atomix-runtime-value-v1-CloseResponse) | Close closes the value |
| Set | [SetRequest](#atomix-runtime-value-v1-SetRequest) | [SetResponse](#atomix-runtime-value-v1-SetResponse) | Set sets the value |
| Insert | [InsertRequest](#atomix-runtime-value-v1-InsertRequest) | [InsertResponse](#atomix-runtime-value-v1-InsertResponse) | Insert inserts the value |
| Update | [UpdateRequest](#atomix-runtime-value-v1-UpdateRequest) | [UpdateResponse](#atomix-runtime-value-v1-UpdateResponse) | Update updates the value |
| Get | [GetRequest](#atomix-runtime-value-v1-GetRequest) | [GetResponse](#atomix-runtime-value-v1-GetResponse) | Get gets the value |
| Delete | [DeleteRequest](#atomix-runtime-value-v1-DeleteRequest) | [DeleteResponse](#atomix-runtime-value-v1-DeleteResponse) | Delete deletes the value |
| Watch | [WatchRequest](#atomix-runtime-value-v1-WatchRequest) | [WatchResponse](#atomix-runtime-value-v1-WatchResponse) stream | Watch watches the value |
| Events | [EventsRequest](#atomix-runtime-value-v1-EventsRequest) | [EventsResponse](#atomix-runtime-value-v1-EventsResponse) stream | Events watches for value change events |

 



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

