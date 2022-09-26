# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/runtime/multimap/v1/multimap.proto](#atomix_runtime_multimap_v1_multimap-proto)
    - [ClearRequest](#atomix-runtime-multimap-v1-ClearRequest)
    - [ClearResponse](#atomix-runtime-multimap-v1-ClearResponse)
    - [CloseRequest](#atomix-runtime-multimap-v1-CloseRequest)
    - [CloseResponse](#atomix-runtime-multimap-v1-CloseResponse)
    - [ContainsRequest](#atomix-runtime-multimap-v1-ContainsRequest)
    - [ContainsResponse](#atomix-runtime-multimap-v1-ContainsResponse)
    - [CreateRequest](#atomix-runtime-multimap-v1-CreateRequest)
    - [CreateRequest.TagsEntry](#atomix-runtime-multimap-v1-CreateRequest-TagsEntry)
    - [CreateResponse](#atomix-runtime-multimap-v1-CreateResponse)
    - [EntriesRequest](#atomix-runtime-multimap-v1-EntriesRequest)
    - [EntriesResponse](#atomix-runtime-multimap-v1-EntriesResponse)
    - [Entry](#atomix-runtime-multimap-v1-Entry)
    - [Event](#atomix-runtime-multimap-v1-Event)
    - [Event.Inserted](#atomix-runtime-multimap-v1-Event-Inserted)
    - [Event.Removed](#atomix-runtime-multimap-v1-Event-Removed)
    - [Event.Updated](#atomix-runtime-multimap-v1-Event-Updated)
    - [EventsRequest](#atomix-runtime-multimap-v1-EventsRequest)
    - [EventsResponse](#atomix-runtime-multimap-v1-EventsResponse)
    - [GetRequest](#atomix-runtime-multimap-v1-GetRequest)
    - [GetResponse](#atomix-runtime-multimap-v1-GetResponse)
    - [PutAllRequest](#atomix-runtime-multimap-v1-PutAllRequest)
    - [PutAllResponse](#atomix-runtime-multimap-v1-PutAllResponse)
    - [PutRequest](#atomix-runtime-multimap-v1-PutRequest)
    - [PutResponse](#atomix-runtime-multimap-v1-PutResponse)
    - [RemoveAllRequest](#atomix-runtime-multimap-v1-RemoveAllRequest)
    - [RemoveAllResponse](#atomix-runtime-multimap-v1-RemoveAllResponse)
    - [RemoveRequest](#atomix-runtime-multimap-v1-RemoveRequest)
    - [RemoveResponse](#atomix-runtime-multimap-v1-RemoveResponse)
    - [ReplaceRequest](#atomix-runtime-multimap-v1-ReplaceRequest)
    - [ReplaceResponse](#atomix-runtime-multimap-v1-ReplaceResponse)
    - [SizeRequest](#atomix-runtime-multimap-v1-SizeRequest)
    - [SizeResponse](#atomix-runtime-multimap-v1-SizeResponse)
  
    - [MultiMap](#atomix-runtime-multimap-v1-MultiMap)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_runtime_multimap_v1_multimap-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/runtime/multimap/v1/multimap.proto



<a name="atomix-runtime-multimap-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-multimap-v1-ClearResponse"></a>

### ClearResponse







<a name="atomix-runtime-multimap-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-multimap-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-runtime-multimap-v1-ContainsRequest"></a>

### ContainsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-multimap-v1-ContainsResponse"></a>

### ContainsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [bool](#bool) |  |  |






<a name="atomix-runtime-multimap-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| tags | [CreateRequest.TagsEntry](#atomix-runtime-multimap-v1-CreateRequest-TagsEntry) | repeated |  |






<a name="atomix-runtime-multimap-v1-CreateRequest-TagsEntry"></a>

### CreateRequest.TagsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-multimap-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-runtime-multimap-v1-EntriesRequest"></a>

### EntriesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| watch | [bool](#bool) |  |  |






<a name="atomix-runtime-multimap-v1-EntriesResponse"></a>

### EntriesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-multimap-v1-Entry) |  |  |






<a name="atomix-runtime-multimap-v1-Entry"></a>

### Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| inserted | [Event.Inserted](#atomix-runtime-multimap-v1-Event-Inserted) |  |  |
| updated | [Event.Updated](#atomix-runtime-multimap-v1-Event-Updated) |  |  |
| removed | [Event.Removed](#atomix-runtime-multimap-v1-Event-Removed) |  |  |






<a name="atomix-runtime-multimap-v1-Event-Inserted"></a>

### Event.Inserted



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-Event-Removed"></a>

### Event.Removed



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-Event-Updated"></a>

### Event.Updated



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| values | [string](#string) | repeated |  |
| prev_values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |






<a name="atomix-runtime-multimap-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-runtime-multimap-v1-Event) |  |  |






<a name="atomix-runtime-multimap-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |






<a name="atomix-runtime-multimap-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-PutAllRequest"></a>

### PutAllRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-PutAllResponse"></a>

### PutAllResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updated | [bool](#bool) |  |  |






<a name="atomix-runtime-multimap-v1-PutRequest"></a>

### PutRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-multimap-v1-PutResponse"></a>

### PutResponse







<a name="atomix-runtime-multimap-v1-RemoveAllRequest"></a>

### RemoveAllRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |






<a name="atomix-runtime-multimap-v1-RemoveAllResponse"></a>

### RemoveAllResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-multimap-v1-RemoveResponse"></a>

### RemoveResponse







<a name="atomix-runtime-multimap-v1-ReplaceRequest"></a>

### ReplaceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-ReplaceResponse"></a>

### ReplaceResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prev_values | [string](#string) | repeated |  |






<a name="atomix-runtime-multimap-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-multimap-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |





 

 

 


<a name="atomix-runtime-multimap-v1-MultiMap"></a>

### MultiMap
MultiMap is a service for a multimap primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-runtime-multimap-v1-CreateRequest) | [CreateResponse](#atomix-runtime-multimap-v1-CreateResponse) | Create creates the map |
| Close | [CloseRequest](#atomix-runtime-multimap-v1-CloseRequest) | [CloseResponse](#atomix-runtime-multimap-v1-CloseResponse) | Close closes the map |
| Size | [SizeRequest](#atomix-runtime-multimap-v1-SizeRequest) | [SizeResponse](#atomix-runtime-multimap-v1-SizeResponse) | Size returns the size of the map |
| Put | [PutRequest](#atomix-runtime-multimap-v1-PutRequest) | [PutResponse](#atomix-runtime-multimap-v1-PutResponse) | Put adds a value to an entry in the map |
| PutAll | [PutAllRequest](#atomix-runtime-multimap-v1-PutAllRequest) | [PutAllResponse](#atomix-runtime-multimap-v1-PutAllResponse) | PutAll adds values to an entry in the map |
| Replace | [ReplaceRequest](#atomix-runtime-multimap-v1-ReplaceRequest) | [ReplaceResponse](#atomix-runtime-multimap-v1-ReplaceResponse) | Replace replaces the values of an entry in the map |
| Contains | [ContainsRequest](#atomix-runtime-multimap-v1-ContainsRequest) | [ContainsResponse](#atomix-runtime-multimap-v1-ContainsResponse) | Contains checks if an entry exists in the map |
| Get | [GetRequest](#atomix-runtime-multimap-v1-GetRequest) | [GetResponse](#atomix-runtime-multimap-v1-GetResponse) | Get gets the entry for a key |
| Remove | [RemoveRequest](#atomix-runtime-multimap-v1-RemoveRequest) | [RemoveResponse](#atomix-runtime-multimap-v1-RemoveResponse) | Remove removes an entry from the map |
| RemoveAll | [RemoveAllRequest](#atomix-runtime-multimap-v1-RemoveAllRequest) | [RemoveAllResponse](#atomix-runtime-multimap-v1-RemoveAllResponse) | RemoveAll removes a key from the map |
| Clear | [ClearRequest](#atomix-runtime-multimap-v1-ClearRequest) | [ClearResponse](#atomix-runtime-multimap-v1-ClearResponse) | Clear removes all entries from the map |
| Events | [EventsRequest](#atomix-runtime-multimap-v1-EventsRequest) | [EventsResponse](#atomix-runtime-multimap-v1-EventsResponse) stream | Events listens for change events |
| Entries | [EntriesRequest](#atomix-runtime-multimap-v1-EntriesRequest) | [EntriesResponse](#atomix-runtime-multimap-v1-EntriesResponse) stream | Entries lists all entries in the map |

 



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

