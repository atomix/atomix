# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/runtime/indexedmap/v1/indexedmap.proto](#atomix_runtime_indexedmap_v1_indexedmap-proto)
    - [AppendRequest](#atomix-runtime-indexedmap-v1-AppendRequest)
    - [AppendResponse](#atomix-runtime-indexedmap-v1-AppendResponse)
    - [ClearRequest](#atomix-runtime-indexedmap-v1-ClearRequest)
    - [ClearResponse](#atomix-runtime-indexedmap-v1-ClearResponse)
    - [CloseRequest](#atomix-runtime-indexedmap-v1-CloseRequest)
    - [CloseResponse](#atomix-runtime-indexedmap-v1-CloseResponse)
    - [CreateRequest](#atomix-runtime-indexedmap-v1-CreateRequest)
    - [CreateRequest.TagsEntry](#atomix-runtime-indexedmap-v1-CreateRequest-TagsEntry)
    - [CreateResponse](#atomix-runtime-indexedmap-v1-CreateResponse)
    - [EntriesRequest](#atomix-runtime-indexedmap-v1-EntriesRequest)
    - [EntriesResponse](#atomix-runtime-indexedmap-v1-EntriesResponse)
    - [Entry](#atomix-runtime-indexedmap-v1-Entry)
    - [Event](#atomix-runtime-indexedmap-v1-Event)
    - [EventsRequest](#atomix-runtime-indexedmap-v1-EventsRequest)
    - [EventsResponse](#atomix-runtime-indexedmap-v1-EventsResponse)
    - [FirstEntryRequest](#atomix-runtime-indexedmap-v1-FirstEntryRequest)
    - [FirstEntryResponse](#atomix-runtime-indexedmap-v1-FirstEntryResponse)
    - [GetRequest](#atomix-runtime-indexedmap-v1-GetRequest)
    - [GetResponse](#atomix-runtime-indexedmap-v1-GetResponse)
    - [LastEntryRequest](#atomix-runtime-indexedmap-v1-LastEntryRequest)
    - [LastEntryResponse](#atomix-runtime-indexedmap-v1-LastEntryResponse)
    - [NextEntryRequest](#atomix-runtime-indexedmap-v1-NextEntryRequest)
    - [NextEntryResponse](#atomix-runtime-indexedmap-v1-NextEntryResponse)
    - [PrevEntryRequest](#atomix-runtime-indexedmap-v1-PrevEntryRequest)
    - [PrevEntryResponse](#atomix-runtime-indexedmap-v1-PrevEntryResponse)
    - [RemoveRequest](#atomix-runtime-indexedmap-v1-RemoveRequest)
    - [RemoveResponse](#atomix-runtime-indexedmap-v1-RemoveResponse)
    - [SizeRequest](#atomix-runtime-indexedmap-v1-SizeRequest)
    - [SizeResponse](#atomix-runtime-indexedmap-v1-SizeResponse)
    - [UpdateRequest](#atomix-runtime-indexedmap-v1-UpdateRequest)
    - [UpdateResponse](#atomix-runtime-indexedmap-v1-UpdateResponse)
    - [Value](#atomix-runtime-indexedmap-v1-Value)
  
    - [Event.Type](#atomix-runtime-indexedmap-v1-Event-Type)
  
    - [IndexedMap](#atomix-runtime-indexedmap-v1-IndexedMap)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_runtime_indexedmap_v1_indexedmap-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/runtime/indexedmap/v1/indexedmap.proto



<a name="atomix-runtime-indexedmap-v1-AppendRequest"></a>

### AppendRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| value | [Value](#atomix-runtime-indexedmap-v1-Value) |  |  |






<a name="atomix-runtime-indexedmap-v1-AppendResponse"></a>

### AppendResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-indexedmap-v1-ClearResponse"></a>

### ClearResponse







<a name="atomix-runtime-indexedmap-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-indexedmap-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-runtime-indexedmap-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| tags | [CreateRequest.TagsEntry](#atomix-runtime-indexedmap-v1-CreateRequest-TagsEntry) | repeated |  |






<a name="atomix-runtime-indexedmap-v1-CreateRequest-TagsEntry"></a>

### CreateRequest.TagsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-indexedmap-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-runtime-indexedmap-v1-EntriesRequest"></a>

### EntriesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-indexedmap-v1-EntriesResponse"></a>

### EntriesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-Entry"></a>

### Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |
| value | [Value](#atomix-runtime-indexedmap-v1-Value) |  |  |
| timestamp | [atomix.runtime.v1.Timestamp](#atomix-runtime-v1-Timestamp) |  |  |






<a name="atomix-runtime-indexedmap-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#atomix-runtime-indexedmap-v1-Event-Type) |  |  |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| replay | [bool](#bool) |  |  |






<a name="atomix-runtime-indexedmap-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-runtime-indexedmap-v1-Event) |  |  |






<a name="atomix-runtime-indexedmap-v1-FirstEntryRequest"></a>

### FirstEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-indexedmap-v1-FirstEntryResponse"></a>

### FirstEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |






<a name="atomix-runtime-indexedmap-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-LastEntryRequest"></a>

### LastEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-indexedmap-v1-LastEntryResponse"></a>

### LastEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-NextEntryRequest"></a>

### NextEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| index | [uint64](#uint64) |  |  |






<a name="atomix-runtime-indexedmap-v1-NextEntryResponse"></a>

### NextEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-PrevEntryRequest"></a>

### PrevEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| index | [uint64](#uint64) |  |  |






<a name="atomix-runtime-indexedmap-v1-PrevEntryResponse"></a>

### PrevEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |
| if_timestamp | [atomix.runtime.v1.Timestamp](#atomix-runtime-v1-Timestamp) |  |  |






<a name="atomix-runtime-indexedmap-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-indexedmap-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-runtime-indexedmap-v1-UpdateRequest"></a>

### UpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |
| value | [Value](#atomix-runtime-indexedmap-v1-Value) |  |  |
| if_timestamp | [atomix.runtime.v1.Timestamp](#atomix-runtime-v1-Timestamp) |  |  |






<a name="atomix-runtime-indexedmap-v1-UpdateResponse"></a>

### UpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-indexedmap-v1-Entry) |  |  |






<a name="atomix-runtime-indexedmap-v1-Value"></a>

### Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |





 


<a name="atomix-runtime-indexedmap-v1-Event-Type"></a>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| INSERT | 1 |  |
| UPDATE | 2 |  |
| REMOVE | 3 |  |
| REPLAY | 4 |  |


 

 


<a name="atomix-runtime-indexedmap-v1-IndexedMap"></a>

### IndexedMap
IndexedMap is a service for a sorted/indexed map primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-runtime-indexedmap-v1-CreateRequest) | [CreateResponse](#atomix-runtime-indexedmap-v1-CreateResponse) | Create creates the map |
| Close | [CloseRequest](#atomix-runtime-indexedmap-v1-CloseRequest) | [CloseResponse](#atomix-runtime-indexedmap-v1-CloseResponse) | Close closes the map |
| Size | [SizeRequest](#atomix-runtime-indexedmap-v1-SizeRequest) | [SizeResponse](#atomix-runtime-indexedmap-v1-SizeResponse) | Size returns the size of the map |
| Append | [AppendRequest](#atomix-runtime-indexedmap-v1-AppendRequest) | [AppendResponse](#atomix-runtime-indexedmap-v1-AppendResponse) | Append appends an entry to the map |
| Update | [UpdateRequest](#atomix-runtime-indexedmap-v1-UpdateRequest) | [UpdateResponse](#atomix-runtime-indexedmap-v1-UpdateResponse) | Update updates an entry in the map |
| Get | [GetRequest](#atomix-runtime-indexedmap-v1-GetRequest) | [GetResponse](#atomix-runtime-indexedmap-v1-GetResponse) | Get gets the entry for a key |
| FirstEntry | [FirstEntryRequest](#atomix-runtime-indexedmap-v1-FirstEntryRequest) | [FirstEntryResponse](#atomix-runtime-indexedmap-v1-FirstEntryResponse) | FirstEntry gets the first entry in the map |
| LastEntry | [LastEntryRequest](#atomix-runtime-indexedmap-v1-LastEntryRequest) | [LastEntryResponse](#atomix-runtime-indexedmap-v1-LastEntryResponse) | LastEntry gets the last entry in the map |
| PrevEntry | [PrevEntryRequest](#atomix-runtime-indexedmap-v1-PrevEntryRequest) | [PrevEntryResponse](#atomix-runtime-indexedmap-v1-PrevEntryResponse) | PrevEntry gets the previous entry in the map |
| NextEntry | [NextEntryRequest](#atomix-runtime-indexedmap-v1-NextEntryRequest) | [NextEntryResponse](#atomix-runtime-indexedmap-v1-NextEntryResponse) | NextEntry gets the next entry in the map |
| Remove | [RemoveRequest](#atomix-runtime-indexedmap-v1-RemoveRequest) | [RemoveResponse](#atomix-runtime-indexedmap-v1-RemoveResponse) | Remove removes an entry from the map |
| Clear | [ClearRequest](#atomix-runtime-indexedmap-v1-ClearRequest) | [ClearResponse](#atomix-runtime-indexedmap-v1-ClearResponse) | Clear removes all entries from the map |
| Events | [EventsRequest](#atomix-runtime-indexedmap-v1-EventsRequest) | [EventsResponse](#atomix-runtime-indexedmap-v1-EventsResponse) stream | Events listens for change events |
| Entries | [EntriesRequest](#atomix-runtime-indexedmap-v1-EntriesRequest) | [EntriesResponse](#atomix-runtime-indexedmap-v1-EntriesResponse) stream | Entries lists all entries in the map |

 



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

