# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/indexedmap/v1/indexedmap.proto](#atomix_indexedmap_v1_indexedmap-proto)
    - [AppendRequest](#atomix-indexedmap-v1-AppendRequest)
    - [AppendResponse](#atomix-indexedmap-v1-AppendResponse)
    - [ClearRequest](#atomix-indexedmap-v1-ClearRequest)
    - [ClearResponse](#atomix-indexedmap-v1-ClearResponse)
    - [CloseRequest](#atomix-indexedmap-v1-CloseRequest)
    - [CloseResponse](#atomix-indexedmap-v1-CloseResponse)
    - [CreateRequest](#atomix-indexedmap-v1-CreateRequest)
    - [CreateResponse](#atomix-indexedmap-v1-CreateResponse)
    - [EntriesRequest](#atomix-indexedmap-v1-EntriesRequest)
    - [EntriesResponse](#atomix-indexedmap-v1-EntriesResponse)
    - [Entry](#atomix-indexedmap-v1-Entry)
    - [Event](#atomix-indexedmap-v1-Event)
    - [Event.Inserted](#atomix-indexedmap-v1-Event-Inserted)
    - [Event.Removed](#atomix-indexedmap-v1-Event-Removed)
    - [Event.Updated](#atomix-indexedmap-v1-Event-Updated)
    - [EventsRequest](#atomix-indexedmap-v1-EventsRequest)
    - [EventsResponse](#atomix-indexedmap-v1-EventsResponse)
    - [FirstEntryRequest](#atomix-indexedmap-v1-FirstEntryRequest)
    - [FirstEntryResponse](#atomix-indexedmap-v1-FirstEntryResponse)
    - [GetRequest](#atomix-indexedmap-v1-GetRequest)
    - [GetResponse](#atomix-indexedmap-v1-GetResponse)
    - [LastEntryRequest](#atomix-indexedmap-v1-LastEntryRequest)
    - [LastEntryResponse](#atomix-indexedmap-v1-LastEntryResponse)
    - [NextEntryRequest](#atomix-indexedmap-v1-NextEntryRequest)
    - [NextEntryResponse](#atomix-indexedmap-v1-NextEntryResponse)
    - [PrevEntryRequest](#atomix-indexedmap-v1-PrevEntryRequest)
    - [PrevEntryResponse](#atomix-indexedmap-v1-PrevEntryResponse)
    - [RemoveRequest](#atomix-indexedmap-v1-RemoveRequest)
    - [RemoveResponse](#atomix-indexedmap-v1-RemoveResponse)
    - [SizeRequest](#atomix-indexedmap-v1-SizeRequest)
    - [SizeResponse](#atomix-indexedmap-v1-SizeResponse)
    - [UpdateRequest](#atomix-indexedmap-v1-UpdateRequest)
    - [UpdateResponse](#atomix-indexedmap-v1-UpdateResponse)
    - [VersionedValue](#atomix-indexedmap-v1-VersionedValue)
  
    - [IndexedMap](#atomix-indexedmap-v1-IndexedMap)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_indexedmap_v1_indexedmap-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/indexedmap/v1/indexedmap.proto



<a name="atomix-indexedmap-v1-AppendRequest"></a>

### AppendRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| value | [bytes](#bytes) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |






<a name="atomix-indexedmap-v1-AppendResponse"></a>

### AppendResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-indexedmap-v1-ClearResponse"></a>

### ClearResponse







<a name="atomix-indexedmap-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-indexedmap-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-indexedmap-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-indexedmap-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-indexedmap-v1-EntriesRequest"></a>

### EntriesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| watch | [bool](#bool) |  |  |






<a name="atomix-indexedmap-v1-EntriesResponse"></a>

### EntriesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-Entry"></a>

### Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |
| value | [VersionedValue](#atomix-indexedmap-v1-VersionedValue) |  |  |






<a name="atomix-indexedmap-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |
| inserted | [Event.Inserted](#atomix-indexedmap-v1-Event-Inserted) |  |  |
| updated | [Event.Updated](#atomix-indexedmap-v1-Event-Updated) |  |  |
| removed | [Event.Removed](#atomix-indexedmap-v1-Event-Removed) |  |  |






<a name="atomix-indexedmap-v1-Event-Inserted"></a>

### Event.Inserted



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-indexedmap-v1-VersionedValue) |  |  |






<a name="atomix-indexedmap-v1-Event-Removed"></a>

### Event.Removed



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-indexedmap-v1-VersionedValue) |  |  |
| expired | [bool](#bool) |  |  |






<a name="atomix-indexedmap-v1-Event-Updated"></a>

### Event.Updated



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [VersionedValue](#atomix-indexedmap-v1-VersionedValue) |  |  |
| prev_value | [VersionedValue](#atomix-indexedmap-v1-VersionedValue) |  |  |






<a name="atomix-indexedmap-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |






<a name="atomix-indexedmap-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-indexedmap-v1-Event) |  |  |






<a name="atomix-indexedmap-v1-FirstEntryRequest"></a>

### FirstEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-indexedmap-v1-FirstEntryResponse"></a>

### FirstEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |






<a name="atomix-indexedmap-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-LastEntryRequest"></a>

### LastEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-indexedmap-v1-LastEntryResponse"></a>

### LastEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-NextEntryRequest"></a>

### NextEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| index | [uint64](#uint64) |  |  |






<a name="atomix-indexedmap-v1-NextEntryResponse"></a>

### NextEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-PrevEntryRequest"></a>

### PrevEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| index | [uint64](#uint64) |  |  |






<a name="atomix-indexedmap-v1-PrevEntryResponse"></a>

### PrevEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |
| prev_version | [uint64](#uint64) |  |  |






<a name="atomix-indexedmap-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-indexedmap-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-indexedmap-v1-UpdateRequest"></a>

### UpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| key | [string](#string) |  |  |
| index | [uint64](#uint64) |  |  |
| value | [bytes](#bytes) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |
| prev_version | [uint64](#uint64) |  |  |






<a name="atomix-indexedmap-v1-UpdateResponse"></a>

### UpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-indexedmap-v1-Entry) |  |  |






<a name="atomix-indexedmap-v1-VersionedValue"></a>

### VersionedValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |
| version | [uint64](#uint64) |  |  |





 

 

 


<a name="atomix-indexedmap-v1-IndexedMap"></a>

### IndexedMap
IndexedMap is a service for a sorted/indexed map primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-indexedmap-v1-CreateRequest) | [CreateResponse](#atomix-indexedmap-v1-CreateResponse) | Create creates the map |
| Close | [CloseRequest](#atomix-indexedmap-v1-CloseRequest) | [CloseResponse](#atomix-indexedmap-v1-CloseResponse) | Close closes the map |
| Size | [SizeRequest](#atomix-indexedmap-v1-SizeRequest) | [SizeResponse](#atomix-indexedmap-v1-SizeResponse) | Size returns the size of the map |
| Append | [AppendRequest](#atomix-indexedmap-v1-AppendRequest) | [AppendResponse](#atomix-indexedmap-v1-AppendResponse) | Append appends an entry to the map |
| Update | [UpdateRequest](#atomix-indexedmap-v1-UpdateRequest) | [UpdateResponse](#atomix-indexedmap-v1-UpdateResponse) | Update updates an entry in the map |
| Get | [GetRequest](#atomix-indexedmap-v1-GetRequest) | [GetResponse](#atomix-indexedmap-v1-GetResponse) | Get gets the entry for a key |
| FirstEntry | [FirstEntryRequest](#atomix-indexedmap-v1-FirstEntryRequest) | [FirstEntryResponse](#atomix-indexedmap-v1-FirstEntryResponse) | FirstEntry gets the first entry in the map |
| LastEntry | [LastEntryRequest](#atomix-indexedmap-v1-LastEntryRequest) | [LastEntryResponse](#atomix-indexedmap-v1-LastEntryResponse) | LastEntry gets the last entry in the map |
| PrevEntry | [PrevEntryRequest](#atomix-indexedmap-v1-PrevEntryRequest) | [PrevEntryResponse](#atomix-indexedmap-v1-PrevEntryResponse) | PrevEntry gets the previous entry in the map |
| NextEntry | [NextEntryRequest](#atomix-indexedmap-v1-NextEntryRequest) | [NextEntryResponse](#atomix-indexedmap-v1-NextEntryResponse) | NextEntry gets the next entry in the map |
| Remove | [RemoveRequest](#atomix-indexedmap-v1-RemoveRequest) | [RemoveResponse](#atomix-indexedmap-v1-RemoveResponse) | Remove removes an entry from the map |
| Clear | [ClearRequest](#atomix-indexedmap-v1-ClearRequest) | [ClearResponse](#atomix-indexedmap-v1-ClearResponse) | Clear removes all entries from the map |
| Events | [EventsRequest](#atomix-indexedmap-v1-EventsRequest) | [EventsResponse](#atomix-indexedmap-v1-EventsResponse) stream | Events listens for change events |
| Entries | [EntriesRequest](#atomix-indexedmap-v1-EntriesRequest) | [EntriesResponse](#atomix-indexedmap-v1-EntriesResponse) stream | Entries lists all entries in the map |

 



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

