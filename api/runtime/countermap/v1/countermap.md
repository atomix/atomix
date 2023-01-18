# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [runtime/countermap/v1/countermap.proto](#runtime_countermap_v1_countermap-proto)
    - [ClearRequest](#atomix-runtime-countermap-v1-ClearRequest)
    - [ClearResponse](#atomix-runtime-countermap-v1-ClearResponse)
    - [CloseRequest](#atomix-runtime-countermap-v1-CloseRequest)
    - [CloseResponse](#atomix-runtime-countermap-v1-CloseResponse)
    - [CounterMapConfig](#atomix-runtime-countermap-v1-CounterMapConfig)
    - [CreateRequest](#atomix-runtime-countermap-v1-CreateRequest)
    - [CreateResponse](#atomix-runtime-countermap-v1-CreateResponse)
    - [DecrementRequest](#atomix-runtime-countermap-v1-DecrementRequest)
    - [DecrementResponse](#atomix-runtime-countermap-v1-DecrementResponse)
    - [EntriesRequest](#atomix-runtime-countermap-v1-EntriesRequest)
    - [EntriesResponse](#atomix-runtime-countermap-v1-EntriesResponse)
    - [Entry](#atomix-runtime-countermap-v1-Entry)
    - [Event](#atomix-runtime-countermap-v1-Event)
    - [Event.Inserted](#atomix-runtime-countermap-v1-Event-Inserted)
    - [Event.Removed](#atomix-runtime-countermap-v1-Event-Removed)
    - [Event.Updated](#atomix-runtime-countermap-v1-Event-Updated)
    - [EventsRequest](#atomix-runtime-countermap-v1-EventsRequest)
    - [EventsResponse](#atomix-runtime-countermap-v1-EventsResponse)
    - [GetRequest](#atomix-runtime-countermap-v1-GetRequest)
    - [GetResponse](#atomix-runtime-countermap-v1-GetResponse)
    - [IncrementRequest](#atomix-runtime-countermap-v1-IncrementRequest)
    - [IncrementResponse](#atomix-runtime-countermap-v1-IncrementResponse)
    - [InsertRequest](#atomix-runtime-countermap-v1-InsertRequest)
    - [InsertResponse](#atomix-runtime-countermap-v1-InsertResponse)
    - [LockRequest](#atomix-runtime-countermap-v1-LockRequest)
    - [LockResponse](#atomix-runtime-countermap-v1-LockResponse)
    - [RemoveRequest](#atomix-runtime-countermap-v1-RemoveRequest)
    - [RemoveResponse](#atomix-runtime-countermap-v1-RemoveResponse)
    - [SetRequest](#atomix-runtime-countermap-v1-SetRequest)
    - [SetResponse](#atomix-runtime-countermap-v1-SetResponse)
    - [SizeRequest](#atomix-runtime-countermap-v1-SizeRequest)
    - [SizeResponse](#atomix-runtime-countermap-v1-SizeResponse)
    - [UnlockRequest](#atomix-runtime-countermap-v1-UnlockRequest)
    - [UnlockResponse](#atomix-runtime-countermap-v1-UnlockResponse)
    - [UpdateRequest](#atomix-runtime-countermap-v1-UpdateRequest)
    - [UpdateResponse](#atomix-runtime-countermap-v1-UpdateResponse)
  
    - [CounterMap](#atomix-runtime-countermap-v1-CounterMap)
  
- [Scalar Value Types](#scalar-value-types)



<a name="runtime_countermap_v1_countermap-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## runtime/countermap/v1/countermap.proto



<a name="atomix-runtime-countermap-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |






<a name="atomix-runtime-countermap-v1-ClearResponse"></a>

### ClearResponse







<a name="atomix-runtime-countermap-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |






<a name="atomix-runtime-countermap-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-runtime-countermap-v1-CounterMapConfig"></a>

### CounterMapConfig







<a name="atomix-runtime-countermap-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-runtime-countermap-v1-CreateResponse"></a>

### CreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| config | [CounterMapConfig](#atomix-runtime-countermap-v1-CounterMapConfig) |  |  |






<a name="atomix-runtime-countermap-v1-DecrementRequest"></a>

### DecrementRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| key | [string](#string) |  |  |
| delta | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-DecrementResponse"></a>

### DecrementResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prev_value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-EntriesRequest"></a>

### EntriesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| watch | [bool](#bool) |  |  |






<a name="atomix-runtime-countermap-v1-EntriesResponse"></a>

### EntriesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-runtime-countermap-v1-Entry) |  |  |






<a name="atomix-runtime-countermap-v1-Entry"></a>

### Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| inserted | [Event.Inserted](#atomix-runtime-countermap-v1-Event-Inserted) |  |  |
| updated | [Event.Updated](#atomix-runtime-countermap-v1-Event-Updated) |  |  |
| removed | [Event.Removed](#atomix-runtime-countermap-v1-Event-Removed) |  |  |






<a name="atomix-runtime-countermap-v1-Event-Inserted"></a>

### Event.Inserted



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-Event-Removed"></a>

### Event.Removed



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-Event-Updated"></a>

### Event.Updated



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |
| prev_value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| key | [string](#string) |  |  |






<a name="atomix-runtime-countermap-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-runtime-countermap-v1-Event) |  |  |






<a name="atomix-runtime-countermap-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| key | [string](#string) |  |  |






<a name="atomix-runtime-countermap-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-IncrementRequest"></a>

### IncrementRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| key | [string](#string) |  |  |
| delta | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-IncrementResponse"></a>

### IncrementResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prev_value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-InsertRequest"></a>

### InsertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| key | [string](#string) |  |  |
| value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-InsertResponse"></a>

### InsertResponse







<a name="atomix-runtime-countermap-v1-LockRequest"></a>

### LockRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| keys | [string](#string) | repeated |  |
| timeout | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |






<a name="atomix-runtime-countermap-v1-LockResponse"></a>

### LockResponse







<a name="atomix-runtime-countermap-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| key | [string](#string) |  |  |
| prev_value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| key | [string](#string) |  |  |
| value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-SetResponse"></a>

### SetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prev_value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |






<a name="atomix-runtime-countermap-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-runtime-countermap-v1-UnlockRequest"></a>

### UnlockRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| keys | [string](#string) | repeated |  |






<a name="atomix-runtime-countermap-v1-UnlockResponse"></a>

### UnlockResponse







<a name="atomix-runtime-countermap-v1-UpdateRequest"></a>

### UpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| key | [string](#string) |  |  |
| value | [int64](#int64) |  |  |
| prev_value | [int64](#int64) |  |  |






<a name="atomix-runtime-countermap-v1-UpdateResponse"></a>

### UpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prev_value | [int64](#int64) |  |  |





 

 

 


<a name="atomix-runtime-countermap-v1-CounterMap"></a>

### CounterMap
CounterMap is a service for a counter map primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-runtime-countermap-v1-CreateRequest) | [CreateResponse](#atomix-runtime-countermap-v1-CreateResponse) | Create creates the map |
| Close | [CloseRequest](#atomix-runtime-countermap-v1-CloseRequest) | [CloseResponse](#atomix-runtime-countermap-v1-CloseResponse) | Close closes the map |
| Size | [SizeRequest](#atomix-runtime-countermap-v1-SizeRequest) | [SizeResponse](#atomix-runtime-countermap-v1-SizeResponse) | Size returns the size of the map |
| Set | [SetRequest](#atomix-runtime-countermap-v1-SetRequest) | [SetResponse](#atomix-runtime-countermap-v1-SetResponse) | Set sets an entry into the map |
| Insert | [InsertRequest](#atomix-runtime-countermap-v1-InsertRequest) | [InsertResponse](#atomix-runtime-countermap-v1-InsertResponse) | Insert inserts an entry into the map |
| Update | [UpdateRequest](#atomix-runtime-countermap-v1-UpdateRequest) | [UpdateResponse](#atomix-runtime-countermap-v1-UpdateResponse) | Update updates an entry in the map |
| Increment | [IncrementRequest](#atomix-runtime-countermap-v1-IncrementRequest) | [IncrementResponse](#atomix-runtime-countermap-v1-IncrementResponse) | Increment increments a counter in the map |
| Decrement | [DecrementRequest](#atomix-runtime-countermap-v1-DecrementRequest) | [DecrementResponse](#atomix-runtime-countermap-v1-DecrementResponse) | Decrement decrements a counter in the map |
| Get | [GetRequest](#atomix-runtime-countermap-v1-GetRequest) | [GetResponse](#atomix-runtime-countermap-v1-GetResponse) | Get gets the entry for a key |
| Remove | [RemoveRequest](#atomix-runtime-countermap-v1-RemoveRequest) | [RemoveResponse](#atomix-runtime-countermap-v1-RemoveResponse) | Remove removes an entry from the map |
| Clear | [ClearRequest](#atomix-runtime-countermap-v1-ClearRequest) | [ClearResponse](#atomix-runtime-countermap-v1-ClearResponse) | Clear removes all entries from the map |
| Lock | [LockRequest](#atomix-runtime-countermap-v1-LockRequest) | [LockResponse](#atomix-runtime-countermap-v1-LockResponse) | Lock locks a key in the map |
| Unlock | [UnlockRequest](#atomix-runtime-countermap-v1-UnlockRequest) | [UnlockResponse](#atomix-runtime-countermap-v1-UnlockResponse) | Unlock unlocks a key in the map |
| Events | [EventsRequest](#atomix-runtime-countermap-v1-EventsRequest) | [EventsResponse](#atomix-runtime-countermap-v1-EventsResponse) stream | Events listens for change events |
| Entries | [EntriesRequest](#atomix-runtime-countermap-v1-EntriesRequest) | [EntriesResponse](#atomix-runtime-countermap-v1-EntriesResponse) stream | Entries lists all entries in the map |

 



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

