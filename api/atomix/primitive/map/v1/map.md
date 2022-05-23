# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/primitive/map/v1/map.proto](#atomix_primitive_map_v1_map-proto)
    - [ClearInput](#atomix-primitive-map-v1-ClearInput)
    - [ClearOutput](#atomix-primitive-map-v1-ClearOutput)
    - [ClearRequest](#atomix-primitive-map-v1-ClearRequest)
    - [ClearResponse](#atomix-primitive-map-v1-ClearResponse)
    - [EntriesInput](#atomix-primitive-map-v1-EntriesInput)
    - [EntriesOutput](#atomix-primitive-map-v1-EntriesOutput)
    - [EntriesRequest](#atomix-primitive-map-v1-EntriesRequest)
    - [EntriesResponse](#atomix-primitive-map-v1-EntriesResponse)
    - [Entry](#atomix-primitive-map-v1-Entry)
    - [Event](#atomix-primitive-map-v1-Event)
    - [EventsInput](#atomix-primitive-map-v1-EventsInput)
    - [EventsOutput](#atomix-primitive-map-v1-EventsOutput)
    - [EventsRequest](#atomix-primitive-map-v1-EventsRequest)
    - [EventsResponse](#atomix-primitive-map-v1-EventsResponse)
    - [GetInput](#atomix-primitive-map-v1-GetInput)
    - [GetOutput](#atomix-primitive-map-v1-GetOutput)
    - [GetRequest](#atomix-primitive-map-v1-GetRequest)
    - [GetResponse](#atomix-primitive-map-v1-GetResponse)
    - [MapConfig](#atomix-primitive-map-v1-MapConfig)
    - [PutInput](#atomix-primitive-map-v1-PutInput)
    - [PutOutput](#atomix-primitive-map-v1-PutOutput)
    - [PutRequest](#atomix-primitive-map-v1-PutRequest)
    - [PutResponse](#atomix-primitive-map-v1-PutResponse)
    - [RemoveInput](#atomix-primitive-map-v1-RemoveInput)
    - [RemoveOutput](#atomix-primitive-map-v1-RemoveOutput)
    - [RemoveRequest](#atomix-primitive-map-v1-RemoveRequest)
    - [RemoveResponse](#atomix-primitive-map-v1-RemoveResponse)
    - [SizeInput](#atomix-primitive-map-v1-SizeInput)
    - [SizeOutput](#atomix-primitive-map-v1-SizeOutput)
    - [SizeRequest](#atomix-primitive-map-v1-SizeRequest)
    - [SizeResponse](#atomix-primitive-map-v1-SizeResponse)
    - [Value](#atomix-primitive-map-v1-Value)
  
    - [Event.Type](#atomix-primitive-map-v1-Event-Type)
  
    - [Map](#atomix-primitive-map-v1-Map)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_primitive_map_v1_map-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/primitive/map/v1/map.proto



<a name="atomix-primitive-map-v1-ClearInput"></a>

### ClearInput







<a name="atomix-primitive-map-v1-ClearOutput"></a>

### ClearOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [atomix.primitive.meta.v1.Timestamp](#atomix-primitive-meta-v1-Timestamp) |  |  |






<a name="atomix-primitive-map-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [ClearInput](#atomix-primitive-map-v1-ClearInput) |  |  |






<a name="atomix-primitive-map-v1-ClearResponse"></a>

### ClearResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [ClearOutput](#atomix-primitive-map-v1-ClearOutput) |  |  |






<a name="atomix-primitive-map-v1-EntriesInput"></a>

### EntriesInput







<a name="atomix-primitive-map-v1-EntriesOutput"></a>

### EntriesOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-primitive-map-v1-Entry) |  |  |






<a name="atomix-primitive-map-v1-EntriesRequest"></a>

### EntriesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [EntriesInput](#atomix-primitive-map-v1-EntriesInput) |  |  |






<a name="atomix-primitive-map-v1-EntriesResponse"></a>

### EntriesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [EntriesOutput](#atomix-primitive-map-v1-EntriesOutput) |  |  |






<a name="atomix-primitive-map-v1-Entry"></a>

### Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [Value](#atomix-primitive-map-v1-Value) |  |  |
| timestamp | [atomix.primitive.meta.v1.Timestamp](#atomix-primitive-meta-v1-Timestamp) |  |  |






<a name="atomix-primitive-map-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#atomix-primitive-map-v1-Event-Type) |  |  |
| entry | [Entry](#atomix-primitive-map-v1-Entry) |  |  |






<a name="atomix-primitive-map-v1-EventsInput"></a>

### EventsInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| replay | [bool](#bool) |  |  |






<a name="atomix-primitive-map-v1-EventsOutput"></a>

### EventsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-primitive-map-v1-Event) |  |  |






<a name="atomix-primitive-map-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [EventsInput](#atomix-primitive-map-v1-EventsInput) |  |  |






<a name="atomix-primitive-map-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [EventsOutput](#atomix-primitive-map-v1-EventsOutput) |  |  |






<a name="atomix-primitive-map-v1-GetInput"></a>

### GetInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |






<a name="atomix-primitive-map-v1-GetOutput"></a>

### GetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-primitive-map-v1-Entry) |  |  |






<a name="atomix-primitive-map-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [GetInput](#atomix-primitive-map-v1-GetInput) |  |  |






<a name="atomix-primitive-map-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [GetOutput](#atomix-primitive-map-v1-GetOutput) |  |  |






<a name="atomix-primitive-map-v1-MapConfig"></a>

### MapConfig







<a name="atomix-primitive-map-v1-PutInput"></a>

### PutInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [Value](#atomix-primitive-map-v1-Value) |  |  |
| timestamp | [atomix.primitive.meta.v1.Timestamp](#atomix-primitive-meta-v1-Timestamp) |  |  |






<a name="atomix-primitive-map-v1-PutOutput"></a>

### PutOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-primitive-map-v1-Entry) |  |  |






<a name="atomix-primitive-map-v1-PutRequest"></a>

### PutRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [PutInput](#atomix-primitive-map-v1-PutInput) |  |  |






<a name="atomix-primitive-map-v1-PutResponse"></a>

### PutResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [PutOutput](#atomix-primitive-map-v1-PutOutput) |  |  |






<a name="atomix-primitive-map-v1-RemoveInput"></a>

### RemoveInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| timestamp | [atomix.primitive.meta.v1.Timestamp](#atomix-primitive-meta-v1-Timestamp) |  |  |






<a name="atomix-primitive-map-v1-RemoveOutput"></a>

### RemoveOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-primitive-map-v1-Entry) |  |  |






<a name="atomix-primitive-map-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [RemoveInput](#atomix-primitive-map-v1-RemoveInput) |  |  |






<a name="atomix-primitive-map-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [RemoveOutput](#atomix-primitive-map-v1-RemoveOutput) |  |  |






<a name="atomix-primitive-map-v1-SizeInput"></a>

### SizeInput







<a name="atomix-primitive-map-v1-SizeOutput"></a>

### SizeOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-primitive-map-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [SizeInput](#atomix-primitive-map-v1-SizeInput) |  |  |






<a name="atomix-primitive-map-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [SizeOutput](#atomix-primitive-map-v1-SizeOutput) |  |  |






<a name="atomix-primitive-map-v1-Value"></a>

### Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |





 


<a name="atomix-primitive-map-v1-Event-Type"></a>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| INSERT | 1 |  |
| UPDATE | 2 |  |
| REMOVE | 3 |  |
| REPLAY | 4 |  |


 

 


<a name="atomix-primitive-map-v1-Map"></a>

### Map
Map is a service for a map primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Size | [SizeRequest](#atomix-primitive-map-v1-SizeRequest) | [SizeResponse](#atomix-primitive-map-v1-SizeResponse) | Size returns the size of the map |
| Put | [PutRequest](#atomix-primitive-map-v1-PutRequest) | [PutResponse](#atomix-primitive-map-v1-PutResponse) | Put puts an entry into the map |
| Get | [GetRequest](#atomix-primitive-map-v1-GetRequest) | [GetResponse](#atomix-primitive-map-v1-GetResponse) | Get gets the entry for a key |
| Remove | [RemoveRequest](#atomix-primitive-map-v1-RemoveRequest) | [RemoveResponse](#atomix-primitive-map-v1-RemoveResponse) | Remove removes an entry from the map |
| Clear | [ClearRequest](#atomix-primitive-map-v1-ClearRequest) | [ClearResponse](#atomix-primitive-map-v1-ClearResponse) | Clear removes all entries from the map |
| Events | [EventsRequest](#atomix-primitive-map-v1-EventsRequest) | [EventsResponse](#atomix-primitive-map-v1-EventsResponse) stream | Events listens for change events |
| Entries | [EntriesRequest](#atomix-primitive-map-v1-EntriesRequest) | [EntriesResponse](#atomix-primitive-map-v1-EntriesResponse) stream | Entries lists all entries in the map |

 



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

