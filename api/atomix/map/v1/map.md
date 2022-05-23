# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/map/v1/map.proto](#atomix_map_v1_map-proto)
    - [ClearInput](#atomix-map-v1-ClearInput)
    - [ClearOutput](#atomix-map-v1-ClearOutput)
    - [ClearRequest](#atomix-map-v1-ClearRequest)
    - [ClearResponse](#atomix-map-v1-ClearResponse)
    - [EntriesInput](#atomix-map-v1-EntriesInput)
    - [EntriesOutput](#atomix-map-v1-EntriesOutput)
    - [EntriesRequest](#atomix-map-v1-EntriesRequest)
    - [EntriesResponse](#atomix-map-v1-EntriesResponse)
    - [Entry](#atomix-map-v1-Entry)
    - [Event](#atomix-map-v1-Event)
    - [EventsInput](#atomix-map-v1-EventsInput)
    - [EventsOutput](#atomix-map-v1-EventsOutput)
    - [EventsRequest](#atomix-map-v1-EventsRequest)
    - [EventsResponse](#atomix-map-v1-EventsResponse)
    - [GetInput](#atomix-map-v1-GetInput)
    - [GetOutput](#atomix-map-v1-GetOutput)
    - [GetRequest](#atomix-map-v1-GetRequest)
    - [GetResponse](#atomix-map-v1-GetResponse)
    - [MapConfig](#atomix-map-v1-MapConfig)
    - [PutInput](#atomix-map-v1-PutInput)
    - [PutOutput](#atomix-map-v1-PutOutput)
    - [PutRequest](#atomix-map-v1-PutRequest)
    - [PutResponse](#atomix-map-v1-PutResponse)
    - [RemoveInput](#atomix-map-v1-RemoveInput)
    - [RemoveOutput](#atomix-map-v1-RemoveOutput)
    - [RemoveRequest](#atomix-map-v1-RemoveRequest)
    - [RemoveResponse](#atomix-map-v1-RemoveResponse)
    - [SizeInput](#atomix-map-v1-SizeInput)
    - [SizeOutput](#atomix-map-v1-SizeOutput)
    - [SizeRequest](#atomix-map-v1-SizeRequest)
    - [SizeResponse](#atomix-map-v1-SizeResponse)
    - [Value](#atomix-map-v1-Value)
  
    - [Event.Type](#atomix-map-v1-Event-Type)
  
    - [Map](#atomix-map-v1-Map)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_map_v1_map-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/map/v1/map.proto



<a name="atomix-map-v1-ClearInput"></a>

### ClearInput







<a name="atomix-map-v1-ClearOutput"></a>

### ClearOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [atomix.primitive.v1.Timestamp](#atomix-primitive-v1-Timestamp) |  |  |






<a name="atomix-map-v1-ClearRequest"></a>

### ClearRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [ClearInput](#atomix-map-v1-ClearInput) |  |  |






<a name="atomix-map-v1-ClearResponse"></a>

### ClearResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [ClearOutput](#atomix-map-v1-ClearOutput) |  |  |






<a name="atomix-map-v1-EntriesInput"></a>

### EntriesInput







<a name="atomix-map-v1-EntriesOutput"></a>

### EntriesOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-map-v1-Entry) |  |  |






<a name="atomix-map-v1-EntriesRequest"></a>

### EntriesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [EntriesInput](#atomix-map-v1-EntriesInput) |  |  |






<a name="atomix-map-v1-EntriesResponse"></a>

### EntriesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [EntriesOutput](#atomix-map-v1-EntriesOutput) |  |  |






<a name="atomix-map-v1-Entry"></a>

### Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [Value](#atomix-map-v1-Value) |  |  |
| timestamp | [atomix.primitive.v1.Timestamp](#atomix-primitive-v1-Timestamp) |  |  |






<a name="atomix-map-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#atomix-map-v1-Event-Type) |  |  |
| entry | [Entry](#atomix-map-v1-Entry) |  |  |






<a name="atomix-map-v1-EventsInput"></a>

### EventsInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| replay | [bool](#bool) |  |  |






<a name="atomix-map-v1-EventsOutput"></a>

### EventsOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-map-v1-Event) |  |  |






<a name="atomix-map-v1-EventsRequest"></a>

### EventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [EventsInput](#atomix-map-v1-EventsInput) |  |  |






<a name="atomix-map-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [EventsOutput](#atomix-map-v1-EventsOutput) |  |  |






<a name="atomix-map-v1-GetInput"></a>

### GetInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |






<a name="atomix-map-v1-GetOutput"></a>

### GetOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-map-v1-Entry) |  |  |






<a name="atomix-map-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [GetInput](#atomix-map-v1-GetInput) |  |  |






<a name="atomix-map-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [GetOutput](#atomix-map-v1-GetOutput) |  |  |






<a name="atomix-map-v1-MapConfig"></a>

### MapConfig







<a name="atomix-map-v1-PutInput"></a>

### PutInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [Value](#atomix-map-v1-Value) |  |  |
| timestamp | [atomix.primitive.v1.Timestamp](#atomix-primitive-v1-Timestamp) |  |  |






<a name="atomix-map-v1-PutOutput"></a>

### PutOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-map-v1-Entry) |  |  |






<a name="atomix-map-v1-PutRequest"></a>

### PutRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [PutInput](#atomix-map-v1-PutInput) |  |  |






<a name="atomix-map-v1-PutResponse"></a>

### PutResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [PutOutput](#atomix-map-v1-PutOutput) |  |  |






<a name="atomix-map-v1-RemoveInput"></a>

### RemoveInput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| timestamp | [atomix.primitive.v1.Timestamp](#atomix-primitive-v1-Timestamp) |  |  |






<a name="atomix-map-v1-RemoveOutput"></a>

### RemoveOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#atomix-map-v1-Entry) |  |  |






<a name="atomix-map-v1-RemoveRequest"></a>

### RemoveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [RemoveInput](#atomix-map-v1-RemoveInput) |  |  |






<a name="atomix-map-v1-RemoveResponse"></a>

### RemoveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [RemoveOutput](#atomix-map-v1-RemoveOutput) |  |  |






<a name="atomix-map-v1-SizeInput"></a>

### SizeInput







<a name="atomix-map-v1-SizeOutput"></a>

### SizeOutput



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |






<a name="atomix-map-v1-SizeRequest"></a>

### SizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.RequestHeaders](#atomix-primitive-v1-RequestHeaders) |  |  |
| input | [SizeInput](#atomix-map-v1-SizeInput) |  |  |






<a name="atomix-map-v1-SizeResponse"></a>

### SizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| headers | [atomix.primitive.v1.ResponseHeaders](#atomix-primitive-v1-ResponseHeaders) |  |  |
| output | [SizeOutput](#atomix-map-v1-SizeOutput) |  |  |






<a name="atomix-map-v1-Value"></a>

### Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  |  |
| ttl | [google.protobuf.Duration](#google-protobuf-Duration) |  |  |





 


<a name="atomix-map-v1-Event-Type"></a>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| INSERT | 1 |  |
| UPDATE | 2 |  |
| REMOVE | 3 |  |
| REPLAY | 4 |  |


 

 


<a name="atomix-map-v1-Map"></a>

### Map
Map is a service for a map primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Size | [SizeRequest](#atomix-map-v1-SizeRequest) | [SizeResponse](#atomix-map-v1-SizeResponse) | Size returns the size of the map |
| Put | [PutRequest](#atomix-map-v1-PutRequest) | [PutResponse](#atomix-map-v1-PutResponse) | Put puts an entry into the map |
| Get | [GetRequest](#atomix-map-v1-GetRequest) | [GetResponse](#atomix-map-v1-GetResponse) | Get gets the entry for a key |
| Remove | [RemoveRequest](#atomix-map-v1-RemoveRequest) | [RemoveResponse](#atomix-map-v1-RemoveResponse) | Remove removes an entry from the map |
| Clear | [ClearRequest](#atomix-map-v1-ClearRequest) | [ClearResponse](#atomix-map-v1-ClearResponse) | Clear removes all entries from the map |
| Events | [EventsRequest](#atomix-map-v1-EventsRequest) | [EventsResponse](#atomix-map-v1-EventsResponse) stream | Events listens for change events |
| Entries | [EntriesRequest](#atomix-map-v1-EntriesRequest) | [EntriesResponse](#atomix-map-v1-EntriesResponse) stream | Entries lists all entries in the map |

 



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

