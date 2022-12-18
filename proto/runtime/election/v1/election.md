# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [runtime/election/v1/election.proto](#runtime_election_v1_election-proto)
    - [AnointRequest](#atomix-runtime-election-v1-AnointRequest)
    - [AnointResponse](#atomix-runtime-election-v1-AnointResponse)
    - [CloseRequest](#atomix-runtime-election-v1-CloseRequest)
    - [CloseResponse](#atomix-runtime-election-v1-CloseResponse)
    - [CreateRequest](#atomix-runtime-election-v1-CreateRequest)
    - [CreateResponse](#atomix-runtime-election-v1-CreateResponse)
    - [DemoteRequest](#atomix-runtime-election-v1-DemoteRequest)
    - [DemoteResponse](#atomix-runtime-election-v1-DemoteResponse)
    - [EnterRequest](#atomix-runtime-election-v1-EnterRequest)
    - [EnterResponse](#atomix-runtime-election-v1-EnterResponse)
    - [EvictRequest](#atomix-runtime-election-v1-EvictRequest)
    - [EvictResponse](#atomix-runtime-election-v1-EvictResponse)
    - [GetTermRequest](#atomix-runtime-election-v1-GetTermRequest)
    - [GetTermResponse](#atomix-runtime-election-v1-GetTermResponse)
    - [PromoteRequest](#atomix-runtime-election-v1-PromoteRequest)
    - [PromoteResponse](#atomix-runtime-election-v1-PromoteResponse)
    - [Term](#atomix-runtime-election-v1-Term)
    - [WatchRequest](#atomix-runtime-election-v1-WatchRequest)
    - [WatchResponse](#atomix-runtime-election-v1-WatchResponse)
    - [WithdrawRequest](#atomix-runtime-election-v1-WithdrawRequest)
    - [WithdrawResponse](#atomix-runtime-election-v1-WithdrawResponse)
  
    - [LeaderElection](#atomix-runtime-election-v1-LeaderElection)
  
- [Scalar Value Types](#scalar-value-types)



<a name="runtime_election_v1_election-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## runtime/election/v1/election.proto



<a name="atomix-runtime-election-v1-AnointRequest"></a>

### AnointRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| candidate | [string](#string) |  |  |






<a name="atomix-runtime-election-v1-AnointResponse"></a>

### AnointResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-runtime-election-v1-Term) |  |  |






<a name="atomix-runtime-election-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-election-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-runtime-election-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-runtime-election-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-runtime-election-v1-DemoteRequest"></a>

### DemoteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| candidate | [string](#string) |  |  |






<a name="atomix-runtime-election-v1-DemoteResponse"></a>

### DemoteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-runtime-election-v1-Term) |  |  |






<a name="atomix-runtime-election-v1-EnterRequest"></a>

### EnterRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| candidate | [string](#string) |  |  |






<a name="atomix-runtime-election-v1-EnterResponse"></a>

### EnterResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-runtime-election-v1-Term) |  |  |






<a name="atomix-runtime-election-v1-EvictRequest"></a>

### EvictRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| candidate | [string](#string) |  |  |






<a name="atomix-runtime-election-v1-EvictResponse"></a>

### EvictResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-runtime-election-v1-Term) |  |  |






<a name="atomix-runtime-election-v1-GetTermRequest"></a>

### GetTermRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-election-v1-GetTermResponse"></a>

### GetTermResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-runtime-election-v1-Term) |  |  |






<a name="atomix-runtime-election-v1-PromoteRequest"></a>

### PromoteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| candidate | [string](#string) |  |  |






<a name="atomix-runtime-election-v1-PromoteResponse"></a>

### PromoteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-runtime-election-v1-Term) |  |  |






<a name="atomix-runtime-election-v1-Term"></a>

### Term



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [uint64](#uint64) |  |  |
| leader | [string](#string) |  |  |
| candidates | [string](#string) | repeated |  |






<a name="atomix-runtime-election-v1-WatchRequest"></a>

### WatchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-election-v1-WatchResponse"></a>

### WatchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-runtime-election-v1-Term) |  |  |






<a name="atomix-runtime-election-v1-WithdrawRequest"></a>

### WithdrawRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |
| candidate | [string](#string) |  |  |






<a name="atomix-runtime-election-v1-WithdrawResponse"></a>

### WithdrawResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-runtime-election-v1-Term) |  |  |





 

 

 


<a name="atomix-runtime-election-v1-LeaderElection"></a>

### LeaderElection
LeaderElection is a service for a leader election primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-runtime-election-v1-CreateRequest) | [CreateResponse](#atomix-runtime-election-v1-CreateResponse) | Create creates the leader election |
| Close | [CloseRequest](#atomix-runtime-election-v1-CloseRequest) | [CloseResponse](#atomix-runtime-election-v1-CloseResponse) | Close closes the leader election |
| Enter | [EnterRequest](#atomix-runtime-election-v1-EnterRequest) | [EnterResponse](#atomix-runtime-election-v1-EnterResponse) | Enter enters the leader election |
| Withdraw | [WithdrawRequest](#atomix-runtime-election-v1-WithdrawRequest) | [WithdrawResponse](#atomix-runtime-election-v1-WithdrawResponse) | Withdraw withdraws a candidate from the leader election |
| Anoint | [AnointRequest](#atomix-runtime-election-v1-AnointRequest) | [AnointResponse](#atomix-runtime-election-v1-AnointResponse) | Anoint anoints a candidate leader |
| Promote | [PromoteRequest](#atomix-runtime-election-v1-PromoteRequest) | [PromoteResponse](#atomix-runtime-election-v1-PromoteResponse) | Promote promotes a candidate |
| Demote | [DemoteRequest](#atomix-runtime-election-v1-DemoteRequest) | [DemoteResponse](#atomix-runtime-election-v1-DemoteResponse) | Demote demotes a candidate |
| Evict | [EvictRequest](#atomix-runtime-election-v1-EvictRequest) | [EvictResponse](#atomix-runtime-election-v1-EvictResponse) | Evict evicts a candidate from the election |
| GetTerm | [GetTermRequest](#atomix-runtime-election-v1-GetTermRequest) | [GetTermResponse](#atomix-runtime-election-v1-GetTermResponse) | GetTerm gets the current leadership term |
| Watch | [WatchRequest](#atomix-runtime-election-v1-WatchRequest) | [WatchResponse](#atomix-runtime-election-v1-WatchResponse) stream | Watch watches the election for events |

 



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

