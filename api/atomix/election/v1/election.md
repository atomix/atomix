# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/election/v1/election.proto](#atomix_election_v1_election-proto)
    - [AnointRequest](#atomix-election-v1-AnointRequest)
    - [AnointResponse](#atomix-election-v1-AnointResponse)
    - [CloseRequest](#atomix-election-v1-CloseRequest)
    - [CloseResponse](#atomix-election-v1-CloseResponse)
    - [CreateRequest](#atomix-election-v1-CreateRequest)
    - [CreateResponse](#atomix-election-v1-CreateResponse)
    - [EnterRequest](#atomix-election-v1-EnterRequest)
    - [EnterResponse](#atomix-election-v1-EnterResponse)
    - [Event](#atomix-election-v1-Event)
    - [EventsRequest](#atomix-election-v1-EventsRequest)
    - [EventsResponse](#atomix-election-v1-EventsResponse)
    - [EvictRequest](#atomix-election-v1-EvictRequest)
    - [EvictResponse](#atomix-election-v1-EvictResponse)
    - [GetTermRequest](#atomix-election-v1-GetTermRequest)
    - [GetTermResponse](#atomix-election-v1-GetTermResponse)
    - [LeaderElectionConfig](#atomix-election-v1-LeaderElectionConfig)
    - [PromoteRequest](#atomix-election-v1-PromoteRequest)
    - [PromoteResponse](#atomix-election-v1-PromoteResponse)
    - [Term](#atomix-election-v1-Term)
    - [WithdrawRequest](#atomix-election-v1-WithdrawRequest)
    - [WithdrawResponse](#atomix-election-v1-WithdrawResponse)
  
    - [Event.Type](#atomix-election-v1-Event-Type)
  
    - [LeaderElection](#atomix-election-v1-LeaderElection)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_election_v1_election-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/election/v1/election.proto



<a name="atomix-election-v1-AnointRequest"></a>

### AnointRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| candidate_id | [string](#string) |  |  |






<a name="atomix-election-v1-AnointResponse"></a>

### AnointResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-election-v1-Term) |  |  |






<a name="atomix-election-v1-CloseRequest"></a>

### CloseRequest







<a name="atomix-election-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-election-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| config | [LeaderElectionConfig](#atomix-election-v1-LeaderElectionConfig) |  |  |






<a name="atomix-election-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-election-v1-EnterRequest"></a>

### EnterRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| candidate_id | [string](#string) |  |  |






<a name="atomix-election-v1-EnterResponse"></a>

### EnterResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-election-v1-Term) |  |  |






<a name="atomix-election-v1-Event"></a>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#atomix-election-v1-Event-Type) |  |  |
| term | [Term](#atomix-election-v1-Term) |  |  |






<a name="atomix-election-v1-EventsRequest"></a>

### EventsRequest







<a name="atomix-election-v1-EventsResponse"></a>

### EventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [Event](#atomix-election-v1-Event) |  |  |






<a name="atomix-election-v1-EvictRequest"></a>

### EvictRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| candidate_id | [string](#string) |  |  |






<a name="atomix-election-v1-EvictResponse"></a>

### EvictResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-election-v1-Term) |  |  |






<a name="atomix-election-v1-GetTermRequest"></a>

### GetTermRequest







<a name="atomix-election-v1-GetTermResponse"></a>

### GetTermResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-election-v1-Term) |  |  |






<a name="atomix-election-v1-LeaderElectionConfig"></a>

### LeaderElectionConfig







<a name="atomix-election-v1-PromoteRequest"></a>

### PromoteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| candidate_id | [string](#string) |  |  |






<a name="atomix-election-v1-PromoteResponse"></a>

### PromoteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-election-v1-Term) |  |  |






<a name="atomix-election-v1-Term"></a>

### Term



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| leader_id | [string](#string) |  |  |
| candidates | [string](#string) | repeated |  |
| timestamp | [atomix.time.v1.Timestamp](#atomix-time-v1-Timestamp) |  |  |






<a name="atomix-election-v1-WithdrawRequest"></a>

### WithdrawRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| candidate_id | [string](#string) |  |  |






<a name="atomix-election-v1-WithdrawResponse"></a>

### WithdrawResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| term | [Term](#atomix-election-v1-Term) |  |  |





 


<a name="atomix-election-v1-Event-Type"></a>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| CHANGED | 1 |  |


 

 


<a name="atomix-election-v1-LeaderElection"></a>

### LeaderElection
LeaderElection is a service for a leader election primitive

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-election-v1-CreateRequest) | [CreateResponse](#atomix-election-v1-CreateResponse) |  |
| Close | [CloseRequest](#atomix-election-v1-CloseRequest) | [CloseResponse](#atomix-election-v1-CloseResponse) |  |
| Enter | [EnterRequest](#atomix-election-v1-EnterRequest) | [EnterResponse](#atomix-election-v1-EnterResponse) | Enter enters the leader election |
| Withdraw | [WithdrawRequest](#atomix-election-v1-WithdrawRequest) | [WithdrawResponse](#atomix-election-v1-WithdrawResponse) | Withdraw withdraws a candidate from the leader election |
| Anoint | [AnointRequest](#atomix-election-v1-AnointRequest) | [AnointResponse](#atomix-election-v1-AnointResponse) | Anoint anoints a candidate leader |
| Promote | [PromoteRequest](#atomix-election-v1-PromoteRequest) | [PromoteResponse](#atomix-election-v1-PromoteResponse) | Promote promotes a candidate |
| Evict | [EvictRequest](#atomix-election-v1-EvictRequest) | [EvictResponse](#atomix-election-v1-EvictResponse) | Evict evicts a candidate from the election |
| GetTerm | [GetTermRequest](#atomix-election-v1-GetTermRequest) | [GetTermResponse](#atomix-election-v1-GetTermResponse) | GetTerm gets the current leadership term |
| Events | [EventsRequest](#atomix-election-v1-EventsRequest) | [EventsResponse](#atomix-election-v1-EventsResponse) stream | Events listens for leadership events |

 



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
