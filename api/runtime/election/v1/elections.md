# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [runtime/election/v1/elections.proto](#runtime_election_v1_elections-proto)
    - [CacheConfig](#atomix-runtime-election-v1-CacheConfig)
    - [CloseRequest](#atomix-runtime-election-v1-CloseRequest)
    - [CloseResponse](#atomix-runtime-election-v1-CloseResponse)
    - [Config](#atomix-runtime-election-v1-Config)
    - [CreateRequest](#atomix-runtime-election-v1-CreateRequest)
    - [CreateResponse](#atomix-runtime-election-v1-CreateResponse)
  
    - [LeaderElections](#atomix-runtime-election-v1-LeaderElections)
  
- [Scalar Value Types](#scalar-value-types)



<a name="runtime_election_v1_elections-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## runtime/election/v1/elections.proto



<a name="atomix-runtime-election-v1-CacheConfig"></a>

### CacheConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | [bool](#bool) |  |  |






<a name="atomix-runtime-election-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |






<a name="atomix-runtime-election-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-runtime-election-v1-Config"></a>

### Config



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cache | [CacheConfig](#atomix-runtime-election-v1-CacheConfig) |  |  |






<a name="atomix-runtime-election-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.runtime.v1.PrimitiveID](#atomix-runtime-v1-PrimitiveID) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-runtime-election-v1-CreateResponse"></a>

### CreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| config | [Config](#atomix-runtime-election-v1-Config) |  |  |





 

 

 


<a name="atomix-runtime-election-v1-LeaderElections"></a>

### LeaderElections
LeaderElections is a service for managing leader election primitives

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-runtime-election-v1-CreateRequest) | [CreateResponse](#atomix-runtime-election-v1-CreateResponse) | Create creates the leader election |
| Close | [CloseRequest](#atomix-runtime-election-v1-CloseRequest) | [CloseResponse](#atomix-runtime-election-v1-CloseResponse) | Close closes the leader election |

 



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

