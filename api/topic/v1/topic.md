# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [topic/v1/topic.proto](#topic_v1_topic-proto)
    - [CloseRequest](#atomix-topic-v1-CloseRequest)
    - [CloseResponse](#atomix-topic-v1-CloseResponse)
    - [CreateRequest](#atomix-topic-v1-CreateRequest)
    - [CreateResponse](#atomix-topic-v1-CreateResponse)
    - [PublishRequest](#atomix-topic-v1-PublishRequest)
    - [PublishResponse](#atomix-topic-v1-PublishResponse)
    - [SubscribeRequest](#atomix-topic-v1-SubscribeRequest)
    - [SubscribeResponse](#atomix-topic-v1-SubscribeResponse)
  
    - [Topic](#atomix-topic-v1-Topic)
  
- [Scalar Value Types](#scalar-value-types)



<a name="topic_v1_topic-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## topic/v1/topic.proto



<a name="atomix-topic-v1-CloseRequest"></a>

### CloseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-topic-v1-CloseResponse"></a>

### CloseResponse







<a name="atomix-topic-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| tags | [string](#string) | repeated |  |






<a name="atomix-topic-v1-CreateResponse"></a>

### CreateResponse







<a name="atomix-topic-v1-PublishRequest"></a>

### PublishRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |
| payload | [bytes](#bytes) |  |  |






<a name="atomix-topic-v1-PublishResponse"></a>

### PublishResponse







<a name="atomix-topic-v1-SubscribeRequest"></a>

### SubscribeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [atomix.v1.PrimitiveId](#atomix-v1-PrimitiveId) |  |  |






<a name="atomix-topic-v1-SubscribeResponse"></a>

### SubscribeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [uint64](#uint64) |  |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| payload | [bytes](#bytes) |  |  |





 

 

 


<a name="atomix-topic-v1-Topic"></a>

### Topic


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#atomix-topic-v1-CreateRequest) | [CreateResponse](#atomix-topic-v1-CreateResponse) | Create creates the topic |
| Close | [CloseRequest](#atomix-topic-v1-CloseRequest) | [CloseResponse](#atomix-topic-v1-CloseResponse) | Close closes the topic |
| Publish | [PublishRequest](#atomix-topic-v1-PublishRequest) | [PublishResponse](#atomix-topic-v1-PublishResponse) | Publish publishes a message to the topic |
| Subscribe | [SubscribeRequest](#atomix-topic-v1-SubscribeRequest) | [SubscribeResponse](#atomix-topic-v1-SubscribeResponse) stream | Subscribe subscribes to receive messages from the topic |

 



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

