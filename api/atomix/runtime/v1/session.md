# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/runtime/v1/session.proto](#atomix_runtime_v1_session-proto)
    - [GetSessionRequest](#atomix-runtime-v1-GetSessionRequest)
    - [GetSessionResponse](#atomix-runtime-v1-GetSessionResponse)
    - [ListSessionsRequest](#atomix-runtime-v1-ListSessionsRequest)
    - [ListSessionsResponse](#atomix-runtime-v1-ListSessionsResponse)
    - [Session](#atomix-runtime-v1-Session)
    - [SessionId](#atomix-runtime-v1-SessionId)
    - [SessionMeta](#atomix-runtime-v1-SessionMeta)
    - [SessionMeta.LabelsEntry](#atomix-runtime-v1-SessionMeta-LabelsEntry)
    - [SessionSpec](#atomix-runtime-v1-SessionSpec)
    - [SessionStatus](#atomix-runtime-v1-SessionStatus)
  
    - [SessionStatus.State](#atomix-runtime-v1-SessionStatus-State)
  
    - [SessionService](#atomix-runtime-v1-SessionService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_runtime_v1_session-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/runtime/v1/session.proto



<a name="atomix-runtime-v1-GetSessionRequest"></a>

### GetSessionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | [SessionId](#atomix-runtime-v1-SessionId) |  |  |






<a name="atomix-runtime-v1-GetSessionResponse"></a>

### GetSessionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [Session](#atomix-runtime-v1-Session) |  |  |






<a name="atomix-runtime-v1-ListSessionsRequest"></a>

### ListSessionsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [string](#string) |  |  |
| watch | [bool](#bool) |  |  |






<a name="atomix-runtime-v1-ListSessionsResponse"></a>

### ListSessionsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sessions | [Session](#atomix-runtime-v1-Session) | repeated |  |






<a name="atomix-runtime-v1-Session"></a>

### Session



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meta | [SessionMeta](#atomix-runtime-v1-SessionMeta) |  |  |
| spec | [SessionSpec](#atomix-runtime-v1-SessionSpec) |  |  |
| status | [SessionStatus](#atomix-runtime-v1-SessionStatus) |  |  |






<a name="atomix-runtime-v1-SessionId"></a>

### SessionId



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| primitive | [PrimitiveId](#atomix-runtime-v1-PrimitiveId) |  |  |






<a name="atomix-runtime-v1-SessionMeta"></a>

### SessionMeta



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [SessionId](#atomix-runtime-v1-SessionId) |  |  |
| version | [uint64](#uint64) |  |  |
| labels | [SessionMeta.LabelsEntry](#atomix-runtime-v1-SessionMeta-LabelsEntry) | repeated |  |






<a name="atomix-runtime-v1-SessionMeta-LabelsEntry"></a>

### SessionMeta.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-v1-SessionSpec"></a>

### SessionSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  |  |
| config | [google.protobuf.Any](#google-protobuf-Any) |  |  |






<a name="atomix-runtime-v1-SessionStatus"></a>

### SessionStatus



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [SessionStatus.State](#atomix-runtime-v1-SessionStatus-State) |  |  |





 


<a name="atomix-runtime-v1-SessionStatus-State"></a>

### SessionStatus.State


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| OPENING | 1 |  |
| OPENED | 2 |  |
| CLOSING | 3 |  |
| CLOSED | 4 |  |


 

 


<a name="atomix-runtime-v1-SessionService"></a>

### SessionService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetSession | [GetSessionRequest](#atomix-runtime-v1-GetSessionRequest) | [GetSessionResponse](#atomix-runtime-v1-GetSessionResponse) |  |
| ListSessions | [ListSessionsRequest](#atomix-runtime-v1-ListSessionsRequest) | [ListSessionsResponse](#atomix-runtime-v1-ListSessionsResponse) |  |

 



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

