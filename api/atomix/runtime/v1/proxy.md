# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/runtime/v1/proxy.proto](#atomix_runtime_v1_proxy-proto)
    - [GetProxyRequest](#atomix-runtime-v1-GetProxyRequest)
    - [GetProxyResponse](#atomix-runtime-v1-GetProxyResponse)
    - [ListProxiesRequest](#atomix-runtime-v1-ListProxiesRequest)
    - [ListProxiesResponse](#atomix-runtime-v1-ListProxiesResponse)
    - [Proxy](#atomix-runtime-v1-Proxy)
    - [ProxyId](#atomix-runtime-v1-ProxyId)
    - [ProxyMeta](#atomix-runtime-v1-ProxyMeta)
    - [ProxyMeta.LabelsEntry](#atomix-runtime-v1-ProxyMeta-LabelsEntry)
    - [ProxySpec](#atomix-runtime-v1-ProxySpec)
    - [ProxyStatus](#atomix-runtime-v1-ProxyStatus)
  
    - [ProxyStatus.State](#atomix-runtime-v1-ProxyStatus-State)
  
    - [ProxyService](#atomix-runtime-v1-ProxyService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_runtime_v1_proxy-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/runtime/v1/proxy.proto



<a name="atomix-runtime-v1-GetProxyRequest"></a>

### GetProxyRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| proxy_id | [ProxyId](#atomix-runtime-v1-ProxyId) |  |  |






<a name="atomix-runtime-v1-GetProxyResponse"></a>

### GetProxyResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| proxy | [Proxy](#atomix-runtime-v1-Proxy) |  |  |






<a name="atomix-runtime-v1-ListProxiesRequest"></a>

### ListProxiesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [string](#string) |  |  |
| watch | [bool](#bool) |  |  |






<a name="atomix-runtime-v1-ListProxiesResponse"></a>

### ListProxiesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| proxies | [Proxy](#atomix-runtime-v1-Proxy) | repeated |  |






<a name="atomix-runtime-v1-Proxy"></a>

### Proxy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meta | [ProxyMeta](#atomix-runtime-v1-ProxyMeta) |  |  |
| spec | [ProxySpec](#atomix-runtime-v1-ProxySpec) |  |  |
| status | [ProxyStatus](#atomix-runtime-v1-ProxyStatus) |  |  |






<a name="atomix-runtime-v1-ProxyId"></a>

### ProxyId



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| application | [string](#string) |  |  |
| primitive | [string](#string) |  |  |
| client | [string](#string) |  |  |






<a name="atomix-runtime-v1-ProxyMeta"></a>

### ProxyMeta



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [ProxyId](#atomix-runtime-v1-ProxyId) |  |  |
| version | [uint64](#uint64) |  |  |
| labels | [ProxyMeta.LabelsEntry](#atomix-runtime-v1-ProxyMeta-LabelsEntry) | repeated |  |






<a name="atomix-runtime-v1-ProxyMeta-LabelsEntry"></a>

### ProxyMeta.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-v1-ProxySpec"></a>

### ProxySpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  |  |
| config | [google.protobuf.Any](#google-protobuf-Any) |  |  |






<a name="atomix-runtime-v1-ProxyStatus"></a>

### ProxyStatus



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [ProxyStatus.State](#atomix-runtime-v1-ProxyStatus-State) |  |  |





 


<a name="atomix-runtime-v1-ProxyStatus-State"></a>

### ProxyStatus.State


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| OPENING | 1 |  |
| OPENED | 2 |  |
| CLOSING | 3 |  |
| CLOSED | 4 |  |


 

 


<a name="atomix-runtime-v1-ProxyService"></a>

### ProxyService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetProxy | [GetProxyRequest](#atomix-runtime-v1-GetProxyRequest) | [GetProxyResponse](#atomix-runtime-v1-GetProxyResponse) |  |
| ListProxies | [ListProxiesRequest](#atomix-runtime-v1-ListProxiesRequest) | [ListProxiesResponse](#atomix-runtime-v1-ListProxiesResponse) |  |

 



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

