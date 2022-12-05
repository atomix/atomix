# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/proxy/v1/proxy.proto](#atomix_proxy_v1_proxy-proto)
    - [ConfigureRequest](#atomix-proxy-v1-ConfigureRequest)
    - [ConfigureResponse](#atomix-proxy-v1-ConfigureResponse)
    - [ConnectRequest](#atomix-proxy-v1-ConnectRequest)
    - [ConnectResponse](#atomix-proxy-v1-ConnectResponse)
    - [DisconnectRequest](#atomix-proxy-v1-DisconnectRequest)
    - [DisconnectResponse](#atomix-proxy-v1-DisconnectResponse)
    - [DriverId](#atomix-proxy-v1-DriverId)
    - [StoreId](#atomix-proxy-v1-StoreId)
  
    - [ProxyControl](#atomix-proxy-v1-ProxyControl)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_proxy_v1_proxy-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/proxy/v1/proxy.proto



<a name="atomix-proxy-v1-ConfigureRequest"></a>

### ConfigureRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_id | [StoreId](#atomix-proxy-v1-StoreId) |  |  |
| config | [bytes](#bytes) |  |  |






<a name="atomix-proxy-v1-ConfigureResponse"></a>

### ConfigureResponse







<a name="atomix-proxy-v1-ConnectRequest"></a>

### ConnectRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_id | [StoreId](#atomix-proxy-v1-StoreId) |  |  |
| driver_id | [DriverId](#atomix-proxy-v1-DriverId) |  |  |
| config | [bytes](#bytes) |  |  |






<a name="atomix-proxy-v1-ConnectResponse"></a>

### ConnectResponse







<a name="atomix-proxy-v1-DisconnectRequest"></a>

### DisconnectRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_id | [StoreId](#atomix-proxy-v1-StoreId) |  |  |






<a name="atomix-proxy-v1-DisconnectResponse"></a>

### DisconnectResponse







<a name="atomix-proxy-v1-DriverId"></a>

### DriverId



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| version | [string](#string) |  |  |






<a name="atomix-proxy-v1-StoreId"></a>

### StoreId



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| namespace | [string](#string) |  |  |
| name | [string](#string) |  |  |





 

 

 


<a name="atomix-proxy-v1-ProxyControl"></a>

### ProxyControl


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Connect | [ConnectRequest](#atomix-proxy-v1-ConnectRequest) | [ConnectResponse](#atomix-proxy-v1-ConnectResponse) |  |
| Configure | [ConfigureRequest](#atomix-proxy-v1-ConfigureRequest) | [ConfigureResponse](#atomix-proxy-v1-ConfigureResponse) |  |
| Disconnect | [DisconnectRequest](#atomix-proxy-v1-DisconnectRequest) | [DisconnectResponse](#atomix-proxy-v1-DisconnectResponse) |  |

 



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

