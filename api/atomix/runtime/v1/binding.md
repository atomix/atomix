# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/runtime/v1/binding.proto](#atomix_runtime_v1_binding-proto)
    - [Binding](#atomix-runtime-v1-Binding)
    - [BindingId](#atomix-runtime-v1-BindingId)
    - [BindingMeta](#atomix-runtime-v1-BindingMeta)
    - [BindingRule](#atomix-runtime-v1-BindingRule)
    - [BindingRule.MetadataEntry](#atomix-runtime-v1-BindingRule-MetadataEntry)
    - [BindingSpec](#atomix-runtime-v1-BindingSpec)
    - [CreateBindingRequest](#atomix-runtime-v1-CreateBindingRequest)
    - [CreateBindingResponse](#atomix-runtime-v1-CreateBindingResponse)
    - [DeleteBindingRequest](#atomix-runtime-v1-DeleteBindingRequest)
    - [DeleteBindingResponse](#atomix-runtime-v1-DeleteBindingResponse)
    - [GetBindingRequest](#atomix-runtime-v1-GetBindingRequest)
    - [GetBindingResponse](#atomix-runtime-v1-GetBindingResponse)
    - [ListBindingsRequest](#atomix-runtime-v1-ListBindingsRequest)
    - [ListBindingsResponse](#atomix-runtime-v1-ListBindingsResponse)
    - [UpdateBindingRequest](#atomix-runtime-v1-UpdateBindingRequest)
    - [UpdateBindingResponse](#atomix-runtime-v1-UpdateBindingResponse)
  
    - [BindingService](#atomix-runtime-v1-BindingService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_runtime_v1_binding-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/runtime/v1/binding.proto



<a name="atomix-runtime-v1-Binding"></a>

### Binding



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meta | [BindingMeta](#atomix-runtime-v1-BindingMeta) |  |  |
| spec | [BindingSpec](#atomix-runtime-v1-BindingSpec) |  |  |






<a name="atomix-runtime-v1-BindingId"></a>

### BindingId



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| namespace | [string](#string) |  |  |
| name | [string](#string) |  |  |






<a name="atomix-runtime-v1-BindingMeta"></a>

### BindingMeta



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [BindingId](#atomix-runtime-v1-BindingId) |  |  |
| version | [uint64](#uint64) |  |  |






<a name="atomix-runtime-v1-BindingRule"></a>

### BindingRule



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kinds | [string](#string) | repeated |  |
| names | [string](#string) | repeated |  |
| metadata | [BindingRule.MetadataEntry](#atomix-runtime-v1-BindingRule-MetadataEntry) | repeated |  |






<a name="atomix-runtime-v1-BindingRule-MetadataEntry"></a>

### BindingRule.MetadataEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-v1-BindingSpec"></a>

### BindingSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster_id | [ClusterId](#atomix-runtime-v1-ClusterId) |  |  |
| rules | [BindingRule](#atomix-runtime-v1-BindingRule) | repeated |  |






<a name="atomix-runtime-v1-CreateBindingRequest"></a>

### CreateBindingRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| binding | [Binding](#atomix-runtime-v1-Binding) |  |  |






<a name="atomix-runtime-v1-CreateBindingResponse"></a>

### CreateBindingResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| binding | [Binding](#atomix-runtime-v1-Binding) |  |  |






<a name="atomix-runtime-v1-DeleteBindingRequest"></a>

### DeleteBindingRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| binding | [Binding](#atomix-runtime-v1-Binding) |  |  |






<a name="atomix-runtime-v1-DeleteBindingResponse"></a>

### DeleteBindingResponse







<a name="atomix-runtime-v1-GetBindingRequest"></a>

### GetBindingRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| binding_id | [BindingId](#atomix-runtime-v1-BindingId) |  |  |






<a name="atomix-runtime-v1-GetBindingResponse"></a>

### GetBindingResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| binding | [Binding](#atomix-runtime-v1-Binding) |  |  |






<a name="atomix-runtime-v1-ListBindingsRequest"></a>

### ListBindingsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| watch | [bool](#bool) |  |  |






<a name="atomix-runtime-v1-ListBindingsResponse"></a>

### ListBindingsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bindings | [Binding](#atomix-runtime-v1-Binding) | repeated |  |






<a name="atomix-runtime-v1-UpdateBindingRequest"></a>

### UpdateBindingRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| binding | [Binding](#atomix-runtime-v1-Binding) |  |  |






<a name="atomix-runtime-v1-UpdateBindingResponse"></a>

### UpdateBindingResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| binding | [Binding](#atomix-runtime-v1-Binding) |  |  |





 

 

 


<a name="atomix-runtime-v1-BindingService"></a>

### BindingService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetBinding | [GetBindingRequest](#atomix-runtime-v1-GetBindingRequest) | [GetBindingResponse](#atomix-runtime-v1-GetBindingResponse) |  |
| ListBindings | [ListBindingsRequest](#atomix-runtime-v1-ListBindingsRequest) | [ListBindingsResponse](#atomix-runtime-v1-ListBindingsResponse) |  |
| CreateBinding | [CreateBindingRequest](#atomix-runtime-v1-CreateBindingRequest) | [CreateBindingResponse](#atomix-runtime-v1-CreateBindingResponse) |  |
| UpdateBinding | [UpdateBindingRequest](#atomix-runtime-v1-UpdateBindingRequest) | [UpdateBindingResponse](#atomix-runtime-v1-UpdateBindingResponse) |  |
| DeleteBinding | [DeleteBindingRequest](#atomix-runtime-v1-DeleteBindingRequest) | [DeleteBindingResponse](#atomix-runtime-v1-DeleteBindingResponse) |  |

 



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

