# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/runtime/v1/application.proto](#atomix_runtime_v1_application-proto)
    - [Application](#atomix-runtime-v1-Application)
    - [ApplicationId](#atomix-runtime-v1-ApplicationId)
    - [ApplicationMeta](#atomix-runtime-v1-ApplicationMeta)
    - [ApplicationSpec](#atomix-runtime-v1-ApplicationSpec)
    - [ApplicationStatus](#atomix-runtime-v1-ApplicationStatus)
    - [Binding](#atomix-runtime-v1-Binding)
    - [BindingRule](#atomix-runtime-v1-BindingRule)
    - [BindingRule.HeadersEntry](#atomix-runtime-v1-BindingRule-HeadersEntry)
    - [CreateApplicationRequest](#atomix-runtime-v1-CreateApplicationRequest)
    - [CreateApplicationResponse](#atomix-runtime-v1-CreateApplicationResponse)
    - [DeleteApplicationRequest](#atomix-runtime-v1-DeleteApplicationRequest)
    - [DeleteApplicationResponse](#atomix-runtime-v1-DeleteApplicationResponse)
    - [GetApplicationRequest](#atomix-runtime-v1-GetApplicationRequest)
    - [GetApplicationResponse](#atomix-runtime-v1-GetApplicationResponse)
    - [ListApplicationsRequest](#atomix-runtime-v1-ListApplicationsRequest)
    - [ListApplicationsResponse](#atomix-runtime-v1-ListApplicationsResponse)
    - [UpdateApplicationRequest](#atomix-runtime-v1-UpdateApplicationRequest)
    - [UpdateApplicationResponse](#atomix-runtime-v1-UpdateApplicationResponse)
  
    - [ApplicationService](#atomix-runtime-v1-ApplicationService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_runtime_v1_application-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/runtime/v1/application.proto



<a name="atomix-runtime-v1-Application"></a>

### Application



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meta | [ApplicationMeta](#atomix-runtime-v1-ApplicationMeta) |  |  |
| spec | [ApplicationSpec](#atomix-runtime-v1-ApplicationSpec) |  |  |
| status | [ApplicationStatus](#atomix-runtime-v1-ApplicationStatus) |  |  |






<a name="atomix-runtime-v1-ApplicationId"></a>

### ApplicationId



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| namespace | [string](#string) |  |  |
| name | [string](#string) |  |  |






<a name="atomix-runtime-v1-ApplicationMeta"></a>

### ApplicationMeta



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [ApplicationId](#atomix-runtime-v1-ApplicationId) |  |  |
| version | [uint64](#uint64) |  |  |






<a name="atomix-runtime-v1-ApplicationSpec"></a>

### ApplicationSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bindings | [Binding](#atomix-runtime-v1-Binding) | repeated |  |






<a name="atomix-runtime-v1-ApplicationStatus"></a>

### ApplicationStatus







<a name="atomix-runtime-v1-Binding"></a>

### Binding



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster_id | [ClusterId](#atomix-runtime-v1-ClusterId) |  |  |
| rules | [BindingRule](#atomix-runtime-v1-BindingRule) | repeated |  |






<a name="atomix-runtime-v1-BindingRule"></a>

### BindingRule



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kinds | [string](#string) | repeated |  |
| names | [string](#string) | repeated |  |
| headers | [BindingRule.HeadersEntry](#atomix-runtime-v1-BindingRule-HeadersEntry) | repeated |  |






<a name="atomix-runtime-v1-BindingRule-HeadersEntry"></a>

### BindingRule.HeadersEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="atomix-runtime-v1-CreateApplicationRequest"></a>

### CreateApplicationRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| application | [Application](#atomix-runtime-v1-Application) |  |  |






<a name="atomix-runtime-v1-CreateApplicationResponse"></a>

### CreateApplicationResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| application | [Application](#atomix-runtime-v1-Application) |  |  |






<a name="atomix-runtime-v1-DeleteApplicationRequest"></a>

### DeleteApplicationRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| application | [Application](#atomix-runtime-v1-Application) |  |  |






<a name="atomix-runtime-v1-DeleteApplicationResponse"></a>

### DeleteApplicationResponse







<a name="atomix-runtime-v1-GetApplicationRequest"></a>

### GetApplicationRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| application_id | [ApplicationId](#atomix-runtime-v1-ApplicationId) |  |  |






<a name="atomix-runtime-v1-GetApplicationResponse"></a>

### GetApplicationResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| application | [Application](#atomix-runtime-v1-Application) |  |  |






<a name="atomix-runtime-v1-ListApplicationsRequest"></a>

### ListApplicationsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| watch | [bool](#bool) |  |  |






<a name="atomix-runtime-v1-ListApplicationsResponse"></a>

### ListApplicationsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| applications | [Application](#atomix-runtime-v1-Application) | repeated |  |






<a name="atomix-runtime-v1-UpdateApplicationRequest"></a>

### UpdateApplicationRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| application | [Application](#atomix-runtime-v1-Application) |  |  |






<a name="atomix-runtime-v1-UpdateApplicationResponse"></a>

### UpdateApplicationResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| application | [Application](#atomix-runtime-v1-Application) |  |  |





 

 

 


<a name="atomix-runtime-v1-ApplicationService"></a>

### ApplicationService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetApplication | [GetApplicationRequest](#atomix-runtime-v1-GetApplicationRequest) | [GetApplicationResponse](#atomix-runtime-v1-GetApplicationResponse) |  |
| ListApplications | [ListApplicationsRequest](#atomix-runtime-v1-ListApplicationsRequest) | [ListApplicationsResponse](#atomix-runtime-v1-ListApplicationsResponse) |  |
| CreateApplication | [CreateApplicationRequest](#atomix-runtime-v1-CreateApplicationRequest) | [CreateApplicationResponse](#atomix-runtime-v1-CreateApplicationResponse) |  |
| UpdateApplication | [UpdateApplicationRequest](#atomix-runtime-v1-UpdateApplicationRequest) | [UpdateApplicationResponse](#atomix-runtime-v1-UpdateApplicationResponse) |  |
| DeleteApplication | [DeleteApplicationRequest](#atomix-runtime-v1-DeleteApplicationRequest) | [DeleteApplicationResponse](#atomix-runtime-v1-DeleteApplicationResponse) |  |

 



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

