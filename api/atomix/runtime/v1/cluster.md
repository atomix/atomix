# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [atomix/runtime/v1/cluster.proto](#atomix_runtime_v1_cluster-proto)
    - [Cluster](#atomix-runtime-v1-Cluster)
    - [ClusterId](#atomix-runtime-v1-ClusterId)
    - [ClusterMeta](#atomix-runtime-v1-ClusterMeta)
    - [ClusterSpec](#atomix-runtime-v1-ClusterSpec)
    - [ClusterStatus](#atomix-runtime-v1-ClusterStatus)
    - [CreateClusterRequest](#atomix-runtime-v1-CreateClusterRequest)
    - [CreateClusterResponse](#atomix-runtime-v1-CreateClusterResponse)
    - [DeleteClusterRequest](#atomix-runtime-v1-DeleteClusterRequest)
    - [DeleteClusterResponse](#atomix-runtime-v1-DeleteClusterResponse)
    - [GetClusterRequest](#atomix-runtime-v1-GetClusterRequest)
    - [GetClusterResponse](#atomix-runtime-v1-GetClusterResponse)
    - [ListClustersRequest](#atomix-runtime-v1-ListClustersRequest)
    - [ListClustersResponse](#atomix-runtime-v1-ListClustersResponse)
    - [UpdateClusterRequest](#atomix-runtime-v1-UpdateClusterRequest)
    - [UpdateClusterResponse](#atomix-runtime-v1-UpdateClusterResponse)
  
    - [ClusterService](#atomix-runtime-v1-ClusterService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="atomix_runtime_v1_cluster-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## atomix/runtime/v1/cluster.proto



<a name="atomix-runtime-v1-Cluster"></a>

### Cluster



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meta | [ClusterMeta](#atomix-runtime-v1-ClusterMeta) |  |  |
| spec | [ClusterSpec](#atomix-runtime-v1-ClusterSpec) |  |  |
| status | [ClusterStatus](#atomix-runtime-v1-ClusterStatus) |  |  |






<a name="atomix-runtime-v1-ClusterId"></a>

### ClusterId



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| namespace | [string](#string) |  |  |
| name | [string](#string) |  |  |






<a name="atomix-runtime-v1-ClusterMeta"></a>

### ClusterMeta



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [ClusterId](#atomix-runtime-v1-ClusterId) |  |  |
| version | [uint64](#uint64) |  |  |






<a name="atomix-runtime-v1-ClusterSpec"></a>

### ClusterSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| driver | [DriverId](#atomix-runtime-v1-DriverId) |  |  |
| config | [google.protobuf.Any](#google-protobuf-Any) |  |  |






<a name="atomix-runtime-v1-ClusterStatus"></a>

### ClusterStatus



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| conn_id | [string](#string) |  |  |






<a name="atomix-runtime-v1-CreateClusterRequest"></a>

### CreateClusterRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster | [Cluster](#atomix-runtime-v1-Cluster) |  |  |






<a name="atomix-runtime-v1-CreateClusterResponse"></a>

### CreateClusterResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster | [Cluster](#atomix-runtime-v1-Cluster) |  |  |






<a name="atomix-runtime-v1-DeleteClusterRequest"></a>

### DeleteClusterRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster | [Cluster](#atomix-runtime-v1-Cluster) |  |  |






<a name="atomix-runtime-v1-DeleteClusterResponse"></a>

### DeleteClusterResponse







<a name="atomix-runtime-v1-GetClusterRequest"></a>

### GetClusterRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster_id | [ClusterId](#atomix-runtime-v1-ClusterId) |  |  |






<a name="atomix-runtime-v1-GetClusterResponse"></a>

### GetClusterResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster | [Cluster](#atomix-runtime-v1-Cluster) |  |  |






<a name="atomix-runtime-v1-ListClustersRequest"></a>

### ListClustersRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| watch | [bool](#bool) |  |  |






<a name="atomix-runtime-v1-ListClustersResponse"></a>

### ListClustersResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| clusters | [Cluster](#atomix-runtime-v1-Cluster) | repeated |  |






<a name="atomix-runtime-v1-UpdateClusterRequest"></a>

### UpdateClusterRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster | [Cluster](#atomix-runtime-v1-Cluster) |  |  |






<a name="atomix-runtime-v1-UpdateClusterResponse"></a>

### UpdateClusterResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cluster | [Cluster](#atomix-runtime-v1-Cluster) |  |  |





 

 

 


<a name="atomix-runtime-v1-ClusterService"></a>

### ClusterService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetCluster | [GetClusterRequest](#atomix-runtime-v1-GetClusterRequest) | [GetClusterResponse](#atomix-runtime-v1-GetClusterResponse) |  |
| ListClusters | [ListClustersRequest](#atomix-runtime-v1-ListClustersRequest) | [ListClustersResponse](#atomix-runtime-v1-ListClustersResponse) |  |
| CreateCluster | [CreateClusterRequest](#atomix-runtime-v1-CreateClusterRequest) | [CreateClusterResponse](#atomix-runtime-v1-CreateClusterResponse) |  |
| UpdateCluster | [UpdateClusterRequest](#atomix-runtime-v1-UpdateClusterRequest) | [UpdateClusterResponse](#atomix-runtime-v1-UpdateClusterResponse) |  |
| DeleteCluster | [DeleteClusterRequest](#atomix-runtime-v1-DeleteClusterRequest) | [DeleteClusterResponse](#atomix-runtime-v1-DeleteClusterResponse) |  |

 



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

