#!/bin/bash

# SPDX-FileCopyrightText: 2023-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

service="atomix-controller"
namespace="kube-system"
output=$(pwd)

while getopts 'c:k:s:n:o:h' opt; do
  case "$opt" in
    k)
      cakey="$OPTARG"
      ;;
    c)
      cacrt="$OPTARG"
      ;;
    s)
      service="$OPTARG"
      ;;
    n)
      namespace="$OPTARG"
      ;;
    o)
      output="$OPTARG"
      ;;
    h)
      echo "usage: $(basename "$0") [-k ca_key_file] [-c ca_cert_file] [-s service_name] [-n namespace] [-o output_path] [-h]" >&2
      exit 0
      ;;
    ?)
      echo "usage: $(basename "$0") [-k ca_key_file] [-c ca_cert_file] [-s service_name] [-n namespace] [-o output_path] [-h]" >&2
      exit 1
      ;;
  esac
done

mkdir -p "${output}"

if [ -z "$cakey" ]; then
  cakey="${output}/ca.key"
  openssl genrsa -out "${cakey}" 2048
  echo "Generated ${cakey}"
fi

if [ -z "$cacrt" ]; then
  cacrt="${output}/ca.crt"
  openssl req -x509 -new -nodes -key "${cakey}" -days 100000 -out "${cacrt}" -subj "/CN=admission_ca"
  echo "Generated ${cacrt}"
fi

srvcfg="${output}/${service}.conf"
srvcsr="${output}/${service}.csr"
srvkey="${output}/${service}.key"
srvcrt="${output}/${service}.crt"

cat >"${srvcfg}" <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[ alt_names ]
DNS.1 = ${service}.${namespace}
DNS.2 = ${service}.${namespace}.svc
[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = @alt_names
EOF

openssl genrsa -out "${srvkey}" 2048
echo "Generated ${srvkey}"

openssl req -new -key "${srvkey}" -out "${srvcsr}" -subj "/CN=${service}.${namespace}.svc" -config "${srvcfg}"
echo "Generated ${srvcsr}"

openssl x509 -req -in "${srvcsr}" -CA "${cacrt}" -CAkey "${cakey}" -CAcreateserial -out "${srvcrt}" -days 100000 -extensions v3_req -extfile "${srvcfg}"
echo "Generated ${srvcrt}"
