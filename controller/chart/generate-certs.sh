#!/bin/bash

# SPDX-FileCopyrightText: 2023-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

openssl genrsa -out certs/ca.key 2048
openssl req -x509 -new -nodes -key certs/ca.key -days 100000 -out certs/ca.crt -subj "/CN=admission_ca"

cat >certs/admission.conf <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[ alt_names ]
DNS.1 = atomix-controller.kube-system
DNS.2 = atomix-controller.kube-system.svc
[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = @alt_names
EOF

openssl genrsa -out certs/admission.key 2048
openssl req -new -key certs/admission.key -out certs/admission.csr -subj "/CN=atomix-controller.kube-system.svc" -config certs/admission.conf
openssl x509 -req -in certs/admission.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/admission.crt -days 100000 -extensions v3_req -extfile certs/admission.conf

cat certs/admission.key | base64
cat certs/admission.crt | base64
cat certs/ca.crt | base64
