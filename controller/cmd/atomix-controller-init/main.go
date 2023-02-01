// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"strings"
	"time"
)

var log = logging.GetLogger()

var crdScheme = apiruntime.NewScheme()
var crdCodecs = serializer.NewCodecFactory(crdScheme)

func init() {
	if err := apiextensionsv1.AddToScheme(crdScheme); err != nil {
		panic(err)
	}
}

func main() {
	log.Info(fmt.Sprintf("Go Version: %s", runtime.Version()))
	log.Info(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))
	//log.SetLevel(logging.DebugLevel)
	//logf.SetLogger(logr.New(&ControllerLogSink{log.WithSkipCalls(1)}))

	cmd := getCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "atomix-controller-init",
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			namespace, _ := cmd.Flags().GetString("namespace")
			service, _ := cmd.Flags().GetString("service")
			webhooks, _ := cmd.Flags().GetStringArray("webhook")
			crds, _ := cmd.Flags().GetStringArray("crd")
			certsDir, _ := cmd.Flags().GetString("certs")

			config := controllerruntime.GetConfigOrDie()

			log.Info("Generating self-signed certificates")

			admissionClient, err := kubernetes.NewForConfig(config)
			if err != nil {
				log.Panic(err)
			}

			var caPEM, serverCertPEM, serverPrivKeyPEM *bytes.Buffer
			ca := &x509.Certificate{
				SerialNumber: big.NewInt(2023),
				Subject: pkix.Name{
					Organization: []string{"Atomix"},
				},
				NotBefore:             time.Now(),
				NotAfter:              time.Now().AddDate(1, 0, 0),
				IsCA:                  true,
				ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
				KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
				BasicConstraintsValid: true,
			}

			caPrivKey, err := rsa.GenerateKey(cryptorand.Reader, 4096)
			if err != nil {
				log.Panic(err)
			}

			caBytes, err := x509.CreateCertificate(cryptorand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
			if err != nil {
				log.Panic(err)
			}

			// PEM encode CA cert
			caPEM = new(bytes.Buffer)
			_ = pem.Encode(caPEM, &pem.Block{
				Type:  "CERTIFICATE",
				Bytes: caBytes,
			})

			dnsNames := []string{
				service,
				fmt.Sprintf("%s.%s", service, namespace),
				fmt.Sprintf("%s.%s.svc", service, namespace),
			}
			commonName := fmt.Sprintf("%s.%s.svc", service, namespace)

			cert := &x509.Certificate{
				DNSNames:     dnsNames,
				SerialNumber: big.NewInt(1658),
				Subject: pkix.Name{
					CommonName:   commonName,
					Organization: []string{"Atomix"},
				},
				NotBefore:    time.Now(),
				NotAfter:     time.Now().AddDate(1, 0, 0),
				SubjectKeyId: []byte{1, 2, 3, 4, 6},
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
				KeyUsage:     x509.KeyUsageDigitalSignature,
			}

			serverPrivKey, err := rsa.GenerateKey(cryptorand.Reader, 4096)
			if err != nil {
				log.Panic(err)
			}

			serverCertBytes, err := x509.CreateCertificate(cryptorand.Reader, cert, ca, &serverPrivKey.PublicKey, caPrivKey)
			if err != nil {
				log.Panic(err)
			}

			serverCertPEM = new(bytes.Buffer)
			_ = pem.Encode(serverCertPEM, &pem.Block{
				Type:  "CERTIFICATE",
				Bytes: serverCertBytes,
			})

			serverPrivKeyPEM = new(bytes.Buffer)
			_ = pem.Encode(serverPrivKeyPEM, &pem.Block{
				Type:  "RSA PRIVATE KEY",
				Bytes: x509.MarshalPKCS1PrivateKey(serverPrivKey),
			})

			err = os.WriteFile(filepath.Join(certsDir, "tls.crt"), serverCertPEM.Bytes(), 0666)
			if err != nil {
				log.Panic(err)
			}

			err = os.WriteFile(filepath.Join(certsDir, "tls.key"), serverPrivKeyPEM.Bytes(), 0666)
			if err != nil {
				log.Panic(err)
			}

			caBundle := caPEM.Bytes()

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			for _, webhookName := range webhooks {
				log.Infof("Injecting certificates into MutatingWebhookConfiguration %s", webhookName)

				webhook, err := admissionClient.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, webhookName, metav1.GetOptions{})
				if err != nil {
					log.Panic(err)
				}

				for i, wh := range webhook.Webhooks {
					wh.ClientConfig.Service.Namespace = namespace
					wh.ClientConfig.Service.Name = service
					wh.ClientConfig.CABundle = caBundle
					webhook.Webhooks[i] = wh
				}

				if _, err := admissionClient.AdmissionregistrationV1().MutatingWebhookConfigurations().Update(ctx, webhook, metav1.UpdateOptions{}); err != nil {
					log.Panic(err)
				}
			}

			extensionsClient, err := apiextensionsv1client.NewForConfig(config)
			if err != nil {
				log.Panic(err)
			}

			for _, nameFile := range crds {
				parts := strings.Split(nameFile, "=")
				crdName, crdFile := parts[0], parts[1]

				log.Infof("Updating versions in CustomResourceDefinition %s", crdName)
				storedCRD, err := extensionsClient.CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
				if err != nil {
					log.Panic(err)
				}

				crdBytes, err := os.ReadFile(crdFile)
				if err != nil {
					log.Panic(err)
				}

				updatedCRD := &apiextensionsv1.CustomResourceDefinition{}
				gvk := apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinition")
				if _, _, err := crdCodecs.UniversalDeserializer().Decode(crdBytes, &gvk, updatedCRD); err != nil {
					log.Panic(err)
				}

				storedCRD.Spec.Versions = updatedCRD.Spec.Versions

				log.Infof("Injecting certificates into CustomResourceDefinition %s", crdName)
				if updatedCRD.Spec.Conversion != nil && updatedCRD.Spec.Conversion.Strategy == apiextensionsv1.WebhookConverter {
					storedCRD.Spec.Conversion = updatedCRD.Spec.Conversion
					if storedCRD.Spec.Conversion.Webhook == nil {
						storedCRD.Spec.Conversion.Webhook = &apiextensionsv1.WebhookConversion{}
					}
					if storedCRD.Spec.Conversion.Webhook.ClientConfig == nil {
						storedCRD.Spec.Conversion.Webhook.ClientConfig = &apiextensionsv1.WebhookClientConfig{}
					}
					if storedCRD.Spec.Conversion.Webhook.ClientConfig.Service == nil {
						storedCRD.Spec.Conversion.Webhook.ClientConfig.Service = &apiextensionsv1.ServiceReference{}
					}
					if storedCRD.Spec.Conversion.Webhook.ConversionReviewVersions == nil {
						storedCRD.Spec.Conversion.Webhook.ConversionReviewVersions = []string{"v1"}
					}
					storedCRD.Spec.Conversion.Webhook.ClientConfig.Service.Namespace = namespace
					storedCRD.Spec.Conversion.Webhook.ClientConfig.Service.Name = service
					storedCRD.Spec.Conversion.Webhook.ClientConfig.CABundle = caBundle
				} else {
					storedCRD.Spec.Conversion = updatedCRD.Spec.Conversion
				}

				if _, err := extensionsClient.CustomResourceDefinitions().Update(ctx, storedCRD, metav1.UpdateOptions{}); err != nil {
					log.Panic(err)
				}
			}
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "the namespace for which to initialize the controller")
	cmd.Flags().StringP("service", "s", "", "the service for which to initialize the controller")
	cmd.Flags().StringArray("webhook", []string{}, "the webhooks to configure")
	cmd.Flags().StringArray("crd", []string{}, "the CRDs to ugprade")
	cmd.Flags().String("certs", "/etc/atomix/certs", "the path to which to write the certificates")
	return cmd
}

type ControllerLogSink struct {
	log logging.Logger
}

func (l *ControllerLogSink) Init(info logr.RuntimeInfo) {

}

func (l *ControllerLogSink) Enabled(level int) bool {
	return true
}

func (l *ControllerLogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	l.log.WithFields(getFields(keysAndValues...)...).Info(msg)
}

func (l *ControllerLogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	l.log.WithFields(getFields(keysAndValues...)...).Error(err, msg)
}

func (l *ControllerLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	return &ControllerLogSink{
		log: l.log.WithFields(getFields(keysAndValues...)...),
	}
}

func (l *ControllerLogSink) WithName(name string) logr.LogSink {
	return &ControllerLogSink{
		log: l.log.GetLogger(name),
	}
}

var _ logr.LogSink = (*ControllerLogSink)(nil)

func getFields(keysAndValues ...interface{}) []logging.Field {
	fields := make([]logging.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues); i += 2 {
		key := keysAndValues[i]
		value := keysAndValues[i+1]
		field := logging.String(fmt.Sprint(key), fmt.Sprint(value))
		fields = append(fields, field)
	}
	return fields
}
