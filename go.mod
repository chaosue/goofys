module github.com/kahing/goofys

go 1.14

require (
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-sdk-for-go v32.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.7.1-0.20190724222048-33c102d4ffd2
	github.com/Azure/go-autorest/autorest v0.9.2
	github.com/Azure/go-autorest/autorest/adal v0.6.1-0.20191007213730-d9a171ca366f
	github.com/Azure/go-autorest/autorest/azure/auth v0.3.1-0.20191007213730-d9a171ca366f
	github.com/Azure/go-autorest/autorest/azure/cli v0.3.1-0.20191007213730-d9a171ca366f
	github.com/Azure/go-autorest/autorest/to v0.3.1-0.20191007213730-d9a171ca366f // indirect
	github.com/Azure/go-autorest/autorest/validation v0.2.1-0.20191007213730-d9a171ca366f // indirect
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/aws/aws-sdk-go v1.17.14-0.20190307201122-7e7bb328b06e
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/google/uuid v1.1.1
	github.com/jacobsa/fuse v0.0.0-20200423191118-1d001802f70a
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/sevlyar/go-daemon v0.1.5
	github.com/shirou/gopsutil v2.20.4+incompatible
	github.com/sirupsen/logrus v1.6.0
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/urfave/cli v1.22.4
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550 // indirect
	golang.org/x/net v0.0.0-20200226121028-0de0cce0169b // indirect
	golang.org/x/sys v0.0.0-20200511232937-7e40ca221e25
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
	gopkg.in/ini.v1 v1.56.0
)

replace github.com/jacobsa/fuse v0.0.0-20200423191118-1d001802f70a => github.com/kahing/fusego v0.0.0-20200327063725-ca77844c7bcc
