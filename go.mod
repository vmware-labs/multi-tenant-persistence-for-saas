module github.com/vmware-labs/multi-tenant-persistence-for-saas

go 1.18

require (
	github.com/bxcodec/faker/v3 v3.8.1
	github.com/bxcodec/faker/v4 v4.0.0-beta.3
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.9
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.9.0
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.2
	gorm.io/driver/postgres v1.5.7
	gorm.io/gorm v1.25.7
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgx/v5 v5.5.4 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace gorm.io/gorm => github.com/go-gorm/gorm v1.25.7
