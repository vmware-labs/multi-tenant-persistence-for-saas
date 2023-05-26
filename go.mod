module github.com/vmware-labs/multi-tenant-persistence-for-saas

go 1.18

require (
	github.com/bxcodec/faker/v3 v3.8.0
	github.com/bxcodec/faker/v4 v4.0.0-beta.3
	github.com/google/go-cmp v0.5.9
	github.com/google/uuid v1.3.0
	github.com/lib/pq v1.10.7
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.1
	google.golang.org/grpc v1.53.0-dev
	google.golang.org/protobuf v1.28.1
	gorm.io/driver/postgres v1.4.7
	gorm.io/gorm v1.24.5
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgx/v5 v5.2.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.5.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace gorm.io/gorm => github.com/go-gorm/gorm v1.24.3
