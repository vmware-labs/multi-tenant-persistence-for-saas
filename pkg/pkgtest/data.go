package pkgtest

import (
	"github.com/google/go-cmp/cmp"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore"
)

// Organizations.
const (
	COKE  = "Coke"
	PEPSI = "Pepsi"
)

// Service roles for test cases.
const (
	TENANT_AUDITOR  = "tenant_auditor"
	TENANT_ADMIN    = "tenant_admin"
	SERVICE_AUDITOR = "service_auditor"
	SERVICE_ADMIN   = "service_admin"
	RANDOM_ID       = "SomeRandomId"
)

var (
	TestMetadataAuthorizer = authorizer.MetadataBasedAuthorizer{}
	ServiceAdminCtx        = TestMetadataAuthorizer.GetAuthContext("", SERVICE_ADMIN)
	CokeAdminCtx           = TestMetadataAuthorizer.GetAuthContext(COKE, TENANT_ADMIN)
	CokeAuditorCtx         = TestMetadataAuthorizer.GetAuthContext(COKE, TENANT_AUDITOR)
	PepsiAdminCtx          = TestMetadataAuthorizer.GetAuthContext(PEPSI, TENANT_ADMIN)
	PepsiAuditorCtx        = TestMetadataAuthorizer.GetAuthContext(PEPSI, TENANT_AUDITOR)

	TestDataStore, _ = datastore.GetDefaultDataStore(datastore.GetLogger(), TestMetadataAuthorizer)
	TestProtoStore   = protostore.GetProtoStore(datastore.GetLogger(), TestDataStore)
)

type AppUser struct {
	Id             string `gorm:"primaryKey;column:user_id"`
	Name           string
	Email          string
	EmailConfirmed bool
	NumFollowing   int32
	NumFollowers   int64
	AppId          string
	Msg            []byte
}

type AppUserSlice []AppUser // Needed for sorting
func (a AppUserSlice) Len() int {
	return len(a)
}

func (a AppUserSlice) Less(x, y int) bool {
	return a[x].Id < a[y].Id
}
func (a AppUserSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type App struct {
	Id       string `gorm:"primaryKey;column:application_id"`
	Name     string
	TenantId string `gorm:"primaryKey;column:org_id"`
}

func (a App) TableName() string {
	return "application"
}

func (a AppUser) AreNonKeyFieldsEmpty() bool {
	a.Id = ""
	return cmp.Equal(a, AppUser{})
}

func (a App) AreNonKeyFieldsEmpty() bool {
	a.Id = ""
	a.TenantId = ""
	return cmp.Equal(a, App{})
}

type Group struct {
	Id       string
	Name     string
	Revision int
}
