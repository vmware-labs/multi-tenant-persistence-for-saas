package datastore_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
)

type xUser struct {
	Id         string `gorm:"primaryKey"`
	Name       string
	Age        int
	InstanceId string
}

func (p xUser) String() string {
	return fmt.Sprintf("%s: %d", p.Name, p.Age)
}

func TestDataStoreWithoutInstancer(t *testing.T) {
	assert := assert.New(t)
	rand.Seed(time.Now().UnixNano())
	uId := fmt.Sprintf("P%d", rand.Intn(1_000_0000))
	p1 := &xUser{uId, "Bob", 31, "Dev"}
	p2 := &xUser{uId, "John", 36, "Prod"}
	p3 := &xUser{"P3", "Pat", 39, "Dev"}

	mdAuthorizer := authorizer.MetadataBasedAuthorizer{}
	instancer := authorizer.SimpleInstancer{}

	DevInstanceCtx := instancer.WithInstanceId(ServiceAdminCtx, "Dev")
	ProdInstanceCtx := instancer.WithInstanceId(ServiceAdminCtx, "Prod")

	ds, _ := datastore.FromEnv(datastore.GetCompLogger(), mdAuthorizer, nil)
	roleMapping := map[string]dbrole.DbRole{
		SERVICE_AUDITOR: dbrole.READER,
		SERVICE_ADMIN:   dbrole.WRITER,
	}

	if err := ds.Register(context.TODO(), roleMapping, &xUser{}); err != nil {
		log.Fatalf("Failed to create DB tables: %+v", err)
	}

	// Insert with Dev Instance Ctx
	rowsAffected, err := ds.Insert(DevInstanceCtx, p1)
	assert.EqualValues(1, rowsAffected)
	assert.NoError(err)

	// Insert with Prod Instance Ctx will fail for same Id
	rowsAffected, err = ds.Insert(ProdInstanceCtx, p2)
	assert.EqualValues(0, rowsAffected)
	assert.Contains(err.Error(), "x_users_pkey")
	assert.Contains(err.Error(), datastore.ERROR_DUPLICATE_KEY)

	// Insert with Dev Instance Ctx and new Id
	rowsAffected, err = ds.Insert(DevInstanceCtx, p3)
	assert.EqualValues(1, rowsAffected)
	assert.NoError(err)

	// Find using Dev Instance Ctx
	q1 := &xUser{Id: uId}
	err = ds.Find(DevInstanceCtx, q1)
	assert.Equal("Bob", q1.Name)
	assert.NoError(err)
	// Find using some other Prod Instance Ctx
	q2 := &xUser{Id: uId}
	err = ds.Find(ProdInstanceCtx, q2)
	assert.Equal("Bob", q2.Name)
	assert.NoError(err)

	// Find using Dev Instance Ctx
	q3 := &xUser{Id: "P3"}
	err = ds.Find(DevInstanceCtx, q3)
	assert.Equal("Pat", q3.Name)
	assert.NoError(err)
	// Find using some other Prod Instance Ctx
	q4 := &xUser{Id: "P3"}
	err = ds.Find(ProdInstanceCtx, q4)
	assert.Equal("Pat", q4.Name)
	assert.NoError(err)

	// Delete using some other ProdInstanceCtx
	rowsAffected, err = ds.Delete(ProdInstanceCtx, q2)
	assert.EqualValues(1, rowsAffected)
	assert.NoError(err)
	// Delete using Dev Instance Ctx
	rowsAffected, err = ds.Delete(DevInstanceCtx, q1)
	assert.EqualValues(0, rowsAffected)
	assert.NoError(err)

	// Delete using valid Dev Instance Ctx
	rowsAffected, err = ds.Delete(DevInstanceCtx, q3)
	assert.EqualValues(1, rowsAffected)
	assert.NoError(err)
	// Delete same using some other Prod Instance Ctx
	rowsAffected, err = ds.Delete(ProdInstanceCtx, q4)
	assert.EqualValues(0, rowsAffected)
	assert.NoError(err)
}
