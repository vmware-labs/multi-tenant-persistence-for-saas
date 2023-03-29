package datastore_test

import (
	"fmt"
	"log"
	"math/rand"
	"time"

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

func ExampleDataStore_noInstancer() {
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

	if err := ds.Register(DevInstanceCtx, roleMapping, &xUser{}); err != nil {
		log.Fatalf("Failed to create DB tables: %+v", err)
	}

	// Insert with appropriate Dev Instance Ctx
	rowsAffected, err := ds.Insert(DevInstanceCtx, p1)
	fmt.Println(rowsAffected, err)

	// Insert with appropriate Prod Instance Ctx
	rowsAffected, err = ds.Insert(ProdInstanceCtx, p2)
	fmt.Println(rowsAffected, err)

	// Insert with appropriate Dev Instance Ctx
	rowsAffected, err = ds.Insert(DevInstanceCtx, p3)
	fmt.Println(rowsAffected, err)

	q1 := &xUser{Id: uId}
	err = ds.Find(DevInstanceCtx, q1)
	fmt.Println(q1, err)

	q2 := &xUser{Id: uId}
	err = ds.Find(ProdInstanceCtx, q2)
	fmt.Println(q2, err)

	// Find using valid Dev Instance Ctx
	q3 := &xUser{Id: "P3"}
	err = ds.Find(DevInstanceCtx, q3)
	fmt.Println(q3, err)

	// Find using invalid Prod Instance Ctx
	q4 := &xUser{Id: "P3"}
	err = ds.Find(ProdInstanceCtx, q4)
	fmt.Println(q4, err)

	rowsAffected, err = ds.Delete(DevInstanceCtx, q1)
	fmt.Println(rowsAffected, err)
	rowsAffected, err = ds.Delete(ProdInstanceCtx, q2)
	fmt.Println(rowsAffected, err)
	// Delete using valid Dev Instance Ctx
	rowsAffected, err = ds.Delete(DevInstanceCtx, q3)
	fmt.Println(rowsAffected, err)
	// Delete  using invalid Prod Instance Ctx
	rowsAffected, err = ds.Delete(ProdInstanceCtx, q4)
	fmt.Println(rowsAffected, err)
	// Output:
	// 1 <nil>
	// 0 SQL statement could not be executed: ERROR: duplicate key value violates unique constraint "x_users_pkey" (SQLSTATE 23505); commit unexpectedly resulted in rollback
	// 1 <nil>
	// Bob: 31 <nil>
	// Bob: 31 <nil>
	// Pat: 39 <nil>
	// Pat: 39 <nil>
	// 1 <nil>
	// 0 <nil>
	// 1 <nil>
	// 0 <nil>
}
