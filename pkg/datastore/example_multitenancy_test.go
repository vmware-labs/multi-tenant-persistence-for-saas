package datastore_test

import (
	"context"
	"fmt"
	"log"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
)

type User struct {
	Id    string `gorm:"primaryKey"`
	Name  string
	Age   int
	OrgId string `gorm:"primaryKey"`
}

func (p User) String() string {
	return fmt.Sprintf("[%s/%s] %s: %d", p.OrgId, p.Id, p.Name, p.Age)
}

// Example for the multi-tenancy feature, illustrates one can create records for different tenants
// and that each tenant context can access the data that belongs to specific tenant only.
func ExampleDataStore_multiTenancy() {
	uId := "P1337"
	p1 := &User{uId, "Bob", 31, "Coke"}
	p2 := &User{uId, "John", 36, "Pepsi"}
	p3 := &User{"P3", "Pat", 39, "Coke"}

	TENANT_ADMIN := "tenant_admin"
	TENANT_AUDITOR := "tenant_auditor"
	mdAuthorizer := authorizer.MetadataBasedAuthorizer{}
	CokeOrgCtx := mdAuthorizer.GetAuthContext("Coke", TENANT_ADMIN)
	PepsiOrgCtx := mdAuthorizer.GetAuthContext("Pepsi", TENANT_ADMIN)

	// Initializes the Datastore using the metadata authorizer and connection details obtained from the ENV variables.
	ds, err := datastore.FromEnv(datastore.GetCompLogger(), mdAuthorizer, nil)
	if err != nil {
		log.Fatalf("datastore initialization from env errored: %s", err)
	}

	// Registers the necessary structs with their corresponding tenant role mappings.
	roleMapping := map[string]dbrole.DbRole{
		TENANT_AUDITOR: dbrole.TENANT_READER,
		TENANT_ADMIN:   dbrole.TENANT_WRITER,
	}
	if err = ds.Register(context.TODO(), roleMapping, &User{}); err != nil {
		log.Fatalf("Failed to create DB tables: %+v", err)
	}

	// Inserts a record with a given Id (uId) using the context of the Coke organization.
	rowsAffected, err := ds.Insert(CokeOrgCtx, p1)
	fmt.Println(rowsAffected, err)
	// Inserts another record with the same uId using the context of the Pepsi organization.
	rowsAffected, err = ds.Insert(PepsiOrgCtx, p2)
	fmt.Println(rowsAffected, err)
	// Inserts a third record with a different uId using the context of the Coke organization.
	rowsAffected, err = ds.Insert(CokeOrgCtx, p3)
	fmt.Println(rowsAffected, err)

	// Finds a record using the context of the Coke organization and the specified uId.
	q1 := &User{Id: uId}
	err = ds.Find(CokeOrgCtx, q1)
	fmt.Println(q1, err)
	// Finds a record using the context of the Pepsi organization and the same uId.
	q2 := &User{Id: uId}
	err = ds.Find(PepsiOrgCtx, q2)
	fmt.Println(q2, err)
	// Finds a record using the correct context of the Coke organization.
	q3 := &User{Id: "P3"}
	err = ds.Find(CokeOrgCtx, q3)
	fmt.Println(q3, err)
	// Attempts to find a record using the incorrect context of the Pepsi organization and should error out.
	q4 := &User{Id: "P3"}
	err = ds.Find(PepsiOrgCtx, q4)
	fmt.Println(err)

	// Deletes a record using the context of the Coke organization and the specified uId.
	rowsAffected, err = ds.Delete(CokeOrgCtx, q1)
	fmt.Println(rowsAffected, err)
	// Deletes a record using the context of the Pepsi organization and the same uId.
	rowsAffected, err = ds.Delete(PepsiOrgCtx, q2)
	fmt.Println(rowsAffected, err)
	// Attempts to delete a record using an invalid context of the Pepsi organization, which should not affect the database.
	rowsAffected, err = ds.Delete(PepsiOrgCtx, q4)
	fmt.Println(rowsAffected, err)
	// Deletes a record using a valid context of the Coke organization.
	rowsAffected, err = ds.Delete(CokeOrgCtx, q3)
	fmt.Println(rowsAffected, err)

	// Output:
	// 1 <nil>
	// 1 <nil>
	// 1 <nil>
	// [Coke/P1337] Bob: 31 <nil>
	// [Pepsi/P1337] John: 36 <nil>
	// [Coke/P3] Pat: 39 <nil>
	// Unable to locate record [record=[/P3] : 0]
	//	record not found
	// 1 <nil>
	// 1 <nil>
	// 0 <nil>
	// 1 <nil>
}
