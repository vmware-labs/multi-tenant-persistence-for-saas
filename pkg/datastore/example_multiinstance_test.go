package datastore_test

import (
	"context"
	"fmt"
	"log"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
)

type Person struct {
	Id         string `gorm:"primaryKey"`
	Name       string
	Age        int
	InstanceId string `gorm:"primaryKey"`
}

func (p Person) String() string {
	return fmt.Sprintf("[%s/%s] %s: %d", p.InstanceId, p.Id, p.Name, p.Age)
}

// Example for the multi-instance feature, illustrates one can create records for different instances
// and that each instance context can access the data that belongs to specific instance only.
func ExampleDataStore_multiInstance() {
	uId := "P1337"
	p1 := &Person{uId, "Bob", 31, "Dev"}
	p2 := &Person{uId, "John", 36, "Prod"}
	p3 := &Person{"P3", "Pat", 39, "Dev"}

	SERVICE_ADMIN := "service_admin"
	SERVICE_AUDITOR := "service_auditor"
	mdAuthorizer := authorizer.MetadataBasedAuthorizer{}
	instancer := authorizer.SimpleInstancer{}

	ServiceAdminCtx := mdAuthorizer.GetAuthContext("", SERVICE_ADMIN)
	DevInstanceCtx := instancer.WithInstanceId(ServiceAdminCtx, "Dev")
	ProdInstanceCtx := instancer.WithInstanceId(ServiceAdminCtx, "Prod")

	// Initializes the Datastore using the metadata authorizer and connection details obtained from the ENV variables.
	ds, err := datastore.FromEnv(datastore.GetCompLogger(), mdAuthorizer, instancer)
	if err != nil {
		log.Fatalf("datastore initialization from env errored: %s", err)
	}

	// Registers the necessary structs with their corresponding role mappings.
	roleMapping := map[string]dbrole.DbRole{
		SERVICE_AUDITOR: dbrole.READER,
		SERVICE_ADMIN:   dbrole.WRITER,
	}
	if err = ds.Register(context.TODO(), roleMapping, &Person{}); err != nil {
		log.Fatalf("Failed to create DB tables: %+v", err)
	}

	// Inserts a record with a given Id (uId) using the context of the Dev instance.
	rowsAffected, err := ds.Insert(DevInstanceCtx, p1)
	fmt.Println(rowsAffected, err)
	// Inserts another record with the same uId using the context of the Prod instance.
	rowsAffected, err = ds.Insert(ProdInstanceCtx, p2)
	fmt.Println(rowsAffected, err)
	// Inserts a third record with a different uId using the context of the Dev instance.
	rowsAffected, err = ds.Insert(DevInstanceCtx, p3)
	fmt.Println(rowsAffected, err)

	// Finds a record using the context of the Dev instance and the specified uId.
	q1 := &Person{Id: uId}
	err = ds.Find(DevInstanceCtx, q1)
	fmt.Println(q1, err)
	// Finds a record using the context of the Prod instance and the same uId.
	q2 := &Person{Id: uId}
	err = ds.Find(ProdInstanceCtx, q2)
	fmt.Println(q2, err)
	// Finds a record using the correct context of the Dev instance.
	q3 := &Person{Id: "P3"}
	err = ds.Find(DevInstanceCtx, q3)
	fmt.Println(q3, err)
	// Attempts to find a record using the incorrect context of the Prod instance and should error out.
	q4 := &Person{Id: "P3"}
	err = ds.Find(ProdInstanceCtx, q4)
	fmt.Println(err)

	// Deletes a record using the context of the Dev instance and the specified uId.
	rowsAffected, err = ds.Delete(DevInstanceCtx, q1)
	fmt.Println(rowsAffected, err)
	// Deletes a record using the context of the Prod instance and the same uId.
	rowsAffected, err = ds.Delete(ProdInstanceCtx, q2)
	fmt.Println(rowsAffected, err)
	// Attempts to delete a record using an invalid context of the Prod instance, which should not affect the database.
	rowsAffected, err = ds.Delete(ProdInstanceCtx, q4)
	fmt.Println(rowsAffected, err)
	// Deletes a record using a valid context of the Dev instance.
	rowsAffected, err = ds.Delete(DevInstanceCtx, q3)
	fmt.Println(rowsAffected, err)
	// Output:
	// 1 <nil>
	// 1 <nil>
	// 1 <nil>
	// [Dev/P1337] Bob: 31 <nil>
	// [Prod/P1337] John: 36 <nil>
	// [Dev/P3] Pat: 39 <nil>
	// Unable to locate record [record=[/P3] : 0]
	//	record not found
	// 1 <nil>
	// 1 <nil>
	// 0 <nil>
	// 1 <nil>
}
