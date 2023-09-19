package protostore_test

import (
	"context"
	"fmt"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

func ExampleProtoStore() {
	// Initialize protostore with proper logger, authorizer and datastore
	myLogger := datastore.GetCompLogger()
	mdAuthorizer := authorizer.MetadataBasedAuthorizer{}
	myDatastore, _ := datastore.FromEnvWithDB(myLogger, mdAuthorizer, nil, "ExampleProtoStore")
	defer myDatastore.Reset()
	ctx := mdAuthorizer.GetAuthContext("Coke", "service_admin")
	myProtostore := protostore.GetProtoStore(myLogger, myDatastore)

	// Register protobufs with proper roleMappings
	roleMappingForMemory := map[string]dbrole.DbRole{
		"service_auditor": dbrole.READER,
		"service_admin":   dbrole.WRITER,
	}
	_ = myProtostore.Register(context.TODO(), roleMappingForMemory, &pb.Memory{})

	// Store protobuf message using Upsert
	id := "0001"
	memory := &pb.Memory{
		Brand: "Samsung",
		Size:  32,
		Speed: 2933,
		Type:  "DDR4",
	}
	rowsAffected, metadata, err := myProtostore.Upsert(ctx, id, memory)
	fmt.Println("After Upsert::", "revision:", metadata.Revision, "rowsAffected:", rowsAffected, "err:", err)

	// Retrieve protobuf message using ID
	memory = &pb.Memory{}
	err = myProtostore.FindById(ctx, id, memory, &metadata)
	fmt.Println("FindById::", "revision:", metadata.Revision, "rowsAffected:", rowsAffected, "err:", err)

	// Update the protobuf message with metadata (existing revision)
	memory.Speed++
	rowsAffected, metadata, err = myProtostore.UpdateWithMetadata(ctx, id, memory, metadata)
	fmt.Println("After UpdateWithMetadata::", "revision:", metadata.Revision, "rowsAffected:", rowsAffected, "err:", err)

	// Retrieve all the protobuf messages
	queryResults := make([]*pb.Memory, 0)
	metadataMap, err := myProtostore.FindAll(ctx, &queryResults, datastore.NoPagination())
	fmt.Println("FindAll::", "revision:", metadataMap[id].Revision, "rowsFound:", len(queryResults), "err:", err)

	// Delete the protobuf (soft delete)
	rowsAffected, err = myProtostore.SoftDeleteById(ctx, id, &pb.Memory{})
	fmt.Println("SoftDeleteById::", "rowsAffected:", rowsAffected, "err:", err)

	// Delete the protobuf (full delete)
	rowsAffected, err = myProtostore.DeleteById(ctx, id, &pb.Memory{})
	fmt.Println("DeleteById::", "rowsAffected:", rowsAffected, "err:", err)

	// Output:
	// After Upsert:: revision: 1 rowsAffected: 1 err: <nil>
	// FindById:: revision: 1 rowsAffected: 1 err: <nil>
	// After UpdateWithMetadata:: revision: 2 rowsAffected: 1 err: <nil>
	// FindAll:: revision: 2 rowsFound: 1 err: <nil>
	// SoftDeleteById:: rowsAffected: 1 err: <nil>
	// DeleteById:: rowsAffected: 1 err: <nil>
}
