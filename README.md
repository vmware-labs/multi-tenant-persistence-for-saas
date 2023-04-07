# multi-tenant-persistence-for-saas

[![Go Report Card](https://goreportcard.com/badge/github.com/vmware-labs/multi-tenant-persistence-for-saas)](https://goreportcard.com/report/github.com/vmware-labs/multi-tenant-persistence-for-saas)
[![GitHub Actions](https://github.com/vmware-labs/multi-tenant-persistence-for-saas/actions/workflows/go.yml/badge.svg)](https://github.com/vmware-labs/multi-tenant-persistence-for-saas/actions?query=branch%3Amaster)
[![Go Reference](https://pkg.go.dev/badge/github.com/vmware-labs/multi-tenant-persistence-for-saas/)](https://pkg.go.dev/github.com/vmware-labs/multi-tenant-persistence-for-saas)
[![Code Coverage](https://codecov.io/gh/vmware-labs/multi-tenant-persistence-for-saas/branch/main/graph/badge.svg?token=F7TQPSFEMCN)](https://app.codecov.io/gh/vmware-labs/multi-tenant-persistence-for-saas)
[![Daily](https://github.com/vmware-labs/multi-tenant-persistence-for-saas/actions/workflows/daily.yml/badge.svg)](https://github.com/vmware-labs/multi-tenant-persistence-for-saas/actions/workflows/daily.yml)


## Overview

Multi-tenant Persistence for SaaS services acts as data abstraction layer for
underlying data store (Postgres) and provide multi-tenancy
capabilities along with ability to integrate with different IAM authorizers.

## Introduction

This repo contains an implementation of a data access layer (DAL) that will be
used by SaaS microservices. It is a Go library that could be used in other
projects and supports data store backed by a Postgres database.

A sample use case for a multi-tenant application using this library to read VM
information stored in a Postgres database:

```mermaid
sequenceDiagram
    actor C1 as Coke User (Americas)
    actor C2 as Coke User (Europe)
    actor P as Pepsi User (Americas)
    participant M as Multitenant Persistence
    participant A as Authorizer(+Instancer)
    participant DB as Postgres

    rect rgb(200, 225, 250)
    C1 ->>+ M: Find VM1
    M ->>+ A: GetOrgID
    A -->>- M: Coke
    M ->>+ A: GetInstanceID
    A -->>- M: Americas
    M ->>+ DB: set_config(org_id=Coke, instance_id=Americas)
    M ->>+ DB: SELECT * FROM VM WHERE ID=VM1
    DB -->>- M: | CokeWebServer1 | ID=VM1 | us-west-1|
    M -->>- C1: Return {Name=CokeWebServer1, region=us-west-1, ID=VM1}
    end

    rect rgb(225, 250, 250)
    C2 ->>+ M: Find VM1
    M ->>+ A: GetOrgID
    A -->>- M: Coke
    M ->>+ A: GetInstanceID
    A -->>- M: Europe
    M ->>+ DB: set_config(org_id=Coke, instance_id=Europe)
    M ->>+ DB: SELECT * FROM VM WHERE ID=VM1
    DB -->>- M: | CokeWebServer1 | ID=VM1 | eu-central-1 |
    M -->>- C2: Return {Name=CokeWebServer1, region=eu-central-1, ID=VM1}
    end

    rect rgb(200, 250, 225)
    P ->>+ M: Find VM1
    M ->>+ A: GetOrgID
    A -->>- M: Pepsi
    M ->>+ A: GetInstanceID
    A -->>- M: Americas
    M ->>+ DB: set_config(org_id=Pepsi, instance_id=Americas)
    M ->>+ DB: SELECT * FROM VM WHERE ID=VM1
    DB -->>- M: | PepsiWebServer1 | ID=VM1 | us-west-2 |
    M -->>- P: Return {Name=PepsiWebServer1, region=us-west-2, ID=VM1}
    end
```

## Features

Currently, following features are supported:

- **CRUD operations** on persisted objects, where data is persisted in Postgres
  database

- **Multi-tenancy** persistence is supported with `org_id` as the column used
 .for accessing data for different tenants using row-level security (RLS)
  feature of Postgres. A pluggable `Authorizer` interface to support multi-tenancy.
  User's with tenant-specific roles (`TENANT_WRITER`, `TENANT_READER`) will be
  able to access only their own tenant's data.

- **Role-based access control (RBAC)** based on the role mappings from the
 .user to DBRole Mappings.

- **Metadata support** like CreatedAt, UpdatedAt, DeletedAt (using `gorm.Model`)

- **Revisioning** is supported if a record being persistent has a field named
 .`revision`. Among concurrent updates on same revision of the record only
 .one of the operations would succeed.

- **Multi-instance** persistence is supported with `instance_id` as the column used
  for accessing data for different deployment instances using row-level security
  (RLS) feature of Postgres. `Instancer` interface is used to support multi-instances.
 .If instancer is not configured `instance_id` column doesnt have any special meaning
 .and treated as normal attribute.



## Documentation

Refer to [DOCUMENTATION.md](docs/DOCUMENTATION.md) for the interfaces exposed like `Datastore`, `Authorizer`, `Protostore`

## Future Support

- Support for services to subscribe for updates to tables.

## Contributing

The multi-tenant-persistence-for-saas project team welcomes contributions from the community. Before you start
working with multi-tenant-persistence-for-saas, please read our [CONTRIBUTING.md](CONTRIBUTING_CLA.md). All
contributions to this repository must be signed as described on that page. Your signature certifies that you
wrote the patch or have the right to pass it on as an open-source patch. For more detailed information,
refer to [CONTRIBUTING.md](CONTRIBUTING_CLA.md).

## License

Refer to [LICENSE](./LICENSE)
