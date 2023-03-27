package authorizer

import (
	"context"
	"fmt"

	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
)

type ContextKey string

const (
	INSTANCE_ID = ContextKey("multiinstance.id")
)

type Instancer interface {
	GetInstanceId(ctx context.Context) (string, error)
	WithInstanceId(ctx context.Context, instanceId string) context.Context
}

type SimpleInstancer struct{}

func (s SimpleInstancer) GetInstanceId(ctx context.Context) (string, error) {
	if v := ctx.Value(INSTANCE_ID); v != nil {
		return fmt.Sprintf("%s", v), nil
	}
	return "", ErrMissingInstanceId
}

func (s SimpleInstancer) WithInstanceId(ctx context.Context, instanceId string) context.Context {
	return context.WithValue(ctx, INSTANCE_ID, instanceId)
}
