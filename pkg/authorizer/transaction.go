package authorizer

import (
	"context"

	"gorm.io/gorm"
)

type TransactionContextKey string

const (
	TransactionCtx = TransactionContextKey("DB_TRANSACTION")
)

type TransactionFetcher interface {
	IsTransactionCtx(ctx context.Context) bool
	GetTransactionCtx(ctx context.Context) *gorm.DB
	WithTransactionCtx(ctx context.Context, tx *gorm.DB) context.Context
}

type SimpleTransactionFetcher struct{}

func (s SimpleTransactionFetcher) GetTransactionCtx(ctx context.Context) *gorm.DB {
	if v := ctx.Value(TransactionCtx); v != nil {
		if dbTx, ok := v.(*gorm.DB); ok {
			return dbTx
		}
	}
	return nil
}

func (s SimpleTransactionFetcher) WithTransactionCtx(ctx context.Context, tx *gorm.DB) context.Context {
	return context.WithValue(ctx, TransactionCtx, tx)
}

func (s SimpleTransactionFetcher) IsTransactionCtx(ctx context.Context) bool {
	return s.GetTransactionCtx(ctx) != nil
}
