package flicrent

import (
	"context"

	"cloud.google.com/go/bigquery"
	fp "github.com/Ulbora/FlicPrep"
)

//Rent Rent
type Rent interface {
	EntFlic(recs *[]fp.Flic) (bool, int64)
	CreateTable(tableName string) bool
	SetClient(clt *bigquery.Client)
	SetContext(ctx context.Context)
}

//go mod init github.com/Ulbora/flicrent
