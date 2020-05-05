package flicrent

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"
	fp "github.com/Ulbora/FlicPrep"
)

//Rent Rent
type Rent interface {
	EntFlic(recs *[]fp.Flic) (bool, int64)
	CreateTable(tableName string) bool
	SetClient(clt *bigquery.Client)
	SetContext(ctx context.Context)
	SetSleepTime(sleepTime time.Duration)
}

//go mod init github.com/Ulbora/flicrent
