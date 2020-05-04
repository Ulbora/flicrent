package flicrent

import (
	fp "github.com/Ulbora/FlicPrep"
)

//Rent Rent
type Rent interface {
	EntFlic(recs *[]fp.Flic) (bool, int64)
	CreateTable(tableName string) bool
}

//go mod init github.com/Ulbora/flicrent
