package flicrent

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	fp "github.com/Ulbora/FlicPrep"
)

//Flic Flic
type Flic struct {
	Key            string    `bigquery:"key"`
	Lic            string    `bigquery:"lic"`
	ExpDate        time.Time `bigquery:"exp_date"`
	LicName        string    `bigquery:"lic_name"`
	BusName        string    `bigquery:"bus_name"`
	PremiseAddress string    `bigquery:"premise_address"`
	PremiseZip     string    `bigquery:"premise_zip"`
	MailingAddress string    `bigquery:"mailing_address"`
	Phone          string    `bigquery:"phone"`
}

//FlicRent FlicRent
type FlicRent struct {
	Ctx         context.Context
	Client      *bigquery.Client
	DatasetName string
	TableName   string
}

//GetNew GetNew
func (f *FlicRent) GetNew() Rent {
	return f
}

//EntFlic EntFlic
func (f *FlicRent) EntFlic(recs *[]fp.Flic) (bool, int64) {
	var rtn = true
	var tot int64
	var flics []Flic
	for _, r := range *recs {
		bqr := f.prepRecord(&r)
		flics = append(flics, *bqr)
	}
	u := f.Client.Dataset(f.DatasetName).Table(f.TableName).Inserter()
	var wg sync.WaitGroup
	var cnt int
	for _, flic := range flics {
		cnt++
		if cnt >= 10 {
			cnt = 0
			time.Sleep(10 * time.Millisecond)
		}
		wg.Add(1)
		go func(val Flic) {
			defer wg.Done()
			err := u.Put(f.Ctx, val)
			if err != nil {
				log.Println("big query put err:", err, val)
				rtn = false
			} else {
				atomic.AddInt64(&tot, 1)
			}
		}(flic)
	}
	wg.Wait()

	return rtn, tot
}

func (f *FlicRent) prepRecord(rec *fp.Flic) *Flic {
	var rtn Flic
	rtn.Key = rec.Key
	rtn.Lic = rec.Lic
	rtn.LicName = rec.LicName
	rtn.BusName = rec.BusName
	rtn.ExpDate = rec.ExpDate
	rtn.PremiseAddress = rec.PremiseAddress
	rtn.PremiseZip = rec.PremiseZip
	rtn.Phone = rec.Phone
	rtn.MailingAddress = rec.MailingAddress
	return &rtn
}

//CreateTable CreateTable
func (f *FlicRent) CreateTable(tableName string) bool {
	var rtn bool

	sampleSchema := bigquery.Schema{
		{Name: "key", Type: bigquery.StringFieldType},
		{Name: "lic", Type: bigquery.StringFieldType},
		{Name: "exp_date", Type: bigquery.TimestampFieldType},
		{Name: "lic_name", Type: bigquery.StringFieldType},
		{Name: "bus_name", Type: bigquery.StringFieldType},
		{Name: "premise_address", Type: bigquery.StringFieldType},
		{Name: "premise_zip", Type: bigquery.StringFieldType},
		{Name: "mailing_address", Type: bigquery.StringFieldType},
		{Name: "phone", Type: bigquery.StringFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         sampleSchema,
		ExpirationTime: time.Now().AddDate(0, 2, 0), // Table will be automatically deleted in 2 months.
	}
	tableRef := f.Client.Dataset(f.DatasetName).Table(tableName)
	if err := tableRef.Create(f.Ctx, metaData); err != nil {
		log.Println("Create table error: ", err)
	} else {
		rtn = true
		f.TableName = tableName
	}
	return rtn
}
