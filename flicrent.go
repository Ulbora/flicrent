package flicrent

import (
	"context"
	"log"
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

//EntFlic EntFlic
func (f *FlicRent) EntFlic(recs *[]fp.Flic) bool {
	var rtn bool
	var flics []Flic
	for _, r := range *recs {
		bqr := f.prepRecord(&r)
		flics = append(flics, *bqr)
	}
	u := f.Client.Dataset(f.DatasetName).Table(f.TableName).Inserter()
	if err := u.Put(f.Ctx, flics); err != nil {
		log.Println("big query put err:", err)
	} else {
		rtn = true
	}
	return rtn
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
	// projectID := "my-project-id"
	// datasetID := "mydatasetid"
	// tableID := "mytableid"
	// ctx := context.Background()

	// client, err := bigquery.NewClient(ctx, projectID)
	// if err != nil {
	// 		return fmt.Errorf("bigquery.NewClient: %v", err)
	// }
	// defer client.Close()

	sampleSchema := bigquery.Schema{
		{Name: "key", Type: bigquery.StringFieldType},
		{Name: "lic", Type: bigquery.StringFieldType},
		{Name: "exp_date", Type: bigquery.DateTimeFieldType},
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

// func insertRec(ctx context.Context, client *bigquery.Client) bool {
// 	fmt.Println("client:", client)
// 	u := client.Dataset("ffl").Table("ffl_list").Uploader()
// 	fmt.Println(ctx)
// 	fmt.Println(u)

// 	// if err := u.Put(ctx, f); err != nil {
// 	if err := u.Put(ctx, f); err != nil {
// 		fmt.Println("err:", err)
// 	}
// 	return true
// }
