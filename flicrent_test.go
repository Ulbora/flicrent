package flicrent

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	frr "github.com/Ulbora/FileReader"
	fpp "github.com/Ulbora/FlicPrep"
	"google.golang.org/api/option"
)

var r Rent
var fr FlicRent

func TestFlicRent_CreateTable(t *testing.T) {
	//var fr FlicRent
	fr.DatasetName = "ulboralabs"
	//fr.TableName = "test1234"
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "august-gantry-192521", option.WithCredentialsFile("../gcpCreds.json"))
	if err != nil {
		fmt.Println("bq err: ", err)
	} else {
		fr.Client = client
		fr.Ctx = ctx
		//fr.SleepTime = 20

		rand.Seed(time.Now().UnixNano())
		chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
			"abcdefghijklmnopqrstuvwxyz" +
			"0123456789")
		length := 8
		var b strings.Builder
		for i := 0; i < length; i++ {
			b.WriteRune(chars[rand.Intn(len(chars))])
		}
		str := b.String() //
		fmt.Println("table name: ", str)

		r = fr.GetNew()
		suc := r.CreateTable(str)
		if !suc {
			t.Fail()
		}
	}

}

func TestFlicRent_EntFlic(t *testing.T) {

	var cr frr.CsvFileReader
	sourceFile, err := ioutil.ReadFile("../full_file.csv")
	fmt.Println("readFile err: ", err)
	rd := cr.GetNew()
	rec := rd.ReadCsvFile(sourceFile)
	fmt.Println("csv err: ", rec.CsvReadErr)
	fmt.Println("csv len: ", len(rec.CsvFileList))
	var fp fpp.FlicPrep
	fpi := fp.GetNew()
	recs := fpi.PrepRecords(rec)
	fmt.Println("Flic len: ", len(*recs))
	var st int = 20
	r.SetSleepTime(time.Duration(st))

	suc, total := r.EntFlic(recs)
	fmt.Println("total records: ", total)
	if !suc {
		t.Fail()
	}

}
