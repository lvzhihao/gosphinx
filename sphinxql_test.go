package gosphinx

import (
	"fmt"
	"testing"
	"base"
	"time"
)

var (
	sql_port  = 3306
	sql_index = "rt"
)

// Same as rt index
type rtData struct {
	Id int
	Title string
	Content string
	Group_id int
}

func TestInitSQLClient(t *testing.T) {
	fmt.Println("Init SphinxQL client ...")
	sc = NewSphinxQLClient()
	if err := sc.Server(host, sql_port).Index(sql_index).Error(); err != nil {
		t.Fatalf("TestInitSQLClient > %v\n", err)
	}
}

func TestTruncate(t *testing.T) {
	fmt.Println("Running Truncate() test ...")
	
	if err := sc.TruncateRT(sql_index); err != nil {
		t.Fatalf("TestTruncate > %v\n", err)
	}
}

type dbTest struct {
	DbTestBasic
	ColString  string
	ColBytes   []byte
	ColBool    bool
	UpdatedTime  uint "TIME"
	UpdatedTimeStamp  int64 "TIMESTAMP"
	CreateTime time.Time
	UpdateTime time.Time
}

type DbTestBasic struct {
	Id       int
	ColInt   int
	ColFloat float64
}
func TestInsert(t *testing.T) {
	fmt.Println("Running Insert() test...")
	
	rtd := rtData {1, "first record", "test one", 123}
	err := sc.Insert(&rtd)
	if err != nil {
		t.Fatalf("TestInsert > %v\n", err)
	}
	
	base.MySqlAddress = "192.168.1.234:3306"
	base.MySqlPassword ="cpp114"
	sq := base.MySqlQuery{
		Database: "test",
		Table: "db_test",
	}
	dbTest1 := dbTest{DbTestBasic{0, 1, 0.1}, "str'ing1", nil, false, 0, 0, time.Time{}, time.Time{}}

	myid, err := sq.Insert(&dbTest1)
	if err != nil {
		t.Fatalf("TestInsert > %s\n", err)
	}
	fmt.Println("mysql insert id:", myid)
	
}
/*
func TestReplace(t *testing.T) {
	fmt.Println("Running Replace() test...")
	sc.columns = []string{"Id", "Title"}
	
	data := rtData{
		Id: 2,
	}
	data.Title = "Replace title!"
	rowsAffected, err := sc.Replace(&data)
	if err != nil {
		t.Fatalf("TestReplace > %v\n", err)
	}
	fmt.Printf("rowsAffected: %d\n", rowsAffected)
	
}

func TestDelete(t *testing.T) {
	fmt.Println("Running Delete() test...")
	
	rowsAffected, err := sc.Delete(2)
	if err != nil {
		t.Fatalf("TestDelete > %v\n", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("TestDelete > rowsAffected: %d\n", rowsAffected)
	}
}
*/