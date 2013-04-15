package gosphinx

import (
	"fmt"
	"testing"
)

var (
	sqlPort = 9306
	rtIndex = "rt"
	amount  = 5
)

// Same as rt index
type rtData struct {
	Id       int
	Title    string
	Content  string
	Group_id int
}

// Truncate the rt index first.
func TestTruncate(t *testing.T) {
	fmt.Println("Running Truncate() test ...")

	sqlc := NewSQLClient().Server(host, sqlPort)
	if err := sqlc.TruncateRT(rtIndex); err != nil {
		t.Fatalf("TestTruncate > %v\n", err)
	}
}

func TestInsert(t *testing.T) {
	fmt.Println("Running Insert() test...")

	sqlc := NewSQLClient().Server(host, sqlPort).Index(rtIndex)
	for i := 1; i <= amount; i++ {
		rtd := rtData{i, "test title", "test content", i * 100}
		if err := sqlc.Insert(&rtd); err != nil {
			t.Fatalf("TestInsert > %v\n", err)
		}
	}

	sc := NewClient().Server(host, port)
	res, err := sc.Query("test", rtIndex, "test rt insert")
	if err != nil {
		t.Fatalf("TestInsert > %v\n", err)
	}

	if len(res.Matches) != amount {
		t.Fatalf("TestInsert > Matches: %v\n", res.Matches)
	}
}

func TestReplace(t *testing.T) {
	fmt.Println("Running Replace() test...")
	sqlc := NewSQLClient().Server(host, sqlPort).Index(rtIndex)
	sqlc.columns = []string{"Id", "Title", "Group_id"}

	testId := 1
	data := rtData{
		Id:       testId,
		Title:    "replaced' title",
		Content:  "replaced content",
		Group_id: 1000,
	}
	if err := sqlc.Replace(&data); err != nil {
		t.Fatalf("TestReplace > %v\n", err)
	}

	sc := NewClient().Server(host, port)
	res, err := sc.Query("replaced", rtIndex, "test rt replace")
	if err != nil {
		t.Fatalf("TestReplace > %v\n", err)
	}

	// Replace
	if len(res.Matches) != 1 || int(res.Matches[0].DocId) != testId {
		t.Fatalf("TestReplace > Matches: %v\n", res.Matches)
	}
}

func TestUpdate(t *testing.T) {
	fmt.Println("Running Update() test...")

	testId := 2
	testGroupId := 2000
	data := rtData{
		Id:       testId,
		Group_id: testGroupId,
	}

	// Update DocId(2)
	sqlc := NewSQLClient().Server(host, sqlPort).Index(rtIndex).Columns("Group_id")
	rowsAffected, err := sqlc.Update(&data)
	if err != nil {
		t.Fatalf("TestUpdate > %v\n", err)
	}

	if rowsAffected != 1 {
		t.Fatalf("TestUpdate > rowsAffected: %d\n", rowsAffected)
	}

	sc := NewClient().Server(host, port).Filter("Group_id", []uint64{uint64(testGroupId)}, false)
	res, err := sc.Query("", rtIndex, "test rt update")
	if err != nil {
		t.Fatalf("TestUpdate > %v\n", err)
	}

	if len(res.Matches) != 1 || int(res.Matches[0].DocId) != testId {
		t.Fatalf("TestUpdate > Matches: %v\n", res.Matches)
	}
}

func TestDelete(t *testing.T) {
	fmt.Println("Running Delete() test...")

	sqlc := NewSQLClient().Server(host, sqlPort).Index(rtIndex)
	// Delete the last one.
	rowsAffected, err := sqlc.Delete(amount)
	if err != nil {
		t.Fatalf("TestDelete > %v\n", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("TestDelete > rowsAffected: %d\n", rowsAffected)
	}

	sc := NewClient().Server(host, port)
	res, err := sc.Query("", rtIndex, "test rt delete")
	if err != nil {
		t.Fatalf("TestDelete > %v\n", err)
	}

	if len(res.Matches) != amount-1 {
		t.Fatalf("TestDelete > Matches: %v\n", res.Matches)
	}

	// Test batch delete
	sqlc = NewSQLClient().Server(host, sqlPort).Index(rtIndex)
	// Delete 3,4
	rowsAffected, err = sqlc.Delete([]int{amount - 1, amount - 2})
	if err != nil {
		t.Fatalf("TestDelete > %v\n", err)
	}
	if rowsAffected != 2 {
		t.Fatalf("TestDelete > rowsAffected: %d\n", rowsAffected)
	}

	sc = NewClient().Server(host, port)
	res, err = sc.Query("", rtIndex, "test rt delete")
	if err != nil {
		t.Fatalf("TestDelete > %v\n", err)
	}

	if len(res.Matches) != amount-3 {
		t.Fatalf("TestDelete > Matches: %v\n", res.Matches)
	}
}

/*
mysql> select * from rt;
+------+----------+
| id   | group_id |
+------+----------+
|    1 |     1000 |
|    2 |     2000 |
+------+----------+
2 rows in set (0.00 sec)
*/

// Note: The test would distroy "index1", you need reindex "index1" manually!
func TestRTCommand(t *testing.T) {
	fmt.Println("Running RT commands test ...")

	sqlc := NewSQLClient().Server(host, sqlPort)
	// ATTACH currently supports empty target RT indexes only, so truncate it first.
	if err := sqlc.TruncateRT(rtIndex); err != nil {
		t.Fatalf("Test TruncateRT > %v\n", err)
	}

	sqlc = NewSQLClient().Server(host, sqlPort)
	if err := sqlc.AttachToRT(index, rtIndex); err != nil {
		t.Fatalf("Test AttachToRT > %v\n", err)
	}

	sqlc = NewSQLClient().Server(host, sqlPort)
	if err := sqlc.FlushRT(rtIndex); err != nil {
		t.Fatalf("Test FlushRT > %v\n", err)
	}

	sqlc = NewSQLClient().Server(host, sqlPort)
	if err := sqlc.Optimize(rtIndex); err != nil {
		t.Fatalf("Test Optimize > %v\n", err)
	}
}

/*
mysql> select * from rt;
+------+----------+----------+-----------+------------+----------+-----------+
| id   | cate_ids | group_id | group_id2 | date_added | latitude | longitude |
+------+----------+----------+-----------+------------+----------+-----------+
|    1 | 1        |        3 |        15 | 1326178239 | 0.521377 |  2.121630 |
|    2 | 1,2      |        4 |        16 | 1326178239 | 0.521206 |  2.121232 |
|    3 | 1,2,3    |        2 |         7 | 1326178239 | 0.521377 |  2.121630 |
|    4 | 1,2,3,4  |        2 |         8 | 1326178239 | 0.523264 |  2.125200 |
|    5 |          |        0 |         0 |          0 | 0.546671 |  2.127820 |
+------+----------+----------+-----------+------------+----------+-----------+
5 rows in set (0.01 sec)
*/
