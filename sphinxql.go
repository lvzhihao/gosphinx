package gosphinx

import(
	_ "github.com/Go-SQL-Driver/MySQL"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const (
	DefaultPK = "Id"
)

func NewSphinxQLClient() (sc *SphinxClient) {
	sc = new(SphinxClient)
	if SQLSocket != "" {
		sc.socket = SQLSocket
	} else {
		sc.host = Host
		sc.port = SQLPort
	}

	sc.limit = Limit
	sc.mode = Mode
	sc.sort = Sort
	sc.groupFunc = GroupFunc
	sc.groupSort = GroupSort
	sc.maxMatches = MaxMatches
	sc.SetConnectTimeout(Timeout)
	sc.ranker = Ranker
	sc.selectStr = SelectStr

	return
}
 

func (sc *SphinxClient) SetIndex(index string) error {
	if index == "" {
		return errors.New("SetIndex > Index name is empty!")
	}

	sc.index = index
	return nil
}
// For chaining
func (sc *SphinxClient) Index(index string) *SphinxClient {
	sc.err = sc.SetIndex(index)
	return sc
}

func (sc *SphinxClient) SetColumns(columns []string) error {
	if len(columns) == 0 {
		return errors.New("SetColumns > Columns is empty!")
	}

	sc.columns = columns
	return nil
}
func (sc *SphinxClient) Columns(columns []string) *SphinxClient {
	sc.err = sc.SetColumns(columns)
	return sc
}

func (sc *SphinxClient) GetDb() (err error) {
	var addr string
	if sc.socket != "" {
		addr = "unix(" + sc.socket + ")"
	} else {
		// Already get default host and port in NewSphinxQLClient()
		addr = "tcp(" + sc.host + ":" +strconv.Itoa(sc.port) + ")"
	}
	
	if sc.DB, err = sql.Open("mysql", addr+"/"); err != nil {
		return err
	}

	return
}

// Caller should close db.
func (sc *SphinxClient) Init(obj interface{}) (err error) {
	// Init sql.DB
	if err = sc.GetDb(); err != nil {
		return fmt.Errorf("Init > %v", err)
	}

	// Get object's reflect.Value
	if obj != nil { //some functions do not need sc.val
		sc.val = reflect.Indirect(reflect.ValueOf(obj))

		// check whether allValue is addressable, 'obj' must be a pointer!
		if !sc.val.CanAddr() {
			return fmt.Errorf("sc.Init> The obj value is unaddressable value")
		}
	}

	// "MyType" to "my_type"
	if sc.index == "" {
		fullTypeName := sc.val.Type().String()	// Such as "main.MyType"
		subStrs := strings.Split(fullTypeName, ".")
		sc.index = subStrs[len(subStrs)-1]	// "MyType"
		sc.index = CamelToSep(sc.index, '_')	// "my_type"
		fmt.Println("sc.index:", sc.index)
	}

	return
}

// Sphinx doesn't support LastInsertId now.
func (sc *SphinxClient) Execute(sqlStr string) (result sql.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			LogError("Recovered from Execute(): ", r)
		}
	}()

	// Init sql.DB
	if sc.DB == nil {
		if err = sc.GetDb(); err != nil {
			return nil, fmt.Errorf("Execute> %v", err)
		}
	}
	defer sc.DB.Close()
	return sc.DB.Exec(sqlStr)
}

func (sc *SphinxClient) ExecuteReturnRowsAffected(sqlStr string) (rowsAffected int, err error) {
	result, err := sc.Execute(sqlStr)
	if err != nil {
		return -1, err
	}
	if result == nil {
		return -1, fmt.Errorf("ExecuteReturnRowsAffected: Nil result")
	}
	
	rowsAffected64, err := result.RowsAffected()
	if err != nil {
		return -1, fmt.Errorf("ExecuteReturnRowsAffected: %v", err)
	}

	rowsAffected = int(rowsAffected64)
	if rowsAffected < 0 {
		return rowsAffected, fmt.Errorf("ExecuteReturnRowsAffected> Nagtive RowsAffected(): %d", rowsAffected)
	}
	return
}

func (sc *SphinxClient) insert(obj interface{}, doReplace bool) (err error) {
	if err = sc.Init(obj); err != nil {
		return fmt.Errorf("Insert > %v", err)
	}

	var colVals []string
	// 如果没有设置sc.columns，则默认选择obj的所有field作为columns
	if len(sc.columns) == 0 {
		if sc.val.Kind() == reflect.Struct {
			var appendField func(*[]string, *[]string, reflect.Value) error
			appendField = func(strs *[]string, vals *[]string, val reflect.Value) (err error) {
				for i := 0; i < val.NumField(); i++ {
					fieldVal := val.Field(i)
					sf := val.Type().Field(i)

					switch fieldVal.Type().Kind() {
					case reflect.Struct:
						if err = appendField(strs, vals, fieldVal); err != nil {
							return err
						}
					case reflect.Slice, reflect.Map:
						// just pass
					default:
						*strs = append(*strs, sf.Name)
						s, err := GetValQuoteStr(fieldVal)
						if err != nil {
							return err
						}
						*vals = append(*vals, s)
					}
				}

				return nil
			}

			if err = appendField(&sc.columns, &colVals, sc.val); err != nil {
				return
			}
		} else {
			// 如果不是struct，则认为只有一个‘id’字段
			sc.columns = []string{DefaultPK}
			s, err := GetValQuoteStr(sc.val)
			if err != nil {
				return fmt.Errorf("Insert > %v", err)
			}
			colVals = []string{s}
		}

	} else if colVals, err = GetColVals(sc.val, sc.columns); err != nil {
		return
	}

	sqlStr := fmt.Sprintf(" INTO %s (%s) VALUES (%s)", sc.index, strings.Join(sc.columns, ","), strings.Join(colVals, ","))
	if doReplace {
		sqlStr = "REPLACE" + sqlStr
	} else {
		sqlStr = "INSERT" + sqlStr
	}
	
	//fmt.Printf("Insert sql: %s\n colVals: %#v\n", sqlStr, colVals)
	if _, err = sc.Execute(sqlStr);	err != nil {
		return fmt.Errorf("Insert > %v", err)
	}
	
	return
}

func (sc *SphinxClient) Insert(obj interface{}) error {
	// false means NOT do REPLACE 
	return sc.insert(obj, false)
}

func (sc *SphinxClient) Replace(obj interface{}) error {
	// true means DO REPLACE
	return sc.insert(obj, true)
}

func (sc *SphinxClient) Delete(id int) (rowsAffected int, err error) {
	if err = sc.Init(nil); err != nil {
		return -1, fmt.Errorf("Delete> %v", err)
	}

	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE id=%d", sc.index, id)
	rowsAffected, err = sc.ExecuteReturnRowsAffected(sqlStr)
	if err != nil {
		return 0, fmt.Errorf("Delete> id:%d  %v", id, err)
	}
	return
}

// BEGIN, COMMIT, and ROLLBACK

// ShowTables

// Added in version 2.1.1-beta, clears the RT index completely.
func (sc *SphinxClient) TruncateRT(index string) error {
	if index == "" {
		if sc.index != "" {
			index = sc.index
		} else {
			return errors.New("Truncate > Empty index name!")
		}
	}
	if _, err := sc.Execute("TRUNCATE RTINDEX " + index); err != nil {
		return fmt.Errorf("Truncate(%s) > %v", index, err)
	}
	return nil
}

/// Util funcs

// 'AbcDefGhi' to 'abc_def_ghi'
func CamelToSep(ori string, sep byte) string {
	var bs []byte

	// If the first char is uppercase
	first := ori[0]
	if first >= 65 && first <= 90 {
		first += 32
	}
	bs = []byte{first}

	for i := 1; i < len(ori); i++ {
		if ori[i] >= 65 && ori[i] <= 90 {
			bs = append(bs, sep, ori[i]+32)
		} else {
			bs = append(bs, ori[i])
		}
	}
	return string(bs)
}

// Set data to database, such as 'Insert','Update'，仅用于设置了sc.columns的时候
func GetColVals(val reflect.Value, cols []string) (values []string, err error) {
	typ := val.Type()
	// if not struct, then must just have one column.
	if val.Kind() != reflect.Struct && len(cols) != 1 {
		return nil, fmt.Errorf("GetColVals> If not a struct(%s), must have one column: %v", val.Kind(), cols)
	}

	values = make([]string, len(cols))
	for i, col := range cols {
		var fieldVal reflect.Value
		if val.Kind() == reflect.Struct {
			fieldIndex := getFieldIndexByName(typ, col)
			if fieldIndex[0] < 0 {
				return nil, fmt.Errorf("GetColVals> Can't found struct field(column): '%s'\n", col)
			}
			fieldVal = val.FieldByIndex(fieldIndex)
		} else {
			fieldVal = val
		}

		if values[i], err = GetValQuoteStr(fieldVal); err != nil {
			return
		}
	}
	return
}

// for insert and update
// If already assigned, then just ignore tag
func GetValQuoteStr(val reflect.Value) (string, error) {
	switch val.Kind() {
	case reflect.Bool:
		boolStr := "N"
		if val.Bool() {
			boolStr = "Y"
		}
		return boolStr, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'f', -1, 64), nil
	case reflect.String:
		return QuoteStr(val.String()), nil
	case reflect.Slice: //[]byte
		if val.Type().Elem().Name() != "uint8" {
			return "", fmt.Errorf("GetValQuoteStr> slicetype is not []byte: %v", val.Interface())
		}
		return QuoteStr(string(val.Interface().([]byte))), nil
	default:
		return "", fmt.Errorf("GetValQuoteStr> reflect.Value is not a string/int/uint/float/bool/[]byte!\nval: %v", val)
	}
	return "", nil
}

//有时需要获取在fields中的index，以便对StructFiled和value分别进行操作；FieldByNameFunc只能获取到field的value或者Type。
func getFieldIndexByName(typ reflect.Type, name string) (index []int) {
	for i := 0; i < typ.NumField(); i++ {
		// 检测field是否是struct
		field := typ.Field(i)
		var subIndex []int
		if field.Type.Kind() == reflect.Struct {
			// 如果获取到的subIndex是有效的，则把当前的index与subIndex合并返回
			if subIndex = getFieldIndexByName(field.Type, name); subIndex[0] >= 0 {
				return append([]int{i}, subIndex...)
			}
		}

		if field.Name == name {
			return []int{i}
		}
	}
	return []int{-1}
}

// No escape handle!?
func QuoteStr(s string) string {
	return "'" + s + "'"
}
/*
  $search=array("\\\\","\\0","\\n","\\r","\Z","\'",'\"');
  "\","\0","\n","\r","\x1a","'",'"'
*/  