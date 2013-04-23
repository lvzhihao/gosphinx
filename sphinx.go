package gosphinx

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"strings"
	"time"
)

// All these variables will be used in NewClient() as default values.
// You can change them, so that you do not need to call Set***() every time.
var (
	Host       = "localhost"
	Port       = 9312
	SqlPort    = 9306
	Socket     = ""
	SqlSocket  = ""
	Limit      = 20
	Mode       = SPH_MATCH_EXTENDED // "When you use one of the legacy modes, Sphinx internally converts the query to the appropriate new syntax and chooses the appropriate ranker."
	Sort       = SPH_SORT_RELEVANCE
	GroupFunc  = SPH_GROUPBY_DAY
	GroupSort  = "@group desc"
	MaxMatches = 1000
	Timeout    = 1000
	Ranker     = SPH_RANK_PROXIMITY_BM25
	SelectStr  = "*"
)

/* searchd command versions */
const (
	VER_MAJOR_PROTO        = 0x1
	VER_COMMAND_SEARCH     = 0x119 // 0x11D for 2.1
	VER_COMMAND_EXCERPT    = 0x104
	VER_COMMAND_UPDATE     = 0x102 // 0x103 for 2.1
	VER_COMMAND_KEYWORDS   = 0x100
	VER_COMMAND_STATUS     = 0x100
	VER_COMMAND_FLUSHATTRS = 0x100
)

/* matching modes */
const (
	SPH_MATCH_ALL = iota
	SPH_MATCH_ANY
	SPH_MATCH_PHRASE
	SPH_MATCH_BOOLEAN
	SPH_MATCH_EXTENDED
	SPH_MATCH_FULLSCAN
	SPH_MATCH_EXTENDED2
)

/* ranking modes (extended2 only) */
const (
	SPH_RANK_PROXIMITY_BM25 = iota // Default mode, phrase proximity major factor and BM25 minor one
	SPH_RANK_BM25
	SPH_RANK_NONE
	SPH_RANK_WORDCOUNT
	SPH_RANK_PROXIMITY
	SPH_RANK_MATCHANY
	SPH_RANK_FIELDMASK
	SPH_RANK_SPH04
	SPH_RANK_EXPR
	SPH_RANK_TOTAL
)

/* sorting modes */
const (
	SPH_SORT_RELEVANCE = iota
	SPH_SORT_ATTR_DESC
	SPH_SORT_ATTR_ASC
	SPH_SORT_TIME_SEGMENTS
	SPH_SORT_EXTENDED
	SPH_SORT_EXPR // Deprecated, never use it.
)

/* grouping functions */
const (
	SPH_GROUPBY_DAY = iota
	SPH_GROUPBY_WEEK
	SPH_GROUPBY_MONTH
	SPH_GROUPBY_YEAR
	SPH_GROUPBY_ATTR
	SPH_GROUPBY_ATTRPAIR
)

/* searchd reply status codes */
const (
	SEARCHD_OK = iota
	SEARCHD_ERROR
	SEARCHD_RETRY
	SEARCHD_WARNING
)

/* attribute types */
const (
	SPH_ATTR_NONE = iota
	SPH_ATTR_INTEGER
	SPH_ATTR_TIMESTAMP
	SPH_ATTR_ORDINAL
	SPH_ATTR_BOOL
	SPH_ATTR_FLOAT
	SPH_ATTR_BIGINT
	SPH_ATTR_STRING
	SPH_ATTR_MULTI   = 0x40000001
	SPH_ATTR_MULTI64 = 0x40000002
)

/* searchd commands */
const (
	SEARCHD_COMMAND_SEARCH = iota
	SEARCHD_COMMAND_EXCERPT
	SEARCHD_COMMAND_UPDATE
	SEARCHD_COMMAND_KEYWORDS
	SEARCHD_COMMAND_PERSIST
	SEARCHD_COMMAND_STATUS
	SEARCHD_COMMAND_QUERY
	SEARCHD_COMMAND_FLUSHATTRS
)

/* filter types */
const (
	SPH_FILTER_VALUES = iota
	SPH_FILTER_RANGE
	SPH_FILTER_FLOATRANGE
)

type filter struct {
	attr       string
	filterType int
	values     []uint64
	umin       uint64
	umax       uint64
	fmin       float32
	fmax       float32
	exclude    bool
}

type override struct {
	attrName string
	attrType int
	values   map[uint64]interface{}
}

type Match struct {
	DocId      uint64        // Matched document ID.
	Weight     int           // Matched document weight.
	AttrValues []interface{} // Matched document attribute values.
}

type WordInfo struct {
	Word string // Word form as returned from search daemon, stemmed or otherwise postprocessed.
	Docs int    // Total amount of matching documents in collection.
	Hits int    // Total amount of hits (occurences) in collection.
}

type Result struct {
	Fields     []string         // Full-text field namess.
	AttrNames  []string         // Attribute names.
	AttrTypes  []int            // Attribute types (refer to SPH_ATTR_xxx constants in Client).
	Matches    []Match    // Retrieved matches.
	Total      int              // Total matches in this result set.
	TotalFound int              // Total matches found in the index(es).
	Time       float32          // Elapsed time (as reported by searchd), in seconds.
	Words      []WordInfo // Per-word statistics.

	Warning string
	Error   error
	Status  int // Query status (refer to SEARCHD_xxx constants in Client).
}

type Options struct {
	Host string
	Port int
	Socket string
	SqlPort int
	SqlSocket string
	RetryCount int
	RetryDelay int
	Timeout time.Duration
	Offset int
	Limit int
	MaxMatches int
	Cutoff int
	MaxQueryTime int
	SelectStr string
	MatchMode int
	RankingMode int
	Rankexpr string
	SortMode int
	SortBy string
	MinId uint64
	MaxId uint64
	LatitudeAttr string
	LongitudeAttr string
	Latitude float32
	Longitude float32
	GroupBy string
	GroupFunc int
	GroupSort string
	GroupDistinct string
}

type Client struct {
	Options *Options
	host   string
	port   int
	socket string // Unix socket
	conn   net.Conn

	offset        int    // how many records to seek from result-set start
	limit         int    // how many records to return from result-set starting at offset (default is 20)
	mode          int    // query matching mode (default is SPH_MATCH_ALL)
	weights       []int  // per-field weights (default is 1 for all fields)
	sort          int    // match sorting mode (default is SPH_SORT_RELEVANCE)
	sortBy        string // attribute to sort by (defualt is "")
	minId         uint64 // min ID to match (default is 0, which means no limit)
	maxId         uint64 // max ID to match (default is 0, which means no limit)
	filters       []filter
	groupBy       string // group-by attribute name
	groupFunc     int    // group-by function (to pre-process group-by attribute value with)
	groupSort     string // group-by sorting clause (to sort groups in result set with)
	groupDistinct string // group-by count-distinct attribute
	maxMatches    int    // max matches to retrieve
	cutoff        int    // cutoff to stop searching at
	retryCount    int
	retryDelay    int
	latitudeAttr  string
	longitudeAttr string
	latitude      float32
	longitude     float32

	warning   string
	err       error
	connerror bool // connection error vs remote error flag
	timeout   time.Duration

	reqs [][]byte // requests array for multi-query

	indexWeights map[string]int
	ranker       int    // ranking mode
	rankexpr     string // ranking expression for SPH_RANK_EXPR
	maxQueryTime int
	fieldWeights map[string]int
	overrides    map[string]override
	selectStr    string // select-list (attributes or expressions, with optional aliases)

	// For sphinxql
	DB      *sql.DB       // Capitalize, so that can "defer sc.Db.Close()"
	val     reflect.Value // object parameter's reflect value
	index   string        // index name for sphinxql query.
	columns []string
	where   string
}

// Do not set socket/host/port
func defaultClient() (sc *Client) {
	sc = new(Client)
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

func NewClient(opts ...*Options) (sc *Client) {
	// if have *Options param, then use it
	if len(opts) > 0 {
		o := opts[0]
		sc = new(Client)
		
		sc.host = o.Host
		sc.port = o.Port
		sc.socket = o.Socket
		// if set opts.SqlPort/SqlSocket, then just ignored opts.Port/Socket
		if o.SqlPort > 0 {
			sc.port = o.SqlPort
		}
		if o.SqlSocket != "" {
			sc.socket = o.SqlSocket
		}
		sc.retryCount = o.RetryCount
		sc.retryDelay = o.RetryDelay
		sc.timeout = o.Timeout
		sc.offset = o.Offset
		sc.limit = o.Limit
		sc.maxMatches = o.MaxMatches
		sc.cutoff = o.Cutoff
		sc.maxQueryTime = o.MaxQueryTime
		sc.selectStr = o.SelectStr
		sc.mode = o.MatchMode
		sc.ranker = o.RankingMode
		sc.rankexpr = o.Rankexpr
		sc.sort = o.SortMode
		sc.sortBy = o.SortBy
		sc.minId = o.MinId
		sc.maxId = o.MaxId
		sc.latitudeAttr = o.LatitudeAttr
		sc.longitudeAttr = o.LongitudeAttr
		sc.latitude = o.Latitude
		sc.longitude = o.Longitude
		sc.groupBy = o.GroupBy
		sc.groupFunc = o.GroupFunc
		sc.groupSort = o.GroupSort
		sc.groupDistinct = o.GroupDistinct

		return
	}
	
	sc = defaultClient()
	
	if Socket != "" {
		sc.socket = Socket
	} else {
		sc.host = Host
		sc.port = Port
	}

	return
}

/***** General API functions *****/

func (sc *Client) GetLastError() error {
	return sc.err
}

// Just for convenience
func (sc *Client) Error() error {
	return sc.err
}

func (sc *Client) GetLastWarning() string {
	return sc.warning
}

// Note: this func also can set sc.socket(unix socket).
// For convenience, you can set gosphinx.Host, gosphinx.Port, gosphinx.Socket as default value,
// then you don't need to call SetServer() every time.
func (sc *Client) SetServer(host string, port int) error {
	var isSocketMode bool

	if host != "" {
		sc.host = host

		if host[0] == '/' {
			sc.socket = host
			isSocketMode = true
		}
		if host[:7] == "unix://" {
			sc.socket = host[7:]
			isSocketMode = true
		}
	}

	if !isSocketMode && port <= 0 {
		sc.err = fmt.Errorf("SetServer > port must be positive: %d", port)
		return sc.err
	}

	sc.port = port
	return nil
}
func (sc *Client) Server(host string, port int) *Client {
	sc.err = sc.SetServer(host, port)
	return sc
}

func (sc *Client) SetRetries(count, delay int) error {
	if count < 0 {
		sc.err = fmt.Errorf("SetRetries > count must not be negative: %d", count)
		return sc.err
	}
	if delay < 0 {
		sc.err = fmt.Errorf("SetRetries > delay must not be negative: %d", delay)
		return sc.err
	}

	sc.retryCount = count
	sc.retryDelay = delay
	return nil
}
func (sc *Client) Retries(count, delay int) *Client {
	sc.err = sc.SetRetries(count, delay)
	return sc
}

// millisecond, not nanosecond.
func (sc *Client) SetConnectTimeout(timeout int) error {
	if timeout < 0 {
		sc.err = fmt.Errorf("SetConnectTimeout > connect timeout must not be negative: %d", timeout)
		return sc.err
	}

	sc.timeout = time.Duration(timeout) * time.Millisecond
	return nil
}
func (sc *Client) ConnectTimeout(timeout int) *Client {
	sc.err = sc.SetConnectTimeout(timeout)
	return sc
}

func (sc *Client) IsConnectError() bool {
	return sc.connerror
}

/***** General query settings *****/

// Set matches offset and limit to return to client, max matches to retrieve on server, and cutoff.
func (sc *Client) SetLimits(offset, limit, maxMatches, cutoff int) error {
	if offset < 0 {
		sc.err = fmt.Errorf("SetLimits > offset must not be negative: %d", offset)
		return sc.err
	}
	if limit <= 0 {
		sc.err = fmt.Errorf("SetLimits > limit must be positive: %d", limit)
		return sc.err
	}
	if maxMatches <= 0 {
		sc.err = fmt.Errorf("SetLimits > maxMatches must be positive: %d", maxMatches)
		return sc.err
	}
	if cutoff < 0 {
		sc.err = fmt.Errorf("SetLimits > cutoff must not be negative: %d", cutoff)
		return sc.err
	}

	sc.offset = offset
	sc.limit = limit
	if maxMatches > 0 {
		sc.maxMatches = maxMatches
	}
	if cutoff > 0 {
		sc.cutoff = cutoff
	}
	return nil
}
func (sc *Client) Limits(offset, limit, maxMatches, cutoff int) *Client {
	sc.err = sc.SetLimits(offset, limit, maxMatches, cutoff)
	return sc
}

// Set maximum query time, in milliseconds, per-index, 0 means "do not limit".
func (sc *Client) SetMaxQueryTime(maxQueryTime int) error {
	if maxQueryTime < 0 {
		sc.err = fmt.Errorf("SetMaxQueryTime > maxQueryTime must not be negative: %d", maxQueryTime)
		return sc.err
	}

	sc.maxQueryTime = maxQueryTime
	return nil
}
func (sc *Client) MaxQueryTime(maxQueryTime int) *Client {
	sc.err = sc.SetMaxQueryTime(maxQueryTime)
	return sc
}

func (sc *Client) SetOverride(attrName string, attrType int, values map[uint64]interface{}) error {
	if attrName == "" {
		sc.err = errors.New("SetOverride > attrName is empty!")
		return sc.err
	}
	// Min value is 'SPH_ATTR_INTEGER = 1', not '0'.
	if (attrType < 1 || attrType > SPH_ATTR_STRING) && attrType != SPH_ATTR_MULTI && SPH_ATTR_MULTI != SPH_ATTR_MULTI64 {
		sc.err = fmt.Errorf("SetOverride > invalid attrType: %d", attrType)
		return sc.err
	}

	sc.overrides[attrName] = override{
		attrName: attrName,
		attrType: attrType,
		values:   values,
	}
	return nil
}
func (sc *Client) Override(attrName string, attrType int, values map[uint64]interface{}) *Client {
	sc.err = sc.SetOverride(attrName, attrType, values)
	return sc
}

func (sc *Client) SetSelect(s string) error {
	if s == "" {
		sc.err = errors.New("SetSelect > selectStr is empty!")
		return sc.err
	}

	sc.selectStr = s
	return nil
}
func (sc *Client) Select(s string) *Client {
	sc.err = sc.SetSelect(s)
	return sc
}

/***** Full-text search query settings *****/

func (sc *Client) SetMatchMode(mode int) error {
	if mode < 0 || mode > SPH_MATCH_EXTENDED2 {
		sc.err = fmt.Errorf("SetMatchMode > unknown mode value; use one of the SPH_MATCH_xxx constants: %d", mode)
		return sc.err
	}

	sc.mode = mode
	return nil
}
func (sc *Client) MatchMode(mode int) *Client {
	sc.err = sc.SetMatchMode(mode)
	return sc
}

func (sc *Client) SetRankingMode(ranker int, rankexpr ...string) error {
	if ranker < 0 || ranker > SPH_RANK_TOTAL {
		sc.err = fmt.Errorf("SetRankingMode > unknown ranker value; use one of the SPH_RANK_xxx constants: %d", ranker)
		return sc.err
	}
	
	sc.ranker = ranker
	
	if len(rankexpr) > 0 {
		if ranker != SPH_RANK_EXPR {
			sc.err = fmt.Errorf("SetRankingMode > rankexpr must used with SPH_RANK_EXPR! ranker: %d  rankexpr: %s", ranker, rankexpr)
			return sc.err
		}
		
		sc.rankexpr = rankexpr[0]
	}
	
	return nil
}
func (sc *Client) RankingMode(ranker int, rankexpr ...string) *Client {
	sc.err = sc.SetRankingMode(ranker, rankexpr...)
	return sc
}

func (sc *Client) SetSortMode(mode int, sortBy string) error {
	if mode < 0 || mode > SPH_SORT_EXPR {
		sc.err = fmt.Errorf("SetSortMode > unknown mode value; use one of the available SPH_SORT_xxx constants: %d", mode)
		return sc.err
	}
	/*SPH_SORT_RELEVANCE ignores any additional parameters and always sorts matches by relevance rank.
	All other modes require an additional sorting clause.*/
	if (mode != SPH_SORT_RELEVANCE) && (sortBy == "") {
		sc.err = fmt.Errorf("SetSortMode > sortby string must not be empty in selected mode: %d", mode)
		return sc.err
	}

	sc.sort = mode
	sc.sortBy = sortBy
	return nil
}
func (sc *Client) SortMode(mode int, sortBy string) *Client {
	sc.err = sc.SetSortMode(mode, sortBy)
	return sc
}

func (sc *Client) SetFieldWeights(weights map[string]int) error {
	// Default weight value is 1.
	for field, weight := range weights {
		if weight < 1 {
			sc.err = fmt.Errorf("SetFieldWeights > weights must be positive 32-bit integers, field:%s  weight:%d", field, weight)
			return sc.err
		}
	}

	sc.fieldWeights = weights
	return nil
}
func (sc *Client) FieldWeights(weights map[string]int) *Client {
	sc.err = sc.SetFieldWeights(weights)
	return sc
}

func (sc *Client) SetIndexWeights(weights map[string]int) error {
	for field, weight := range weights {
		if weight < 1 {
			sc.err = fmt.Errorf("SetIndexWeights > weights must be positive 32-bit integers, field:%s  weight:%d", field, weight)
			return sc.err
		}
	}

	sc.indexWeights = weights
	return nil
}
func (sc *Client) IndexWeights(weights map[string]int) *Client {
	sc.err = sc.SetIndexWeights(weights)
	return sc
}

/***** Result set filtering settings *****/

func (sc *Client) SetIDRange(min, max uint64) error {
	if min > max {
		sc.err = fmt.Errorf("SetIDRange > min > max! min:%d  max:%d", min, max)
		return sc.err
	}

	sc.minId = min
	sc.maxId = max
	return nil
}
func (sc *Client) IDRange(min, max uint64) *Client {
	sc.err = sc.SetIDRange(min, max)
	return sc
}

func (sc *Client) SetFilter(attr string, values []uint64, exclude bool) error {
	if attr == "" {
		sc.err = fmt.Errorf("SetFilter > attribute name is empty!")
		return sc.err
	}
	if len(values) == 0 {
		sc.err = fmt.Errorf("SetFilter > values is empty!")
		return sc.err
	}

	sc.filters = append(sc.filters, filter{
		filterType: SPH_FILTER_VALUES,
		attr:       attr,
		values:     values,
		exclude:    exclude,
	})
	return nil
}
func (sc *Client) Filter(attr string, values []uint64, exclude bool) *Client {
	sc.err = sc.SetFilter(attr, values, exclude)
	return sc
}

func (sc *Client) SetFilterRange(attr string, umin, umax uint64, exclude bool) error {
	if attr == "" {
		sc.err = fmt.Errorf("SetFilterRange > attribute name is empty!")
		return sc.err
	}
	if umin > umax {
		sc.err = fmt.Errorf("SetFilterRange > min > max! umin:%d  umax:%d", umin, umax)
		return sc.err
	}

	sc.filters = append(sc.filters, filter{
		filterType: SPH_FILTER_RANGE,
		attr:       attr,
		umin:       umin,
		umax:       umax,
		exclude:    exclude,
	})
	return nil
}
func (sc *Client) FilterRange(attr string, umin, umax uint64, exclude bool) *Client {
	sc.err = sc.SetFilterRange(attr, umin, umax, exclude)
	return sc
}

func (sc *Client) SetFilterFloatRange(attr string, fmin, fmax float32, exclude bool) error {
	if attr == "" {
		sc.err = fmt.Errorf("SetFilterFloatRange > attribute name is empty!")
		return sc.err
	}
	if fmin > fmax {
		sc.err = fmt.Errorf("SetFilterFloatRange > min > max! fmin:%d  fmax:%d", fmin, fmax)
		return sc.err
	}

	sc.filters = append(sc.filters, filter{
		filterType: SPH_FILTER_FLOATRANGE,
		attr:       attr,
		fmin:       fmin,
		fmax:       fmax,
		exclude:    exclude,
	})
	return nil
}
func (sc *Client) FilterFloatRange(attr string, fmin, fmax float32, exclude bool) *Client {
	sc.err = sc.SetFilterFloatRange(attr, fmin, fmax, exclude)
	return sc
}

// The latitude and longitude are expected to be in radians. Use DegreeToRadian() to transform degree values.
func (sc *Client) SetGeoAnchor(latitudeAttr, longitudeAttr string, latitude, longitude float32) error {
	if latitudeAttr == "" {
		sc.err = fmt.Errorf("SetGeoAnchor > latitudeAttr is empty!")
		return sc.err
	}
	if longitudeAttr == "" {
		sc.err = fmt.Errorf("SetGeoAnchor > longitudeAttr is empty!")
		return sc.err
	}

	sc.latitudeAttr = latitudeAttr
	sc.longitudeAttr = longitudeAttr
	sc.latitude = latitude
	sc.longitude = longitude
	return nil
}
func (sc *Client) GeoAnchor(latitudeAttr, longitudeAttr string, latitude, longitude float32) *Client {
	sc.err = sc.SetGeoAnchor(latitudeAttr, longitudeAttr, latitude, longitude)
	return sc
}

/***** GROUP BY settings *****/

func (sc *Client) SetGroupBy(groupBy string, groupFunc int, groupSort string) error {
	if groupFunc < 0 || groupFunc > SPH_GROUPBY_ATTRPAIR {
		sc.err = fmt.Errorf("SetGroupBy > unknown groupFunc value: '%d', use one of the available SPH_GROUPBY_xxx constants.", groupFunc)
		return sc.err
	}

	sc.groupBy = groupBy
	sc.groupFunc = groupFunc
	sc.groupSort = groupSort
	return nil
}
func (sc *Client) GroupBy(groupBy string, groupFunc int, groupSort string) *Client {
	sc.err = sc.SetGroupBy(groupBy, groupFunc, groupSort)
	return sc
}

func (sc *Client) SetGroupDistinct(groupDistinct string) error {
	if groupDistinct == "" {
		sc.err = errors.New("SetGroupDistinct > groupDistinct is empty!")
		return sc.err
	}
	sc.groupDistinct = groupDistinct
	return nil
}
func (sc *Client) GroupDistinct(groupDistinct string) *Client {
	sc.err = sc.SetGroupDistinct(groupDistinct)
	return sc
}

/***** Querying *****/

func (sc *Client) Query(query, index, comment string) (result *Result, err error) {
	if index == "" {
		index = "*"
	}

	// reset requests array
	sc.reqs = nil
	if _, err = sc.AddQuery(query, index, comment); err != nil {
		return nil, err
	}

	results, err := sc.RunQueries()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("Query > Empty results!\nClient: %#v", sc)
	}

	result = &results[0]
	if result.Error != nil {
		return nil, fmt.Errorf("Query > Result error: %v", result.Error)
	}

	sc.warning = result.Warning
	return
}

func (sc *Client) AddQuery(query, index, comment string) (i int, err error) {
	var req []byte

	req = writeInt32ToBytes(req, sc.offset)
	req = writeInt32ToBytes(req, sc.limit)
	req = writeInt32ToBytes(req, sc.mode)
	req = writeInt32ToBytes(req, sc.ranker)
	if sc.ranker == SPH_RANK_EXPR {
		req = writeLenStrToBytes(req, sc.rankexpr)
	}
	req = writeInt32ToBytes(req, sc.sort)
	req = writeLenStrToBytes(req, sc.sortBy)
	req = writeLenStrToBytes(req, query)

	req = writeInt32ToBytes(req, len(sc.weights))
	for _, w := range sc.weights {
		req = writeInt32ToBytes(req, w)
	}

	req = writeLenStrToBytes(req, index)

	req = writeInt32ToBytes(req, 1) // id64 range marker
	req = writeInt64ToBytes(req, sc.minId)
	req = writeInt64ToBytes(req, sc.maxId)

	req = writeInt32ToBytes(req, len(sc.filters))
	for _, f := range sc.filters {
		req = writeLenStrToBytes(req, f.attr)
		req = writeInt32ToBytes(req, f.filterType)

		switch f.filterType {
		case SPH_FILTER_VALUES:
			req = writeInt32ToBytes(req, len(f.values))
			for _, v := range f.values {
				req = writeInt64ToBytes(req, v)
			}
		case SPH_FILTER_RANGE:
			req = writeInt64ToBytes(req, f.umin)
			req = writeInt64ToBytes(req, f.umax)
		case SPH_FILTER_FLOATRANGE:
			req = writeFloat32ToBytes(req, f.fmin)
			req = writeFloat32ToBytes(req, f.fmax)
		}

		if f.exclude {
			req = writeInt32ToBytes(req, 1)
		} else {
			req = writeInt32ToBytes(req, 0)
		}
	}

	req = writeInt32ToBytes(req, sc.groupFunc)
	req = writeLenStrToBytes(req, sc.groupBy)

	req = writeInt32ToBytes(req, sc.maxMatches)
	req = writeLenStrToBytes(req, sc.groupSort)

	req = writeInt32ToBytes(req, sc.cutoff)
	req = writeInt32ToBytes(req, sc.retryCount)
	req = writeInt32ToBytes(req, sc.retryDelay)

	req = writeLenStrToBytes(req, sc.groupDistinct)

	if sc.latitudeAttr == "" || sc.longitudeAttr == "" {
		req = writeInt32ToBytes(req, 0)
	} else {
		req = writeInt32ToBytes(req, 1)
		req = writeLenStrToBytes(req, sc.latitudeAttr)
		req = writeLenStrToBytes(req, sc.longitudeAttr)
		req = writeFloat32ToBytes(req, sc.latitude)
		req = writeFloat32ToBytes(req, sc.longitude)
	}

	req = writeInt32ToBytes(req, len(sc.indexWeights))
	for ind, wei := range sc.indexWeights {
		req = writeLenStrToBytes(req, ind)
		req = writeInt32ToBytes(req, wei)
	}

	req = writeInt32ToBytes(req, sc.maxQueryTime)

	req = writeInt32ToBytes(req, len(sc.fieldWeights))
	for fie, wei := range sc.fieldWeights {
		req = writeLenStrToBytes(req, fie)
		req = writeInt32ToBytes(req, wei)
	}

	req = writeLenStrToBytes(req, comment)

	// attribute overrides
	req = writeInt32ToBytes(req, len(sc.overrides))
	for _, override := range sc.overrides {
		req = writeLenStrToBytes(req, override.attrName)
		req = writeInt32ToBytes(req, override.attrType)
		req = writeInt32ToBytes(req, len(override.values))
		for id, v := range override.values {
			req = writeInt64ToBytes(req, id)
			switch override.attrType {
			case SPH_ATTR_INTEGER:
				req = writeInt32ToBytes(req, v.(int))
			case SPH_ATTR_FLOAT:
				req = writeFloat32ToBytes(req, v.(float32))
			case SPH_ATTR_BIGINT:
				req = writeInt64ToBytes(req, v.(uint64))
			default:
				return -1, fmt.Errorf("AddQuery > attr value is not int/float32/uint64.")
			}
		}
	}

	// select-list
	req = writeLenStrToBytes(req, sc.selectStr)

	// send query, get response
	sc.reqs = append(sc.reqs, req)
	return len(sc.reqs) - 1, nil
}

//Returns None on network IO failure; or an array of result set hashes on success.
func (sc *Client) RunQueries() (results []Result, err error) {
	if len(sc.reqs) == 0 {
		return nil, fmt.Errorf("RunQueries > No queries defined, issue AddQuery() first.")
	}

	nreqs := len(sc.reqs)
	var allReqs []byte

	allReqs = writeInt32ToBytes(allReqs, 0) // it's a client
	allReqs = writeInt32ToBytes(allReqs, nreqs)
	for _, req := range sc.reqs {
		allReqs = append(allReqs, req...)
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_SEARCH, VER_COMMAND_SEARCH, allReqs)
	if err != nil {
		return nil, err
	}

	p := 0
	for i := 0; i < nreqs; i++ {
		var result = Result{Status: -1} // Default value of stauts is 0, but SEARCHD_OK = 0, so must set it to another num.

		result.Status = int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		if result.Status != SEARCHD_OK {
			length := int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			message := response[p : p+length]
			p += length

			if result.Status == SEARCHD_WARNING {
				result.Warning = string(message)
			} else {
				result.Error = errors.New(string(message))
				continue
			}
		}

		// read schema
		nfields := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		result.Fields = make([]string, nfields)
		for fieldNum := 0; fieldNum < nfields; fieldNum++ {
			fieldLen := int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			result.Fields[fieldNum] = string(response[p : p+fieldLen])
			p += fieldLen
		}

		nattrs := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		result.AttrNames = make([]string, nattrs)
		result.AttrTypes = make([]int, nattrs)
		for attrNum := 0; attrNum < nattrs; attrNum++ {
			attrLen := int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			result.AttrNames[attrNum] = string(response[p : p+attrLen])
			p += attrLen
			result.AttrTypes[attrNum] = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
		}

		// read match count
		count := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		id64 := binary.BigEndian.Uint32(response[p : p+4]) // if id64 == 1, then docId is uint64
		p += 4
		result.Matches = make([]Match, count)
		for matchesNum := 0; matchesNum < count; matchesNum++ {
			var match Match
			if id64 == 1 {
				match.DocId = binary.BigEndian.Uint64(response[p : p+8])
				p += 8
			} else {
				match.DocId = uint64(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
			}
			match.Weight = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4

			match.AttrValues = make([]interface{}, nattrs)

			for attrNum := 0; attrNum < len(result.AttrTypes); attrNum++ {
				attrType := result.AttrTypes[attrNum]
				switch attrType {
				case SPH_ATTR_BIGINT:
					match.AttrValues[attrNum] = binary.BigEndian.Uint64(response[p : p+8])
					p += 8
				case SPH_ATTR_FLOAT:
					var f float32
					buf := bytes.NewBuffer(response[p : p+4])
					if err := binary.Read(buf, binary.BigEndian, &f); err != nil {
						return nil, fmt.Errorf("binary.Read error: %v", err)
					}
					match.AttrValues[attrNum] = f
					p += 4
				case SPH_ATTR_STRING:
					slen := int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					match.AttrValues[attrNum] = ""
					if slen > 0 {
						match.AttrValues[attrNum] = response[p : p+slen]
					}
					p += slen //p += slen-4
				case SPH_ATTR_MULTI: // SPH_ATTR_MULTI is 2^30+1, not an int value.
					nvals := int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					var vals = make([]uint32, nvals)
					for valNum := 0; valNum < nvals; valNum++ {
						vals[valNum] = binary.BigEndian.Uint32(response[p : p+4])
						p += 4
					}
					match.AttrValues[attrNum] = vals
				case SPH_ATTR_MULTI64:
					nvals := int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					nvals = nvals / 2
					var vals = make([]uint64, nvals)
					for valNum := 0; valNum < nvals; valNum++ {
						vals[valNum] = binary.BigEndian.Uint64(response[p : p+4])
						p += 8
					}
					match.AttrValues[attrNum] = vals
				default: // handle everything else as unsigned ints
					match.AttrValues[attrNum] = binary.BigEndian.Uint32(response[p : p+4])
					p += 4
				}
			}
			result.Matches[matchesNum] = match
		}

		result.Total = int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		result.TotalFound = int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4

		msecs := binary.BigEndian.Uint32(response[p : p+4])
		p += 4
		result.Time = float32(msecs) / 1000.0

		nwords := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4

		result.Words = make([]WordInfo, nwords)
		for wordNum := 0; wordNum < nwords; wordNum++ {
			wordLen := int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			result.Words[wordNum].Word = string(response[p : p+wordLen])
			p += wordLen
			result.Words[wordNum].Docs = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			result.Words[wordNum].Hits = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
		}

		results = append(results, result)
	}

	return
}

func (sc *Client) ResetFilters() {
	sc.filters = []filter{}

	/* reset GEO anchor */
	sc.latitudeAttr = ""
	sc.longitudeAttr = ""
	sc.latitude = 0.0
	sc.longitude = 0.0
}

func (sc *Client) ResetGroupBy() {
	sc.groupBy = ""
	sc.groupFunc = SPH_GROUPBY_DAY
	sc.groupSort = "@group desc"
	sc.groupDistinct = ""
}

/***** Additional functionality *****/

// all bool values are default false.
type ExcerptsOpts struct {
	BeforeMatch        string // default is "<b>".
	AfterMatch         string // default is "</b>".
	ChunkSeparator     string // A string to insert between snippet chunks (passages). Default is " ... ".
	Limit              int    // Maximum snippet size, in symbols (codepoints). default is 256.
	Around             int    // How much words to pick around each matching keywords block. default is 5.
	ExactPhrase        bool   // Whether to highlight exact query phrase matches only instead of individual keywords.
	SinglePassage      bool   // Whether to extract single best passage only.
	UseBoundaries      bool   // Whether to additionaly break passages by phrase boundary characters, as configured in index settings with phrase_boundary directive.
	WeightOrder        bool   // Whether to sort the extracted passages in order of relevance (decreasing weight), or in order of appearance in the document (increasing position). 
	QueryMode          bool   // Whether to handle $words as a query in extended syntax, or as a bag of words (default behavior). 
	ForceAllWords      bool   // Ignores the snippet length limit until it includes all the keywords.
	LimitPassages      int    // Limits the maximum number of passages that can be included into the snippet. default is 0 (no limit).
	LimitWords         int    // Limits the maximum number of keywords that can be included into the snippet. default is 0 (no limit).
	StartPassageId     int    // Specifies the starting value of %PASSAGE_ID% macro (that gets detected and expanded in BeforeMatch, AfterMatch strings). default is 1.
	LoadFiles          bool   // Whether to handle $docs as data to extract snippets from (default behavior), or to treat it as file names, and load data from specified files on the server side. 
	LoadFilesScattered bool   // It assumes "load_files" option, and works only with distributed snippets generation with remote agents. The source files for snippets could be distributed among different agents, and the main daemon will merge together all non-erroneous results. So, if one agent of the distributed index has 'file1.txt', another has 'file2.txt' and you call for the snippets with both these files, the sphinx will merge results from the agents together, so you will get the snippets from both 'file1.txt' and 'file2.txt'.
	HtmlStripMode      string // HTML stripping mode setting. Defaults to "index", allowed values are "none", "strip", "index", and "retain".
	AllowEmpty         bool   // Allows empty string to be returned as highlighting result when a snippet could not be generated (no keywords match, or no passages fit the limit). By default, the beginning of original text would be returned instead of an empty string.
	PassageBoundary    string // Ensures that passages do not cross a sentence, paragraph, or zone boundary (when used with an index that has the respective indexing settings enabled). String, allowed values are "sentence", "paragraph", and "zone".
	EmitZones          bool   // Emits an HTML tag with an enclosing zone name before each passage.
}

func (sc *Client) BuildExcerpts(docs []string, index, words string, opts ExcerptsOpts) (resDocs []string, err error) {
	if len(docs) == 0 {
		return nil, errors.New("BuildExcerpts > Have no documents to process!")
	}
	if index == "" {
		return nil, errors.New("BuildExcerpts > index name is empty!")
	}
	if words == "" {
		return nil, errors.New("BuildExcerpts > Have no words to highlight!")
	}
	if opts.PassageBoundary != "" && opts.PassageBoundary != "sentence" && opts.PassageBoundary != "paragraph" && opts.PassageBoundary != "zone" {
		return nil, fmt.Errorf("BuildExcerpts > PassageBoundary allowed values are 'sentence', 'paragraph', and 'zone', now is: %s", opts.PassageBoundary)
	}

	// Default values, all bool values are default false.
	if opts.BeforeMatch == "" {
		opts.BeforeMatch = "<b>"
	}
	if opts.AfterMatch == "" {
		opts.AfterMatch = "</b>"
	}
	if opts.ChunkSeparator == "" {
		opts.ChunkSeparator = "..."
	}
	if opts.HtmlStripMode == "" {
		opts.HtmlStripMode = "index"
	}
	if opts.Limit == 0 {
		opts.Limit = 256
	}
	if opts.Around == 0 {
		opts.Around = 5
	}
	if opts.StartPassageId == 0 {
		opts.StartPassageId = 1
	}

	var req []byte
	req = writeInt32ToBytes(req, 0)

	iFlags := 1 // remove_spaces
	if opts.ExactPhrase != false {
		iFlags |= 2
	}
	if opts.SinglePassage != false {
		iFlags |= 4
	}
	if opts.UseBoundaries != false {
		iFlags |= 8
	}
	if opts.WeightOrder != false {
		iFlags |= 16
	}
	if opts.QueryMode != false {
		iFlags |= 32
	}
	if opts.ForceAllWords != false {
		iFlags |= 64
	}
	if opts.LoadFiles != false {
		iFlags |= 128
	}
	if opts.AllowEmpty != false {
		iFlags |= 256
	}
	if opts.EmitZones != false {
		iFlags |= 256
	}
	req = writeInt32ToBytes(req, iFlags)

	req = writeLenStrToBytes(req, index)
	req = writeLenStrToBytes(req, words)

	req = writeLenStrToBytes(req, opts.BeforeMatch)
	req = writeLenStrToBytes(req, opts.AfterMatch)
	req = writeLenStrToBytes(req, opts.ChunkSeparator)
	req = writeInt32ToBytes(req, opts.Limit)
	req = writeInt32ToBytes(req, opts.Around)
	req = writeInt32ToBytes(req, opts.LimitPassages)
	req = writeInt32ToBytes(req, opts.LimitWords)
	req = writeInt32ToBytes(req, opts.StartPassageId)
	req = writeLenStrToBytes(req, opts.HtmlStripMode)
	req = writeLenStrToBytes(req, opts.PassageBoundary)

	req = writeInt32ToBytes(req, len(docs))
	for _, doc := range docs {
		req = writeLenStrToBytes(req, doc)
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_EXCERPT, VER_COMMAND_EXCERPT, req)
	if err != nil {
		return nil, err
	}

	resDocs = make([]string, len(docs))
	p := 0
	for i := 0; i < len(docs); i++ {
		length := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		resDocs[i] = string(response[p : p+length])
		p += length
	}

	return resDocs, nil
}

/*
 Connect to searchd server and update given attributes on given documents in given indexes.
 values[*][0] is docId, must be an uint64.
 values[*][1:] should be int or []int(mva mode)
 'ndocs'	-1 on failure, amount of actually found and updated documents (might be 0) on success
*/
func (sc *Client) UpdateAttributes(index string, attrs []string, values [][]interface{}, ignorenonexistent bool) (ndocs int, err error) {
	if index == "" {
		return -1, errors.New("UpdateAttributes > index name is empty!")
	}
	if len(attrs) == 0 {
		return -1, errors.New("UpdateAttributes > no attribute names provided!")
	}
	if len(values) < 2 {
		return -1, errors.New("UpdateAttributes > no update values provided!")
	}

	for _, v := range values {
		// values[*][0] is docId, so +1
		if len(v) != len(attrs)+1 {
			return -1, fmt.Errorf("UpdateAttributes > update entry has wrong length: %#v", v)
		}
	}

	var mva bool
	if _, ok := values[0][1].([]int); ok {
		mva = true
	}

	// build request
	var req []byte
	req = writeLenStrToBytes(req, index)
	req = writeInt32ToBytes(req, len(attrs))

	if VER_COMMAND_UPDATE > 0x102 {
		if ignorenonexistent {
			req = writeInt32ToBytes(req, 1)
		} else {
			req = writeInt32ToBytes(req, 0)
		}
	}

	for _, attr := range attrs {
		req = writeLenStrToBytes(req, attr)
		if mva {
			req = writeInt32ToBytes(req, 1)
		} else {
			req = writeInt32ToBytes(req, 0)
		}
	}

	req = writeInt32ToBytes(req, len(values))
	for i := 0; i < len(values); i++ {
		if docId, ok := values[i][0].(uint64); !ok {
			return -1, fmt.Errorf("UpdateAttributes > docId must be uint64: %#v", docId)
		} else {
			req = writeInt64ToBytes(req, docId)
		}
		for j := 1; j < len(values[i]); j++ {
			if mva {
				vars, ok := values[i][j].([]int)
				if !ok {
					return -1, fmt.Errorf("UpdateAttributes > must be []int in mva mode: %#v", vars)
				}
				req = writeInt32ToBytes(req, len(vars))
				for _, v := range vars {
					req = writeInt32ToBytes(req, v)
				}
			} else {
				v, ok := values[i][j].(int)
				if !ok {
					return -1, fmt.Errorf("UpdateAttributes > must be int if not in mva mode: %#v", values[i][j])
				}
				req = writeInt32ToBytes(req, v)
			}
		}
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_UPDATE, VER_COMMAND_UPDATE, req)
	if err != nil {
		return -1, err
	}

	ndocs = int(binary.BigEndian.Uint32(response[0:4]))
	return
}

type Keyword struct {
	Tokenized  string "Tokenized"
	Normalized string "Normalized"
	Docs       int
	Hits       int
}

// Connect to searchd server, and generate keyword list for a given query.
// Returns null on failure, an array of Maps with misc per-keyword info on success.
func (sc *Client) BuildKeywords(query, index string, hits bool) (keywords []Keyword, err error) {
	var req []byte
	req = writeLenStrToBytes(req, query)
	req = writeLenStrToBytes(req, index)
	if hits {
		req = writeInt32ToBytes(req, 1)
	} else {
		req = writeInt32ToBytes(req, 0)
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_KEYWORDS, VER_COMMAND_KEYWORDS, req)
	if err != nil {
		return nil, err
	}

	p := 0
	nwords := int(binary.BigEndian.Uint32(response[p : p+4]))
	p += 4

	keywords = make([]Keyword, nwords)

	for i := 0; i < nwords; i++ {
		var k Keyword
		length := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		k.Tokenized = string(response[p : p+length])
		p += length

		length = int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		k.Normalized = string(response[p : p+length])
		p += length

		if hits {
			k.Docs = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			k.Hits = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
		}
		keywords[i] = k
	}

	return
}

func EscapeString(s string) string {
	chars := []string{`\`, `(`, `)`, `|`, `-`, `!`, `@`, `~`, `"`, `&`, `/`, `^`, `$`, `=`}
	for _, char := range chars {
		s = strings.Replace(s, char, `\`+char, -1)
	}
	return s
}

func (sc *Client) Status() (response [][]string, err error) {
	var req []byte
	req = writeInt32ToBytes(req, 1)

	res, err := sc.doRequest(SEARCHD_COMMAND_STATUS, VER_COMMAND_STATUS, req)
	if err != nil {
		return nil, err
	}

	p := 0
	rows := binary.BigEndian.Uint32(res[p : p+4])
	p += 4
	cols := binary.BigEndian.Uint32(res[p : p+4])
	p += 4

	response = make([][]string, rows)
	for i := 0; i < int(rows); i++ {
		response[i] = make([]string, cols)
		for j := 0; j < int(cols); j++ {
			length := int(binary.BigEndian.Uint32(res[p : p+4]))
			p += 4
			response[i][j] = string(res[p : p+length])
			p += length
		}
	}
	return response, nil
}

func (sc *Client) FlushAttributes() (iFlushTag int, err error) {
	res, err := sc.doRequest(SEARCHD_COMMAND_FLUSHATTRS, VER_COMMAND_FLUSHATTRS, []byte{})
	if err != nil {
		return -1, err
	}

	if len(res) != 4 {
		return -1, errors.New("FlushAttributes > unexpected response length!")
	}

	iFlushTag = int(binary.BigEndian.Uint32(res[0:4]))
	return
}

func (sc *Client) connect() (err error) {
	if sc.conn != nil {
		return
	}

	// set connerror to false.
	sc.connerror = false

	// Try unix socket first.
	if sc.socket != "" {
		if sc.conn, err = net.DialTimeout("unix", sc.socket, sc.timeout); err != nil {
			sc.connerror = true
			return fmt.Errorf("connect() net.DialTimeout(%d ms) > %v", sc.timeout/time.Millisecond, err)
		}
	} else if sc.port > 0 {
		if sc.conn, err = net.DialTimeout("tcp", fmt.Sprintf("%s:%d", sc.host, sc.port), sc.timeout); err != nil {
			sc.connerror = true
			return fmt.Errorf("connect() net.DialTimeout(%d ms) > %v", sc.timeout/time.Millisecond, err)
		}
	} else {
		return fmt.Errorf("connect() > No valid socket or port!\n%Client: #v", sc)
	}

	deadTime := time.Now().Add(time.Duration(sc.timeout) * time.Millisecond)
	if err = sc.conn.SetDeadline(deadTime); err != nil {
		sc.connerror = true
		return fmt.Errorf("connect() conn.SetDeadline() > %v", err)
	}

	header := make([]byte, 4)
	if _, err = io.ReadFull(sc.conn, header); err != nil {
		sc.connerror = true
		return fmt.Errorf("connect() io.ReadFull() > %v", err)
	}

	version := binary.BigEndian.Uint32(header)
	if version < 1 {
		return fmt.Errorf("connect() > expected searchd protocol version 1+, got version %d", version)
	}

	// send my version
	var i int
	i, err = sc.conn.Write(writeInt32ToBytes([]byte{}, VER_MAJOR_PROTO))
	if err != nil {
		sc.connerror = true
		return fmt.Errorf("connect() conn.Write() > %d bytes, %v", i, err)
	}

	return
}

func (sc *Client) Open() (err error) {
	if err = sc.connect(); err != nil {
		return fmt.Errorf("Open > %v", err)
	}

	var req []byte
	req = writeInt16ToBytes(req, SEARCHD_COMMAND_PERSIST)
	req = writeInt16ToBytes(req, 0) // command version
	req = writeInt32ToBytes(req, 4) // body length
	req = writeInt32ToBytes(req, 1) // body

	var n int
	n, err = sc.conn.Write(req)
	if err != nil {
		sc.connerror = true
		return fmt.Errorf("Open > sc.conn.Write() %d bytes, %v", n, err)
	}

	return nil
}

func (sc *Client) Close() error {
	if sc.conn == nil {
		return errors.New("Close > Not connected!")
	}

	if err := sc.conn.Close(); err != nil {
		return err
	}

	sc.conn = nil
	return nil
}

func (sc *Client) doRequest(command int, version int, req []byte) (res []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			res = nil
			err = fmt.Errorf("doRequest panic > %#v", x)
		}
	}()

	if err = sc.connect(); err != nil {
		return nil, err
	}

	var cmdVerLen []byte
	cmdVerLen = writeInt16ToBytes(cmdVerLen, command)
	cmdVerLen = writeInt16ToBytes(cmdVerLen, version)
	cmdVerLen = writeInt32ToBytes(cmdVerLen, len(req))
	req = append(cmdVerLen, req...)
	_, err = sc.conn.Write(req)
	if err != nil {
		sc.connerror = true
		return nil, fmt.Errorf("conn.Write error: %v", err)
	}

	header := make([]byte, 8)
	if i, err := io.ReadFull(sc.conn, header); err != nil {
		sc.connerror = true
		return nil, fmt.Errorf("doRequest > just read %d bytes into header!", i)
	}

	status := binary.BigEndian.Uint16(header[0:2])
	ver := binary.BigEndian.Uint16(header[2:4])
	size := binary.BigEndian.Uint32(header[4:8])
	if size <= 0 || size > 10*1024*1024 {
		return nil, fmt.Errorf("doRequest > invalid response packet size (len=%d).", size)
	}

	res = make([]byte, size)
	if i, err := io.ReadFull(sc.conn, res); err != nil {
		sc.connerror = true
		return nil, fmt.Errorf("doRequest > just read %d bytes into res (size=%d).", i, size)
	}

	switch status {
	case SEARCHD_OK:
		// do nothing
	case SEARCHD_WARNING:
		wlen := binary.BigEndian.Uint32(res[0:4])
		sc.warning = string(res[4:wlen])
		res = res[4+wlen:]
	case SEARCHD_ERROR, SEARCHD_RETRY:
		wlen := binary.BigEndian.Uint32(res[0:4])
		return nil, fmt.Errorf("doRequest > SEARCHD_ERROR: " + string(res[4:wlen]))
	default:
		return nil, fmt.Errorf("doRequest > unknown status code (status=%d), ver: %d", status, ver)
	}

	return res, nil
}

func writeFloat32ToBytes(bs []byte, f float32) []byte {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, f); err != nil {
		fmt.Println(err)
	}
	return append(bs, buf.Bytes()...)
}

func writeInt16ToBytes(bs []byte, i int) []byte {
	var byte2 = make([]byte, 2)
	binary.BigEndian.PutUint16(byte2, uint16(i))
	return append(bs, byte2...)
}

func writeInt32ToBytes(bs []byte, i int) []byte {
	var byte4 = make([]byte, 4)
	binary.BigEndian.PutUint32(byte4, uint32(i))
	return append(bs, byte4...)
}

func writeInt64ToBytes(bs []byte, ui uint64) []byte {
	var byte8 = make([]byte, 8)
	binary.BigEndian.PutUint64(byte8, ui)
	return append(bs, byte8...)
}

func writeLenStrToBytes(bs []byte, s string) []byte {
	var byte4 = make([]byte, 4)
	binary.BigEndian.PutUint32(byte4, uint32(len(s)))
	bs = append(bs, byte4...)
	return append(bs, []byte(s)...)
}

// For SetGeoAnchor()
func DegreeToRadian(degree float32) float32 {
	return degree * math.Pi / 180
}
