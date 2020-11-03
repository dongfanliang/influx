package influx

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type Point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type Serise struct {
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
	Data   []Point           `json:"data"`
}

type WriteAPI struct {
	Api api.WriteAPI
}

type InfluxClient struct {
	Client    influxdb2.Client
	WriteAPIs map[string]*WriteAPI
	sync.RWMutex
}

func (this *InfluxClient) GetWriteAPI(key string) (*WriteAPI, bool) {
	this.RLock()
	v, ok := this.WriteAPIs[key]
	this.RUnlock()

	return v, ok
}

func (this *InfluxClient) SetWriteAPI(key string, api *WriteAPI) {
	this.Lock()
	this.WriteAPIs[key] = api
	this.Unlock()
}

func NewInfluxClient(url string) *InfluxClient {
	client := influxdb2.NewClientWithOptions(url, "", influxdb2.DefaultOptions().SetPrecision(time.Second))
	return &InfluxClient{
		Client:    client,
		WriteAPIs: make(map[string]*WriteAPI),
	}
}

func (this *InfluxClient) Query(sql string) ([]*Serise, error) {
	queryApi := this.Client.QueryAPI("")
	result, err := queryApi.Query(context.Background(), sql)
	if err != nil {
		return []*Serise{}, err
	}

	var (
		tmpData = map[string]*Serise{}
		tags    = map[string]string{}
		tmpTime = map[int64]float64{}
		tmpV    float64
		IsGroup = false
		key     = ""
		ok      bool
	)

	for result.Next() {
		record := result.Record()
		ts := record.Time().Unix()
		v := record.Value().(float64)
		if result.TableChanged() {
			tmpTime = map[int64]float64{}
			tags = map[string]string{}
			IsGroup = false

			meta := result.TableMetadata()
			for _, c := range meta.Columns() {
				if c.IsGroup() {
					IsGroup = true
					name := c.Name()
					if !strings.HasPrefix(name, "_") {
						tags[name] = ""
					}
				}
			}

			for k, v := range record.Values() {
				if strings.HasPrefix(k, "_") || k == "result" || k == "table" {
					continue
				}

				if !IsGroup {
					tags[k] = v.(string)
				} else {
					if _, ok = tags[k]; ok {
						tags[k] = v.(string)
					}
				}
			}
			key = SortedTags(tags) + record.Field()
			tmpData[key] = &Serise{
				Metric: record.Field(),
				Tags:   tags,
				Data:   []Point{Point{Timestamp: ts, Value: v}},
			}
			tmpTime[ts] = v
		} else {
			tmpV, ok = tmpTime[ts]
			if !ok || v < tmpV {
				tmpData[key].Data = append(tmpData[key].Data, Point{Timestamp: ts, Value: v})
			}
		}
	}

	seriseData := make([]*Serise, len(tmpData))
	i := 0
	for _, v := range tmpData {
		seriseData[i] = v
		i++
	}
	return seriseData, nil
}

func (this *InfluxClient) BatchWrite(db, table string, serise *Serise) {
	writeAPI, ok := this.GetWriteAPI(db)
	if !ok {
		writeAPI = &WriteAPI{Api: this.Client.WriteAPI("org", db)}
		this.SetWriteAPI(db, writeAPI)

		errorsCh := writeAPI.Api.Errors()
		go func() {
			for err := range errorsCh {
				fmt.Printf("influx write db: %s, table: %s, error: %s\n", db, table, err.Error())
			}
		}()
	}

	tags := serise.Tags
	for i, _ := range serise.Data {
		p := influxdb2.NewPoint(
			table,
			tags,
			map[string]interface{}{"__value__": serise.Data[i].Value},
			time.Unix(serise.Data[i].Timestamp, 0),
		)
		writeAPI.Api.WritePoint(p)
	}
	writeAPI.Api.Flush()
}

func (this *InfluxClient) Close() {
	if this.Client != nil {
		this.Client.Close()
	}
}

var TmpBufferPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func SortedTags(tags map[string]string) string {
	if tags == nil {
		return ""
	}

	size := len(tags)

	if size == 0 {
		return ""
	}

	ret := TmpBufferPool.Get().(*bytes.Buffer)
	ret.Reset()
	defer TmpBufferPool.Put(ret)

	if size == 1 {
		for k, v := range tags {
			ret.WriteString(k)
			ret.WriteString("=")
			ret.WriteString(v)
		}
		return ret.String()
	}

	keys := make([]string, size)
	i := 0
	for k := range tags {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	for j, key := range keys {
		ret.WriteString(key)
		ret.WriteString("=")
		ret.WriteString(tags[key])
		if j != size-1 {
			ret.WriteString(",")
		}
	}

	return ret.String()
}
