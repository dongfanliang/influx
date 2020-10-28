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

type InfluxClient struct {
	Client    influxdb2.Client
	WriteAPIs map[string]api.WriteAPI
}

func NewInfluxClient(url string) *InfluxClient {
	client := influxdb2.NewClientWithOptions(url, "", influxdb2.DefaultOptions().SetPrecision(time.Second))
	return &InfluxClient{
		Client:    client,
		WriteAPIs: make(map[string]api.WriteAPI),
	}
}

func (this *InfluxClient) Query(sql string) ([]*Serise, error) {
	queryApi := this.Client.QueryAPI("")
	result, err := queryApi.Query(context.Background(), sql)
	if err != nil {
		return []*Serise{}, err
	}

	tmpData := map[string]*Serise{}
	tags := map[string]string{}
	key := ""
	IsGroup := false
	for result.Next() {
		record := result.Record()
		if result.TableChanged() {
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
					if _, ok := tags[k]; ok {
						tags[k] = v.(string)
					}
				}
			}
			key = SortedTags(tags) + record.Field()
			tmpData[key] = &Serise{
				Metric: record.Field(),
				Tags:   tags,
				Data:   []Point{Point{Timestamp: record.Time().Unix(), Value: record.Value().(float64)}},
			}
			continue
		}

		tmpData[key].Data = append(tmpData[key].Data, Point{Timestamp: record.Time().Unix(), Value: record.Value().(float64)})
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
	if _, ok := this.WriteAPIs[db]; !ok {
		writeAPI := this.Client.WriteAPI("org", db)
		errorsCh := writeAPI.Errors()
		go func() {
			for err := range errorsCh {
				fmt.Printf("influx write error: %s\n", err.Error())
			}
		}()

		this.WriteAPIs[db] = this.Client.WriteAPI("org", db)
	}

	writeAPI := this.WriteAPIs[db]
	tags := serise.Tags
	metric := serise.Metric
	for i, _ := range serise.Data {
		p := influxdb2.NewPoint(
			table,
			tags,
			map[string]interface{}{metric: serise.Data[i].Value},
			time.Unix(serise.Data[i].Timestamp, 0),
		)
		writeAPI.WritePoint(p)
	}
	writeAPI.Flush()
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
