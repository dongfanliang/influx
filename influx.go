package influx

import (
	"context"
	"fmt"
	"strings"
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

	tmpData := map[int]*Serise{}
	for result.Next() {
		record := result.Record()
		table := record.Table()
		if _, ok := tmpData[table]; !ok {
			tags := map[string]string{}
			for k, v := range result.Record().Values() {
				if strings.HasPrefix(k, "_") || k == "result" || k == "table" {
					continue
				}
				tags[k] = v.(string)
			}

			tmpData[table] = &Serise{
				Metric: record.Field(),
				Tags:   tags,
				Data:   []Point{},
			}
		}

		tmpData[table].Data = append(tmpData[table].Data, Point{Timestamp: record.Time().Unix(), Value: record.Value().(float64)})
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
