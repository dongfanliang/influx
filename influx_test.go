package influx

import (
	"testing"
	"time"
)

var url = "http://localhost:8086"

func TestInfluxWrite(t *testing.T) {
	_ = t
	client := NewInfluxClient(url)
	serise := &Serise{
		Metric: "redis_endpoints",
		Tags: map[string]string{
			"endpoints": "dm-xxx",
		},
		Data: []Point{},
	}
	serise1 := &Serise{
		Metric: "redis_endpoints",
		Tags: map[string]string{
			"endpoints": "dm-xxx",
		},
		Data: []Point{},
	}

	now := time.Now().Unix()
	for i := 0; i < 10; i++ {
		serise.Data = append(serise.Data, Point{Timestamp: now - int64(i*10), Value: 0.0})
		serise1.Data = append(serise.Data, Point{Timestamp: now - int64(i*10), Value: float64(i)})
	}

	client.BatchWrite("sla_meta", "redis_endpoints", serise)
	client.BatchWrite("sla_meta", "redis_endpoints", serise1)
	client.Close()
}

func TestInfluxRead(t *testing.T) {
	client := NewInfluxClient(url)
	sql := `
	from(bucket: "sla_meta")
	|> range(start: -5h)
	|> filter(fn: (r) =>
	  r._measurement == "redis_endpoints"
	)
	|> group(columns: ["endpoints"], mode:"by")
	`
	data, err := client.Query(sql)
	defer client.Close()
	if err != nil {
		t.Log(err)
	}
	for _, d := range data {
		t.Logf("%v", d)
	}
}
