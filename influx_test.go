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
		Metric: "cpu.used",
		Tags: map[string]string{
			"id":       "rack",
			"vendor":   "AWS",
			"hostname": "hostname",
		},
		Data: []Point{},
	}

	now := time.Now().Unix()
	for i := 0; i < 10; i++ {
		serise.Data = append(serise.Data, Point{Timestamp: now - int64(i*10), Value: float64(i)})
	}

	client.BatchWrite("mydb1", "mytest", serise)
	client.Close()
}

func TestInfluxRead(t *testing.T) {
	client := NewInfluxClient(url)
	sql := `
	from(bucket: "mydb1")
	|> range(start: -5h)
	|> filter(fn: (r) =>
	  r._measurement == "mytest"
	)
	`
	data, err := client.Query(sql)
	defer client.Close()
	if err != nil {
		t.Log(err)
	}
	for _, d := range data {
		t.Logf("%#v", d)
	}
}
