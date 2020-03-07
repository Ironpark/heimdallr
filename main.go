package heimdallr

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/golang/snappy"
	l "github.com/heimdallr/logproto"
	"github.com/segmentio/kafka-go"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

func main() {
	var (
		lokiAddr   = flag.String("lokiurl", "http://loki:3100", "the loki url, default is http://loki:3100")
		topicName  = flag.String("topic", "logging", "Regex pattern for kafka topic subscription")
		brokerList = flag.String("brokers", "localhost:9092", "the kafka broker list")
		labels     = flag.String("labels", "application,client,ip,region,id,name,email", "Regex pattern for kafka topic subscription")
	)
	labelKeys := map[string]bool{}
	for _, lv := range strings.Split(*labels, ",") {
		labelKeys[lv] = true
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(*brokerList, ","),
		GroupID: "heimdallr.log-writer",
		Topic:   *topicName,
	})

	logs := []Log{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if len(logs) == 0 {
				continue
			}
			go sendToLoki(*lokiAddr, logs, labelKeys)
			logs = make([]Log, 0, 1000)
		default:
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			logEntry := Log{}
			logEntry.JSON = string(m.Value)
			logEntry.TimeStamp = m.Time
			json.Unmarshal(m.Value, &logEntry.Log)
			logs = append(logs, logEntry)
		}
	}

	r.Close()
}

type Log struct {
	TimeStamp time.Time
	Log       map[string]json.RawMessage
	JSON      string
}

func sendToLoki(lokiURL string, logs []Log, labelKeys map[string]bool) {
	body := map[string][]l.Entry{}

	for _, logEntry := range logs {
		labels := []byte{}
		labels = append(labels, '{')
		for k, v := range logEntry.Log {
			if v[0] == '{' || v[0] == '[' {
				continue
			}
			if labelKeys[k] {
				labels = append(labels, '"')
				labels = append(labels, k...)
				labels = append(labels, "\":"...)
				labels = append(labels, v...)
				labels = append(labels, '"')
			}
			delete(logEntry.Log, k)
		}
		labels = append(labels, '}')

		entry := l.Entry{Timestamp: logEntry.TimeStamp, Line: logEntry.JSON}
		if val, ok := body[string(labels)]; ok {
			body[string(labels)] = append(val, entry)
		} else {
			body[string(labels)] = []l.Entry{entry}
		}
	}

	streams := make([]*l.Stream, 0, len(body))

	for key, val := range body {
		stream := &l.Stream{
			Labels:  key,
			Entries: val,
		}
		sort.SliceStable(stream.Entries, func(i, j int) bool {
			return stream.Entries[i].Timestamp.Before(stream.Entries[j].Timestamp)
		})
		streams = append(streams, stream)
	}

	req := l.PushRequest{
		Streams: streams,
	}

	buf, err := req.Marshal() //proto.Marshal(&req)

	if err != nil {
		return
	}

	buf = snappy.Encode(nil, buf)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	httpreq, err := http.NewRequest(http.MethodPost, lokiURL, bytes.NewReader(buf))

	if err != nil {
		return
	}

	httpreq = httpreq.WithContext(ctx)
	httpreq.Header.Add("Content-Type", "application/x-protobuf")

	client := http.Client{}

	resp, err := client.Do(httpreq)

	if err != nil {
		return
	}

	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s (%d): %s", resp.Status, resp.StatusCode, line)
		return
	}
}
