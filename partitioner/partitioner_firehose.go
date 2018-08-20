package partitioner

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/Shopify/sarama"
	"github.com/UnityTech/kafka-archiver/buffer"
	"github.com/golang/glog"
	"github.com/buger/jsonparser"
)

// TimeFieldPartitioner partitions the data in hourly buckets based on a given timestamp.
// The field must be a unix timestamp in milliseconds.
type FirehosePartitioner struct {
	BasePartitioner
	fieldName string
}

// GetBuffer returns a buffer that can be used to write the message to.
func (t *FirehosePartitioner) GetBuffer(msg *sarama.ConsumerMessage) (*buffer.Buffer, error) {

	// Get the hour Bucket for the given message.
	_, fhMsg := decode_firehose(msg.Value)

	var ts time.Time
	if ts_tmp, err := jsonparser.GetString(fhMsg, t.fieldName); err != nil {
		glog.Errorf("error getting timestamp from key %q in %q: %v", t.fieldName, msg.Topic, err)
		ts = time.Now()
	} else {
		ts, err = time.Parse(time.RFC3339, ts_tmp)
		if err != nil {
			glog.Errorf("error parsing timestamp in key %q, error was: %q", t.fieldName, err)
			ts = time.Now()
		}
	}

	dt := ts.Truncate(time.Hour)

	glog.V(6).Infof("getting buffer for topic=%s partition=%d timestamp=%s", msg.Topic, msg.Partition, dt)

	// Generate a context with dateBucket.
	ctx := context.WithValue(context.Background(), "dateBucket", dt)

	// Get the buffer.
	buf, err := t.getBuffer(ctx, t.partition(dt, msg), msg)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// partition returns the hash key based on topic, partition and time bucket.
func (t *FirehosePartitioner) partition(dateBucket time.Time, msg *sarama.ConsumerMessage) string {
	w := md5.New()

	if _, err := io.WriteString(w, msg.Topic); err != nil {
		glog.Fatal(err)
	}

	if _, err := io.WriteString(w, fmt.Sprintf("%d", msg.Partition)); err != nil {
		glog.Fatal(err)
	}

	if _, err := io.WriteString(w, dateBucket.Format(time.RFC3339)); err != nil {
		glog.Fatal(err)
	}

	return fmt.Sprintf("%x", w.Sum(nil))
}

// GetKey returns the file path used after a file is flushed.
func (t *FirehosePartitioner) GetKey(f *buffer.Flush) string {
	dateBucket := f.Ctx.Value("dateBucket").(time.Time)

	return filepath.Join(
		t.getBasePath(f),
		fmt.Sprintf(
			"date=%d-%02d-%02d/",
			dateBucket.Year(),
			dateBucket.Month(),
			dateBucket.Day(),
		),
		t.getBaseFileName(f),
	)
}


func decode_firehose(msg []byte) (string, []byte) {

	if msg[0] != 1 {
		return "", []byte{}
	}

	topicLen := uint8(msg[1])

	end := topicLen + 6

	topic := string(msg[6:end])
	msgStart := end + 1
	message := msg[msgStart:]

	return topic, message
}
