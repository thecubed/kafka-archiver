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
)

// TimeFieldPartitioner partitions the data in hourly buckets based on a given timestamp.
// The field must be a unix timestamp in milliseconds.
type KafkaDatePartitioner struct {
	BasePartitioner
	fieldName string
}

// GetBuffer returns a buffer that can be used to write the message to.
func (t *KafkaDatePartitioner) GetBuffer(msg *sarama.ConsumerMessage) (*buffer.Buffer, error) {

	// Get the hour Bucket for the given message.
	dateBucket := msg.Timestamp
	glog.V(6).Infof("getting buffer for topic=%s partition=%d timestamp=%s", msg.Topic, msg.Partition, msg.Timestamp)

	// Generate a context with hourBucket.
	ctx := context.WithValue(context.Background(), "dateBucket", dateBucket)

	// Get the buffer.
	buf, err := t.getBuffer(ctx, t.partition(dateBucket, msg), msg)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// partition returns the hash key based on topic, partition and time bucket.
func (t *KafkaDatePartitioner) partition(dateBucket time.Time, msg *sarama.ConsumerMessage) string {
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
func (t *KafkaDatePartitioner) GetKey(f *buffer.Flush) string {
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
