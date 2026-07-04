package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

//go:embed avro/vehicle_position.avsc
var vehiclePositionSchema string

const (
	feedURL       = "https://bustime.ttc.ca/gtfsrt/vehicles"
	pollInterval  = 15 * time.Second
	kafkaTopic    = "vehicle-positions"
	schemaSubject = "vehicle-positions-value"
)

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

	schemaID, err := registerSchema(schemaRegistryURL)
	if err != nil {
		log.Fatalf("failed to register schema with schema registry: %v", err)
	}
	log.Printf("registered Avro schema for subject %q (schema id %d)", schemaSubject, schemaID)

	codec, err := goavro.NewCodec(vehiclePositionSchema)
	if err != nil {
		log.Fatalf("failed to create avro codec: %v", err)
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  kafkaTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	log.Printf("ingestion-service started: polling %s every %s, publishing Avro to topic %q on broker %q",
		feedURL, pollInterval, kafkaTopic, broker)

	httpClient := &http.Client{Timeout: 10 * time.Second}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	pollAndPublish(httpClient, writer, codec, schemaID)
	for range ticker.C {
		pollAndPublish(httpClient, writer, codec, schemaID)
	}
}

// registerSchema registers the embedded Avro schema under the subject for the
// vehicle-positions topic and returns the schema id assigned by the registry.
// Registration is idempotent: re-registering an identical schema returns the
// existing id. Retries while the registry is still coming up.
func registerSchema(registryURL string) (int, error) {
	body, err := json.Marshal(map[string]string{"schema": vehiclePositionSchema})
	if err != nil {
		return 0, err
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", registryURL, schemaSubject)
	client := &http.Client{Timeout: 10 * time.Second}

	var lastErr error
	for attempt := 1; attempt <= 10; attempt++ {
		resp, err := client.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
		} else {
			respBody, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				lastErr = readErr
			} else if resp.StatusCode != http.StatusOK {
				lastErr = fmt.Errorf("schema registry returned %d: %s", resp.StatusCode, respBody)
			} else {
				var result struct {
					ID int `json:"id"`
				}
				if err := json.Unmarshal(respBody, &result); err != nil {
					return 0, fmt.Errorf("invalid schema registry response: %w", err)
				}
				return result.ID, nil
			}
		}
		log.Printf("schema registration attempt %d/10 failed: %v (retrying in 3s)", attempt, lastErr)
		time.Sleep(3 * time.Second)
	}
	return 0, lastErr
}

func pollAndPublish(client *http.Client, writer *kafka.Writer, codec *goavro.Codec, schemaID int) {
	feed, err := fetchFeed(client)
	if err != nil {
		log.Printf("error fetching GTFS-RT feed: %v", err)
		return
	}

	published := 0
	for _, entity := range feed.GetEntity() {
		vehicle := entity.GetVehicle()
		if vehicle == nil {
			continue
		}

		vehicleID, native := toAvroNative(vehicle)
		payload, err := encodeConfluentAvro(codec, schemaID, native)
		if err != nil {
			log.Printf("error encoding vehicle event: %v", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(vehicleID),
			Value: payload,
		})
		cancel()
		if err != nil {
			log.Printf("error producing message: %v", err)
			continue
		}

		log.Printf("published vehicle position: vehicle_id=%s route_id=%s lat=%.5f lon=%.5f",
			vehicleID, native["route_id"], native["latitude"], native["longitude"])
		published++
	}

	log.Printf("poll complete: %d vehicle positions published", published)
}

// encodeConfluentAvro serializes a native Avro value using the Confluent wire
// format: magic byte 0x00, 4-byte big-endian schema id, then Avro binary.
func encodeConfluentAvro(codec *goavro.Codec, schemaID int, native map[string]interface{}) ([]byte, error) {
	avroBytes, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 5, 5+len(avroBytes))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	return append(buf, avroBytes...), nil
}

func fetchFeed(client *http.Client) (*gtfs.FeedMessage, error) {
	resp, err := client.Get(feedURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	feed := &gtfs.FeedMessage{}
	if err := proto.Unmarshal(body, feed); err != nil {
		return nil, err
	}

	return feed, nil
}

func toAvroNative(v *gtfs.VehiclePosition) (string, map[string]interface{}) {
	vehicleID := ""
	if id := v.GetVehicle(); id != nil {
		vehicleID = id.GetId()
	}

	routeID := ""
	if trip := v.GetTrip(); trip != nil {
		routeID = trip.GetRouteId()
	}

	native := map[string]interface{}{
		"vehicle_id": vehicleID,
		"route_id":   routeID,
		"latitude":   0.0,
		"longitude":  0.0,
		"timestamp":  int64(v.GetTimestamp()),
		"bearing":    nil,
		"speed":      nil,
		"occupancy":  nil,
	}

	if pos := v.GetPosition(); pos != nil {
		native["latitude"] = float64(pos.GetLatitude())
		native["longitude"] = float64(pos.GetLongitude())
		if pos.Bearing != nil {
			native["bearing"] = goavro.Union("float", pos.GetBearing())
		}
		if pos.Speed != nil {
			native["speed"] = goavro.Union("float", pos.GetSpeed())
		}
	}
	if v.OccupancyStatus != nil {
		native["occupancy"] = goavro.Union("string", v.GetOccupancyStatus().String())
	}

	return vehicleID, native
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
