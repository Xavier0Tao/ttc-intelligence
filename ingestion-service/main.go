package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const (
	vehiclesFeedURL     = "https://bustime.ttc.ca/gtfsrt/vehicles"
	alertsFeedURL       = "https://bustime.ttc.ca/gtfsrt/alerts"
	vehiclePollInterval = 15 * time.Second
	alertPollInterval   = 30 * time.Second
	vehiclesTopic       = "vehicle-positions"
	alertsTopic         = "service-alerts"
	vehiclesSubject     = "vehicle-positions-value"
	alertsSubject       = "service-alerts-value"
)

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	schemasDir := getEnv("SCHEMAS_DIR", filepath.Join("..", "schemas"))

	vehicleCodec, vehicleSchemaID := mustPrepareSchema(schemaRegistryURL, schemasDir, "vehicle_position.avsc", vehiclesSubject)
	alertCodec, alertSchemaID := mustPrepareSchema(schemaRegistryURL, schemasDir, "service_alert.avsc", alertsSubject)

	vehicleWriter := newWriter(broker, vehiclesTopic)
	defer vehicleWriter.Close()
	alertWriter := newWriter(broker, alertsTopic)
	defer alertWriter.Close()

	httpClient := &http.Client{Timeout: 10 * time.Second}

	log.Printf("ingestion-service started: vehicles every %s -> %q, alerts every %s -> %q (broker %q)",
		vehiclePollInterval, vehiclesTopic, alertPollInterval, alertsTopic, broker)

	var wg sync.WaitGroup
	wg.Add(2)
	go pollVehicles(&wg, httpClient, vehicleWriter, vehicleCodec, vehicleSchemaID)
	go pollAlerts(&wg, httpClient, alertWriter, alertCodec, alertSchemaID)
	wg.Wait()
}

func mustPrepareSchema(registryURL, schemasDir, filename, subject string) (*goavro.Codec, int) {
	schema, err := loadSchema(schemasDir, filename)
	if err != nil {
		log.Fatalf("failed to load schema: %v", err)
	}
	id, err := registerSchema(registryURL, subject, schema)
	if err != nil {
		log.Fatalf("failed to register schema for subject %q: %v", subject, err)
	}
	log.Printf("registered Avro schema for subject %q (schema id %d)", subject, id)
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatalf("failed to create avro codec for %s: %v", filename, err)
	}
	return codec, id
}

func newWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
}

// ---------------------------------------------------------------------------
// Vehicle positions poller
// ---------------------------------------------------------------------------

func pollVehicles(wg *sync.WaitGroup, client *http.Client, writer *kafka.Writer, codec *goavro.Codec, schemaID int) {
	defer wg.Done()

	ticker := time.NewTicker(vehiclePollInterval)
	defer ticker.Stop()

	pollAndPublishVehicles(client, writer, codec, schemaID)
	for range ticker.C {
		pollAndPublishVehicles(client, writer, codec, schemaID)
	}
}

func pollAndPublishVehicles(client *http.Client, writer *kafka.Writer, codec *goavro.Codec, schemaID int) {
	feed, err := fetchFeed(client, vehiclesFeedURL)
	if err != nil {
		log.Printf("error fetching vehicles feed: %v", err)
		return
	}

	published := 0
	for _, entity := range feed.GetEntity() {
		vehicle := entity.GetVehicle()
		if vehicle == nil {
			continue
		}

		vehicleID, native := vehicleToAvroNative(vehicle)
		payload, err := encodeConfluentAvro(codec, schemaID, native)
		if err != nil {
			log.Printf("error encoding vehicle event: %v", err)
			continue
		}
		if err := produce(writer, vehicleID, payload); err != nil {
			log.Printf("error producing vehicle message: %v", err)
			continue
		}

		log.Printf("published vehicle position: vehicle_id=%s route_id=%s lat=%.5f lon=%.5f",
			vehicleID, native["route_id"], native["latitude"], native["longitude"])
		published++
	}

	log.Printf("Vehicles poll: %d positions published", published)
}

func vehicleToAvroNative(v *gtfs.VehiclePosition) (string, map[string]interface{}) {
	vehicleID := ""
	if id := v.GetVehicle(); id != nil {
		vehicleID = id.GetId()
	}

	routeID := ""
	if trip := v.GetTrip(); trip != nil {
		routeID = trip.GetRouteId()
	}

	native := map[string]interface{}{
		"vehicle_id":            vehicleID,
		"route_id":              routeID,
		"latitude":              0.0,
		"longitude":             0.0,
		"timestamp":             int64(v.GetTimestamp()),
		"bearing":               nil,
		"speed":                 nil,
		"occupancy":             nil,
		"trip_id":               nil,
		"direction_id":          nil,
		"current_stop_sequence": nil,
	}

	if trip := v.GetTrip(); trip != nil {
		if trip.TripId != nil {
			native["trip_id"] = goavro.Union("string", trip.GetTripId())
		}
		if trip.DirectionId != nil {
			native["direction_id"] = goavro.Union("int", int32(trip.GetDirectionId()))
		}
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
	if v.CurrentStopSequence != nil {
		native["current_stop_sequence"] = goavro.Union("int", int32(v.GetCurrentStopSequence()))
	}

	return vehicleID, native
}

// ---------------------------------------------------------------------------
// Service alerts poller
// ---------------------------------------------------------------------------

func pollAlerts(wg *sync.WaitGroup, client *http.Client, writer *kafka.Writer, codec *goavro.Codec, schemaID int) {
	defer wg.Done()

	// alert_id -> content fingerprint of the last published version. GTFS-RT
	// alerts carry no per-alert timestamp (the feed header timestamp changes
	// on every poll), so "changed" is detected by comparing alert content.
	published := make(map[string]string)

	ticker := time.NewTicker(alertPollInterval)
	defer ticker.Stop()

	pollAndPublishAlerts(client, writer, codec, schemaID, published)
	for range ticker.C {
		pollAndPublishAlerts(client, writer, codec, schemaID, published)
	}
}

func pollAndPublishAlerts(client *http.Client, writer *kafka.Writer, codec *goavro.Codec, schemaID int, publishedAlerts map[string]string) {
	feed, err := fetchFeed(client, alertsFeedURL)
	if err != nil {
		log.Printf("error fetching alerts feed: %v", err)
		return
	}

	feedTimestamp := int64(feed.GetHeader().GetTimestamp())
	if feedTimestamp == 0 {
		feedTimestamp = time.Now().Unix()
	}

	active := 0
	newPublished := 0
	for _, entity := range feed.GetEntity() {
		alert := entity.GetAlert()
		if alert == nil {
			continue
		}
		active++

		alertID := entity.GetId()
		native := alertToAvroNative(alertID, alert, feedTimestamp)
		fingerprint := alertFingerprint(native)

		if prev, seen := publishedAlerts[alertID]; seen && prev == fingerprint {
			continue
		}

		payload, err := encodeConfluentAvro(codec, schemaID, native)
		if err != nil {
			log.Printf("error encoding alert %s: %v", alertID, err)
			continue
		}
		if err := produce(writer, alertID, payload); err != nil {
			log.Printf("error producing alert message: %v", err)
			continue
		}

		publishedAlerts[alertID] = fingerprint
		newPublished++
		log.Printf("published service alert: alert_id=%s effect=%s header=%q",
			alertID, native["effect"], native["header_text"])
	}

	log.Printf("Alerts poll: %d active, %d new published", active, newPublished)
}

func alertToAvroNative(alertID string, a *gtfs.Alert, feedTimestamp int64) map[string]interface{} {
	routeIDs := make([]string, 0)
	stopIDs := make([]string, 0)
	seenRoutes := make(map[string]bool)
	seenStops := make(map[string]bool)
	for _, informed := range a.GetInformedEntity() {
		if r := informed.GetRouteId(); r != "" && !seenRoutes[r] {
			seenRoutes[r] = true
			routeIDs = append(routeIDs, r)
		}
		if s := informed.GetStopId(); s != "" && !seenStops[s] {
			seenStops[s] = true
			stopIDs = append(stopIDs, s)
		}
	}

	activeSince := feedTimestamp
	if periods := a.GetActivePeriod(); len(periods) > 0 && periods[0].GetStart() > 0 {
		activeSince = int64(periods[0].GetStart())
	}

	native := map[string]interface{}{
		"alert_id":           alertID,
		"effect":             a.GetEffect().String(),
		"cause":              nil,
		"header_text":        firstTranslation(a.GetHeaderText()),
		"description_text":   nil,
		"affected_route_ids": routeIDs,
		"affected_stop_ids":  stopIDs,
		"active_since":       activeSince,
		"timestamp":          feedTimestamp,
	}

	if a.Cause != nil {
		native["cause"] = goavro.Union("string", a.GetCause().String())
	}
	if desc := firstTranslation(a.GetDescriptionText()); desc != "" {
		native["description_text"] = goavro.Union("string", desc)
	}

	return native
}

// alertFingerprint captures the fields that constitute a meaningful change to
// an alert. timestamp and active_since are deliberately excluded: both fall
// back to the feed-header timestamp (which moves every poll) when the alert
// carries no explicit values, and including them would defeat deduplication.
func alertFingerprint(native map[string]interface{}) string {
	routes := append([]string(nil), native["affected_route_ids"].([]string)...)
	stops := append([]string(nil), native["affected_stop_ids"].([]string)...)
	sort.Strings(routes)
	sort.Strings(stops)
	return fmt.Sprintf("%v|%v|%v|%v|%s|%s",
		native["effect"], native["cause"], native["header_text"], native["description_text"],
		strings.Join(routes, ","), strings.Join(stops, ","))
}

func firstTranslation(ts *gtfs.TranslatedString) string {
	for _, t := range ts.GetTranslation() {
		if text := t.GetText(); text != "" {
			return text
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

func produce(writer *kafka.Writer, key string, payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
	})
}

func loadSchema(schemasDir, filename string) (string, error) {
	data, err := os.ReadFile(filepath.Join(schemasDir, filename))
	if err != nil {
		return "", fmt.Errorf("reading %s from %s: %w", filename, schemasDir, err)
	}
	return string(data), nil
}

// registerSchema registers an Avro schema under the given subject and returns
// the schema id assigned by the registry. Registration is idempotent:
// re-registering an identical schema returns the existing id, and a
// backward-compatible evolution registers a new version of the same subject.
// Retries while the registry is still coming up.
func registerSchema(registryURL, subject, schema string) (int, error) {
	body, err := json.Marshal(map[string]string{"schema": schema})
	if err != nil {
		return 0, err
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", registryURL, subject)
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
		log.Printf("schema registration attempt %d/10 for %s failed: %v (retrying in 3s)", attempt, subject, lastErr)
		time.Sleep(3 * time.Second)
	}
	return 0, lastErr
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

func fetchFeed(client *http.Client, url string) (*gtfs.FeedMessage, error) {
	resp, err := client.Get(url)
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

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
