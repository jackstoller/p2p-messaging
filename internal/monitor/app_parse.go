package monitor

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func parseLogLine(line string) (parsedLine, bool) {
	source, payload := splitComposeLogLine(line)
	fields := tokenizeKeyValues(payload)
	if len(fields) == 0 || fields["event"] == "" {
		return parsedLine{}, false
	}
	ts, _ := time.Parse(time.RFC3339Nano, fields["ts"])
	return parsedLine{Source: source, Raw: line, Fields: fields, Time: ts}, true
}

func splitComposeLogLine(line string) (string, string) {
	payload := strings.TrimSpace(line)
	if idx := strings.Index(payload, "|"); idx >= 0 {
		left := strings.TrimSpace(payload[:idx])
		right := strings.TrimSpace(payload[idx+1:])
		return left, right
	}
	return "", payload
}

func tokenizeKeyValues(input string) map[string]string {
	fields := make(map[string]string)
	for i := 0; i < len(input); {
		for i < len(input) && input[i] == ' ' {
			i++
		}
		if i >= len(input) {
			break
		}
		keyStart := i
		for i < len(input) && input[i] != '=' && input[i] != ' ' {
			i++
		}
		if i >= len(input) || input[i] != '=' {
			for i < len(input) && input[i] != ' ' {
				i++
			}
			continue
		}
		key := input[keyStart:i]
		i++
		if i >= len(input) {
			fields[key] = ""
			break
		}
		if input[i] == '"' {
			valueStart := i
			i++
			escaped := false
			for i < len(input) {
				if escaped {
					escaped = false
					i++
					continue
				}
				if input[i] == '\\' {
					escaped = true
					i++
					continue
				}
				if input[i] == '"' {
					i++
					break
				}
				i++
			}
			raw := input[valueStart:i]
			unquoted, err := strconv.Unquote(raw)
			if err != nil {
				fields[key] = strings.Trim(raw, "\"")
			} else {
				fields[key] = unquoted
			}
			continue
		}
		valueStart := i
		for i < len(input) && input[i] != ' ' {
			i++
		}
		fields[key] = input[valueStart:i]
	}
	return fields
}

func shouldSuppressEvent(fields map[string]string) bool {
	event := fields["event"]
	if event == "" {
		return false
	}
	outcome := strings.ToLower(fields["outcome"])
	level := strings.ToLower(fields["level"])

	switch event {
	case "membership.manager.init", "transfer.manager.init", "replica.manager.init", "server.init":
		return true
	case "storage.migrate":
		return outcome == "started" || outcome == "succeeded"
	case "heartbeat.tick", "membership.peer.suspect.clear", "retry.wait.complete":
		return true
	case "retry.attempt":
		return outcome == "started"
	case "retry.complete":
		return outcome == "succeeded"
	}

	if strings.HasSuffix(event, ".init") && outcome == "succeeded" && level != "warn" && level != "error" {
		return true
	}

	return false
}

func buildMessage(fields map[string]string) string {
	event := fields["event"]
	outcome := fields["outcome"]

	switch event {
	case "node.start":
		return sentence("Starting on port %s", fallback(fields["listen_addr"], "unknown"))
	case "storage.open":
		if outcome == "started" {
			return sentence("Opening storage at %s", fallback(fields["db_path"], "the configured path"))
		}
		if outcome == "succeeded" {
			return sentence("Storage opened at %s", fallback(fields["db_path"], "the configured path"))
		}
	case "membership.ring.rebuild":
		return sentence("Rebuilding the ring with %s active entries", fallback(fields["active_entries"], "0"))
	case "heartbeat.ping":
		peer := fallback(fields["peer_id"], "unknown")
		if outcome == "started" {
			return sentence("Pinging peer %s", peer)
		}
		if outcome == "succeeded" {
			return sentence("Ping to peer %s succeeded", peer)
		}
		if outcome == "failed" {
			return sentence("Ping to peer %s failed", peer)
		}
	case "heartbeat.tick":
		return sentence("Heartbeat tick targeting %s peers", fallback(fields["targets"], "0"))
	case "membership.client.dial":
		peerAddr := fallback(fields["peer_addr"], "unknown")
		cache := fields["cache"]
		if cache != "" {
			return sentence("Dialing %s (%s cache)", peerAddr, cache)
		}
		return sentence("Dialing %s", peerAddr)
	case "rpc.data.write":
		if outcome == "started" {
			return sentence("Processing write for key %s", fallback(fields["key"], "(unknown)"))
		}
		if outcome == "succeeded" {
			return sentence("Write for key %s committed", fallback(fields["key"], "(unknown)"))
		}
	case "rpc.data.read":
		return sentence("Processing read for key %s", fallback(fields["key"], "(unknown)"))
	case "storage.record.upsert":
		return sentence("Upserting key %s on vnode %s", fallback(fields["key"], "(unknown)"), fallback(fields["vnode_id"], "(unknown)"))
	case "transfer.claim.snapshot":
		return sentence("Streaming snapshot for transfer %s", fallback(fields["transfer_id"], "(unknown)"))
	case "transfer.claim.complete", "transfer.complete":
		return sentence("Completing transfer %s", fallback(fields["transfer_id"], "(unknown)"))
	case "transfer.activate_range":
		return sentence("Activating vnode %s at position %s", fallback(fields["vnode_id"], fallback(fields["target_vnode_id"], "(unknown)")), fallback(fields["position"], "(unknown)"))
	case "rpc.transfer.vnode_status":
		return sentence("Received vnode status update for %s", fallback(fields["vnode_id"], fallback(fields["target_vnode_id"], "(unknown)")))
	case "node.grpc.listen":
		return sentence("gRPC server listening on %s", fallback(fields["listen_addr"], "unknown"))
	case "node.shutdown":
		if outcome == "started" {
			return "Shutdown initiated"
		}
		if outcome == "succeeded" {
			return "Shutdown complete"
		}
	}

	if errText := fields["err"]; errText != "" {
		return sentence("%s (%s)", genericEventSentence(event, outcome), errText)
	}
	return genericEventSentence(event, outcome)
}

func genericEventSentence(event, outcome string) string {
	name := humanizeEventName(event)
	if outcome == "" || outcome == "started" {
		return sentence("%s", name)
	}
	switch outcome {
	case "succeeded":
		return sentence("%s succeeded", name)
	case "failed":
		return sentence("%s failed", name)
	case "rejected":
		return sentence("%s was rejected", name)
	case "skipped":
		return sentence("%s was skipped", name)
	}
	return sentence("%s (%s)", name, outcome)
}

func buildRawMessage(raw string, isError bool) string {
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "monitor listening on") {
		return sentence("Monitor started: %s", raw)
	}
	if strings.Contains(lower, "attribute `version` is obsolete") {
		return "Compose warning: docker-compose version field is obsolete"
	}
	if isError {
		return sentence("stderr: %s", raw)
	}
	return raw
}

func humanizeEventName(event string) string {
	if event == "" {
		return "Event"
	}
	event = strings.ReplaceAll(event, ".", " ")
	event = strings.ReplaceAll(event, "_", " ")
	words := strings.Fields(event)
	if len(words) == 0 {
		return "Event"
	}
	for i := range words {
		w := strings.ToLower(words[i])
		switch w {
		case "tls", "grpc", "rpc", "db", "id":
			words[i] = strings.ToUpper(w)
		default:
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}

func sentence(format string, args ...any) string {
	msg := fmt.Sprintf(format, args...)
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return "(empty message)"
	}
	if strings.HasSuffix(msg, ".") || strings.HasSuffix(msg, "!") || strings.HasSuffix(msg, "?") {
		return msg
	}
	return msg + "."
}

func fallback(value, defaultValue string) string {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	return value
}

func eventKind(name string) string {
	switch {
	case strings.HasPrefix(name, "heartbeat") || strings.HasSuffix(name, ".ping"):
		return "ping"
	case strings.HasPrefix(name, "transfer"):
		return "transfer"
	case strings.HasPrefix(name, "rpc.data") || strings.HasPrefix(name, "storage.record"):
		return "data"
	default:
		return "mesh"
	}
}

func eventTime(event FeedEvent) time.Time {
	if event.Time.IsZero() {
		return time.Now().UTC()
	}
	return event.Time.UTC()
}

func appendTrimmedEvent(events []FeedEvent, event FeedEvent, limit int) []FeedEvent {
	events = append(events, event)
	if len(events) <= limit {
		return events
	}
	trimmed := make([]FeedEvent, limit)
	copy(trimmed, events[len(events)-limit:])
	return trimmed
}
