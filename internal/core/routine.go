package core

import (
	"fmt"
	"strings"
	"time"
)

func ParseRoutineInputs(raw string) ([]RoutineInput, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "none") || raw == "-" {
		return nil, nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';'
	})
	routines := make([]RoutineInput, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Fields(part)
		if len(fields) != 3 || strings.ToLower(fields[1]) != "every" {
			return nil, fmt.Errorf("routine %q must look like: name every 1h", part)
		}
		duration, err := time.ParseDuration(fields[2])
		if err != nil {
			return nil, fmt.Errorf("routine %q has invalid duration: %w", part, err)
		}
		routines = append(routines, RoutineInput{Name: fields[0], Every: duration})
	}
	return routines, nil
}
