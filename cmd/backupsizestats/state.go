package main

import (
	"encoding/json"
	"fmt"
	"os"
)

const dist_resolution_kb = 64
const dist_resolution = dist_resolution_kb * 1024

type State struct {
	LastProcessedSeq uint64
	V2Distribution   map[int]int
	JsonDistribution map[int]int
}

func ReadStateOrEmpty(path string) (*State, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return &State{
				LastProcessedSeq: 0,
				V2Distribution:   make(map[int]int),
				JsonDistribution: make(map[int]int),
			}, nil
		}
		return nil, fmt.Errorf("unable to stat file '%s': %w", path, err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("unable to read file '%s': %w", path, err)
	}
	s := &State{}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, fmt.Errorf("unable to unmarshal state: %w", err)
	}
	return s, nil
}

func (s *State) Save(path string) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("unable to marshal state: %w", err)
	}
	return WriteFileAtomically(path, data)
}

func (s *State) AcknowledgeSize(v2size int, jsonSize int) {
	s.V2Distribution[getSizeBucket(v2size)] += 1
	s.JsonDistribution[getSizeBucket(jsonSize)] += 1
}

func getSizeBucket(size int) int {
	n := size / dist_resolution
	if size%dist_resolution > 0 {
		n += 1
	}
	return n * dist_resolution_kb
}
