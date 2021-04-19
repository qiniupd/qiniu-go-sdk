package input_file

import (
	"encoding/csv"
	"errors"
	"os"
)

func ParseKeys(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	all, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(all))
	for _, splited := range all {
		if len(splited) == 0 {
			return nil, errors.New("empty key exists")
		}
		keys = append(keys, splited[0])
	}
	return keys, nil
}

type Pair struct {
	Left  string
	Right string
}

func ParsePairs(path string) ([]Pair, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	all, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}
	keys := make([]Pair, 0, len(all))
	for _, splited := range all {
		if len(splited) < 2 {
			return nil, errors.New("invalid pair exists")
		}
		keys = append(keys, Pair{Left: splited[0], Right: splited[1]})
	}
	return keys, nil
}
