package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	scanner := Scanner{}

	metaData := os.WriteFile("./metadata", make([]byte, 0), 0644)
	if metaData != nil {
		panic(metaData)
	}

	for {
		err := scanner.Scan()
		if err != nil {
			panic(err)
		}

		time.Sleep(time.Second * 5)
	}

}

type Scanner struct {
}

func (s *Scanner) Scan() error {

	files, err := os.ReadDir("./input")

	if err != nil {
		return err
	}

	for _, file := range files {

		// tsString := file.Name()[len("ts_log_"):]
		// timeStamp, err := time.ParseDuration(tsString + "s")

		// if err != nil {
		// 	return err
		// }

		// if int(timeStamp.Seconds()) < time.Now().Second()-300000000 {
		// 	continue
		// }

		parser := LogParser{}
		err = parser.Parse("./input/" + file.Name())
		if err != nil {
			return err
		}

	}
	return nil

}

type LogParser struct {
}

func (p *LogParser) Parse(fileName string) error {

	fmt.Println("Parsing file %s", fileName)

	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	logData := []LogEntry{}

	metaData, err := os.ReadFile("./metadata")
	if err != nil {
		return err
	}

	lastFile := ""
	lastLine := 0

	metaDataStr := string(metaData)
	metaDataContents := strings.SplitN(metaDataStr, "\n", 2)
	if len(metaDataContents) == 2 {
		lastFile = metaDataContents[0]
		lastLine, _ = strconv.Atoi(metaDataContents[1])
	}

	line := 0
	for scanner.Scan() {
		if line < 5 && lastFile == fileName && line > lastLine {
			data := scanner.Text()
			dataParts := strings.SplitN(data, "\t", 3)
			if len(dataParts) != 3 {
				continue
			}
			logData = append(logData, LogEntry{
				ID:   dataParts[1],
				Time: dataParts[0],
				Data: dataParts[2],
			})
		} else {
			PushLogData(logData)
			logData = []LogEntry{}
			os.WriteFile("./metadata", []byte(fmt.Sprintf(fileName+"\n"+strconv.Itoa(lastLine))), 0644)
			line = 0
		}
		line++
	}

	if line <= 5 && lastFile == fileName && line > lastLine {
		PushLogData(logData)
		logData = []LogEntry{}
		line = 0
	}

	return nil
}

func PushLogData(logData []LogEntry) error {

	for _, log := range logData {
		fmt.Printf("Pushing log with ID: %s, Time: %s, Data: %s\n", log.ID, log.Time, log.Data)
	}

	return nil
}

type LogEntry struct {
	ID   string `json:"id"`
	Time string `json:"time"`
	Data string `json:"data"`
}
