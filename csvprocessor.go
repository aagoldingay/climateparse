package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Station models data from the corresponding CSV data
type Station struct {
	WBAN                       string // some blanks
	WMO                        string // some blanks
	CallSign                   string // some blanks
	ClimateDivisionCode        string // some blanks
	ClimateDivisionStateCode   string // some blanks
	ClimateDivisionStationCode string // some blanks
	Name                       string // some blanks
	State                      string // some blanks
	Location                   string
	Latitude                   float64
	Longitude                  float64
	GroundHeight               int
	StationHeight              string // some blanks
	Barometer                  string // some blanks
	TimeZone                   string
}

// Precip models data from the corresponding CSV data
type Precip struct {
	WBAN          string //0
	StationID     string
	TimeDate      time.Time //yeardaymonth = 1, hour = 2
	Precipitation float64   //3
}

// Hourly models data from the corresponding CSV data
type Hourly struct {
	WBAN                string //0
	StationID           string
	Date                string //1
	Time                string //2
	StationType         string //3
	SkyCondition        string //4
	Visibility          string //6
	WeatherType         string //8
	DryBulbFarenheit    string //10
	DryBulbCelsius      string //12
	WetBulbFarenheit    string //14
	WetBulbCelsius      string //16
	DewPointFarenheit   string //18
	DewPointCelsius     string //20
	RelativeHumidity    string //22
	WindSpeed           string //24
	WindDirection       string //26
	ValueForWind        string //28
	StationPressure     string //30
	PressureTendency    string //32
	PressureChange      string //34
	SeaLevelPressure    string //36
	RecordType          string //38
	HourlyPrecipitation string //40
	Altimeter           string //42
}

// Daily models data from the corresponding CSV data
type Daily struct {
	WBAN         string //0
	StationID    string
	YearMonthDay string //1
	Tmax         string //2
	Tmin         string //4
	Tavg         string //6
	Depart       string //8
	DewPoint     string //10
	WetBulb      string //12
	Heat         string //14
	Cool         string //16
	Sunrise      string //18
	Sunset       string //20
	CodeSum      string //22
	Depth        string //24
	Water1       string //26
	SnowFall     string //28
	PrecipTotal  string //30
	StnPressure  string //32
	SeaLevel     string //34
	ResultSpeed  string //36
	ResultDir    string //38
	AvgSpeed     string //40
	Max5Speed    string //42
	Max5Dir      string //44
	Max2Speed    string //46
	Max2Dir      string //48
}

func processStationsCSV(path, id string) ([]interface{}, []string) {
	file, _ := os.Open(fmt.Sprintf("%s/%sstation.csv", path, id))
	reader := csv.NewReader(bufio.NewReader(file))
	var stations []interface{}
	wbans := []string{}
	firstLine := true
	for {
		line, err := reader.Read()
		if firstLine {
			firstLine = !firstLine
			continue
		}
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		if line[0] == "" { // not concerned if there is no WBAN identifier
			continue
		}

		lat, err := strconv.ParseFloat(strings.Trim(line[9], " "), 64)
		if err != nil {
			log.Fatal(err, strings.Trim(line[9], " "))
		}
		lon, err := strconv.ParseFloat(strings.Trim(line[10], " "), 64)
		if err != nil {
			log.Fatal(err, strings.Trim(line[10], " "))
		}
		gr, err := strconv.Atoi(strings.Trim(line[11], " "))
		if err != nil {
			log.Fatal(err, strings.Trim(line[11], " "))
		}
		stations = append(stations, Station{
			WBAN:                       strings.Trim(strings.TrimLeft(line[0], "0"), " "),
			WMO:                        strings.Trim(line[1], " "),
			CallSign:                   strings.Trim(line[2], " "),
			ClimateDivisionCode:        strings.Trim(line[3], " "),
			ClimateDivisionStateCode:   strings.Trim(line[4], " "),
			ClimateDivisionStationCode: strings.Trim(line[5], " "),
			Name:                       strings.Trim(line[6], " "),
			State:                      strings.Trim(line[7], " "),
			Location:                   strings.Trim(line[8], " "),
			Latitude:                   lat,
			Longitude:                  lon,
			GroundHeight:               gr,
			StationHeight:              strings.Trim(line[12], " "),
			Barometer:                  strings.Trim(line[13], " "),
			TimeZone:                   strings.Trim(line[14], " "),
		})
		wbans = append(wbans, strings.Trim(strings.TrimLeft(line[0], "0"), " "))
	}
	return stations, wbans
}

//daily
//hourly

func processPrecipCSV(path, id string, stationIDs map[string]string) []interface{} {
	file, _ := os.Open(fmt.Sprintf("%s/%sprecip.csv", path, id))
	reader := csv.NewReader(bufio.NewReader(file))
	var precips []interface{}
	firstLine := true
	for {
		line, err := reader.Read()
		if firstLine {
			firstLine = !firstLine
			continue
		}
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		if line[0] == "" { // not concerned if there is no WBAN identifier
			continue
		}

		h, err := strconv.Atoi(strings.Trim(line[2], " "))
		if err != nil {
			log.Fatal(err, strings.Trim(line[9], " "))
		}
		prec, err := strconv.ParseFloat(strings.Trim(line[3], " "), 64)
		if err != nil {
			prec = 0.0
		}
		td, _ := time.Parse("20060102 15", fmt.Sprintf("%s %v", line[1], h-1))

		// map
		id, prs := stationIDs[strings.Trim(strings.TrimLeft(line[0], "0"), " ")]
		if !prs {
			log.Printf("entry not with a station: %v\n", strings.Trim(strings.TrimLeft(line[0], "0"), " "))
			continue
		}
		precips = append(precips, Precip{
			WBAN:          strings.Trim(strings.TrimLeft(line[0], "0"), " "),
			StationID:     id,
			TimeDate:      td,
			Precipitation: prec,
		})
	}
	return precips
}
