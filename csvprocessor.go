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
	Date                time.Time //date = 1, time = 2
	StationType         string    //3
	SkyCondition        []string  //4
	Visibility          float64   //6
	WeatherType         []string  //8
	DryBulbFarenheit    int       //10
	DryBulbCelsius      float64   //12
	WetBulbFarenheit    int       //14
	WetBulbCelsius      float64   //16
	DewPointFarenheit   int       //18
	DewPointCelsius     float64   //20
	RelativeHumidity    int       //22
	WindSpeed           int       //24
	WindDirection       int       //26
	ValueForWind        int       //28
	StationPressure     float64   //30
	PressureTendency    string    //32
	PressureChange      string    //34
	SeaLevelPressure    float64   //36
	RecordType          string    //38
	HourlyPrecipitation float64   //40
	Altimeter           float64   //42
}

// Daily models data from the corresponding CSV data
type Daily struct {
	WBAN         string //0
	StationID    string
	YearMonthDay time.Time //1
	Tmax         int       //2
	Tmin         int       //4
	Tavg         int       //6
	DewPoint     int       //10
	WetBulb      int       //12
	Heat         int       //14
	Cool         int       //16
	CodeSum      []string  //22
	SnowFall     float64   //28
	PrecipTotal  float64   //30
	StnPressure  float64   //32
	SeaLevel     float64   //34
	ResultSpeed  float64   //36
	ResultDir    int       //38
	AvgSpeed     float64   //40
	Max5Speed    int       //42
	Max5Dir      int       //44
	Max2Speed    int       //46
	Max2Dir      int       //48
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

func processDailyCSV(path, id string, stationIDs map[string]string) []interface{} {
	file, _ := os.Open(fmt.Sprintf("%s/%sdaily.csv", path, id))
	reader := csv.NewReader(bufio.NewReader(file))
	var dailys []interface{}
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
		id, prs := stationIDs[strings.Trim(strings.TrimLeft(line[0], "0"), " ")]
		if !prs {
			log.Printf("entry not with a station: %v\n", strings.Trim(strings.TrimLeft(line[0], "0"), " "))
			continue
		}

		td, _ := time.Parse("20060102", line[1])

		dly := Daily{
			WBAN:         strings.Trim(strings.TrimLeft(line[0], "0"), " "),
			StationID:    id,
			YearMonthDay: td,
		}

		// some of these values show invalid, best not add them if possible
		tmax, err := strconv.Atoi(strings.Trim(line[2], " "))
		if err == nil {
			dly.Tmax = tmax
		}
		tmin, err := strconv.Atoi(strings.Trim(line[4], " "))
		if err == nil {
			dly.Tmin = tmin
		}
		tavg, err := strconv.Atoi(strings.Trim(line[6], " "))
		if err == nil {
			dly.Tavg = tavg
		}
		dp, err := strconv.Atoi(strings.Trim(line[10], " "))
		if err == nil {
			dly.DewPoint = dp
		}
		wb, err := strconv.Atoi(strings.Trim(line[12], " "))
		if err == nil {
			dly.WetBulb = wb
		}
		heat, err := strconv.Atoi(strings.Trim(line[14], " "))
		if err == nil {
			dly.Heat = heat
		}
		cool, err := strconv.Atoi(strings.Trim(line[16], " "))
		if err == nil {
			dly.Cool = cool
		}
		// split codesum
		sf, err := strconv.ParseFloat(strings.Trim(line[28], " "), 64)
		if err == nil {
			dly.SnowFall = sf
		}
		pt, err := strconv.ParseFloat(strings.Trim(line[30], " "), 64)
		if err == nil {
			dly.PrecipTotal = pt
		}
		sp, err := strconv.ParseFloat(strings.Trim(line[32], " "), 64)
		if err == nil {
			dly.StnPressure = sp
		}
		sl, err := strconv.ParseFloat(strings.Trim(line[34], " "), 64)
		if err == nil {
			dly.SeaLevel = sl
		}
		rs, err := strconv.ParseFloat(strings.Trim(line[36], " "), 64)
		if err == nil {
			dly.ResultSpeed = rs
		}
		rd, err := strconv.Atoi(strings.Trim(line[38], " "))
		if err == nil {
			dly.ResultDir = rd
		}
		as, err := strconv.ParseFloat(strings.Trim(line[40], " "), 64)
		if err == nil {
			dly.AvgSpeed = as
		}
		//
		m5s, err := strconv.Atoi(strings.Trim(line[42], " "))
		if err == nil {
			dly.Max5Speed = m5s
		}
		m5d, err := strconv.Atoi(strings.Trim(line[44], " "))
		if err == nil {
			dly.Max5Dir = m5d
		}
		m2s, err := strconv.Atoi(strings.Trim(line[46], " "))
		if err == nil {
			dly.ResultDir = m2s
		}
		m2d, err := strconv.Atoi(strings.Trim(line[48], " "))
		if err == nil {
			dly.ResultDir = m2d
		}
		dailys = append(dailys, dly)
	}
	return dailys
}

func processHourlyCSV(path, id string, stationIDs map[string]string) []interface{} {
	file, _ := os.Open(fmt.Sprintf("%s/%shourly.csv", path, id))
	reader := csv.NewReader(bufio.NewReader(file))
	var hourlys []interface{}
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

		id, prs := stationIDs[strings.Trim(strings.TrimLeft(line[0], "0"), " ")]
		if !prs {
			log.Printf("entry not with a station: %v\n", strings.Trim(strings.TrimLeft(line[0], "0"), " "))
			continue
		}

		td, _ := time.Parse("20060102 1504", fmt.Sprintf("%s %s", line[1], line[2]))
		hrly := Hourly{
			WBAN:             strings.Trim(strings.TrimLeft(line[0], "0"), " "),
			StationID:        id,
			Date:             td,
			StationType:      strings.Trim(line[3], " "),
			SkyCondition:     strings.Split(strings.Trim(line[4], " "), " "),
			WeatherType:      strings.Split(strings.Trim(line[8], " "), " "),
			PressureTendency: strings.Trim(line[32], " "),
			PressureChange:   strings.Trim(line[34], " "),
			RecordType:       strings.Trim(line[38], " "),
		}

		v, err := strconv.ParseFloat(strings.Trim(line[6], " "), 64)
		if err == nil {
			hrly.Visibility = v
		}
		dbc, err := strconv.ParseFloat(strings.Trim(line[12], " "), 64)
		if err == nil {
			hrly.DryBulbCelsius = dbc
		}
		wbc, err := strconv.ParseFloat(strings.Trim(line[16], " "), 64)
		if err == nil {
			hrly.WetBulbCelsius = wbc
		}
		dpc, err := strconv.ParseFloat(strings.Trim(line[20], " "), 64)
		if err == nil {
			hrly.DewPointCelsius = dpc
		}
		sp, err := strconv.ParseFloat(strings.Trim(line[30], " "), 64)
		if err == nil {
			hrly.StationPressure = sp
		}
		slp, err := strconv.ParseFloat(strings.Trim(line[36], " "), 64)
		if err == nil {
			hrly.SeaLevelPressure = slp
		}
		hp, err := strconv.ParseFloat(strings.Trim(line[40], " "), 64)
		if err == nil {
			hrly.HourlyPrecipitation = hp
		}
		a, err := strconv.ParseFloat(strings.Trim(line[42], " "), 64)
		if err == nil {
			hrly.Altimeter = a
		}
		dbf, err := strconv.Atoi(strings.Trim(line[10], " "))
		if err == nil {
			hrly.DryBulbFarenheit = dbf
		}
		wbf, err := strconv.Atoi(strings.Trim(line[14], " "))
		if err == nil {
			hrly.WetBulbFarenheit = wbf
		}
		dpf, err := strconv.Atoi(strings.Trim(line[18], " "))
		if err == nil {
			hrly.DewPointFarenheit = dpf
		}
		rh, err := strconv.Atoi(strings.Trim(line[22], " "))
		if err == nil {
			hrly.RelativeHumidity = rh
		}
		ws, err := strconv.Atoi(strings.Trim(line[24], " "))
		if err == nil {
			hrly.WindSpeed = ws
		}
		wd, err := strconv.Atoi(strings.Trim(line[26], " "))
		if err == nil {
			hrly.WindDirection = wd
		}
		vfw, err := strconv.Atoi(strings.Trim(line[28], " "))
		if err == nil {
			hrly.ValueForWind = vfw
		}

		hourlys = append(hourlys, hrly)
	}
	return hourlys
}
