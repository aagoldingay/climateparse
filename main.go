package main

import "os"

func getFileFromArguments() string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}
	return "test201712"
}

func splitFilePath(path string) string {
	return path[len(path)-6:]
}

//daily
//hourly
//monthly
//precip
//remarks
//station

func main() {
	//pathToFile := getFileFromArguments()
	//d := splitFilePath(pathToFile)
}
