package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	discoveryJsonPath := flag.String("discovery-json-path", "", "path to discovery.json")
	portNum := flag.Int("port", 0, "port number to listen at")
	flag.Parse()

	if *discoveryJsonPath == "" {
		log.Fatalf("Please specify -discovery-json-path")
	}
	if *portNum == 0 {
		log.Fatalf("Please specify -port")
	}

	var err error
	discoveryJson, err := ioutil.ReadFile(*discoveryJsonPath)
	if err != nil {
		panic(err)
	}
	myUrl := fmt.Sprintf("http://localhost:%d", *portNum)
	discoveryJson = bytes.Replace(discoveryJson,
		[]byte("https://www.googleapis.com"), []byte(myUrl), -1)

	listenAndServe(discoveryJson, *portNum)
}

func listenAndServe(discoveryJson []byte, portNum int) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		route(w, r, discoveryJson)
	})

	log.Printf("Listening on :%d...", portNum)
	err := http.ListenAndServe(fmt.Sprintf(":%d", portNum), nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
