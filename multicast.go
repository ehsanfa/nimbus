package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

func setupMarcoReceiver(serverAddress, udpAddress string) {
	addr, err := net.ResolveUDPAddr("udp", udpAddress)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for {
		buffer := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Fatal(err)
		}
		message := buffer[:n]
		if string(message) == "marco" {
			send(udpAddress, []byte(fmt.Sprintf("polo!%s", serverAddress)))
		} else {
			log.Printf("marco received unknown message", string(message))
		}
	}
}

func listenPoloReceiver(ctx context.Context, selfAddress, udpAddress string) (string, error) {
	addr, err := net.ResolveUDPAddr("udp", udpAddress)
	if err != nil {
		log.Fatal(err)
		return "", err
	}

	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		log.Fatal(err)
		return "", err
	}
	defer conn.Close()

	responders := make(chan string)
	go func() {
		for {
			buffer := make([]byte, 1024)
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Fatal(err)
			}
			message := buffer[:n]
			if string(message[:5]) == "polo!" {
				fmt.Println("POLOOOOO")
				serverAddress := string(message[5:])
				if serverAddress == selfAddress {
					continue
				}
				responders <- serverAddress
				return
			} else {
				log.Printf("polo received unknown message", string(message))
			}
		}
	}()

	select {
	case <-ctx.Done():
		return "", errors.New("context closed")
	case resp := <-responders:
		return resp, nil
	}
}

func marco(ctx context.Context, address string) {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			send(address, []byte("marco"))
		case <-ctx.Done():
			return
		}
	}

}

func send(address string, message []byte) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Write(message)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Sent message: %s", message)
}
