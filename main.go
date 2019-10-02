package main

import (
	"encoding/json"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/swd-commons/model"
	"github.com/ndphu/swd-commons/service"
	"github.com/ndphu/swd-commons/slack"
	"log"
	"notification-service/config"
	"os"
	"os/signal"
)

func main() {
	opts := service.NewClientOpts(config.Get().MQTTBroker)
	opts.OnConnect = func(client mqtt.Client) {
		log.Println("[MQTT]", "Connected to broker")
		client.Subscribe("/3ml/notifications/broadcast", 0, func(client mqtt.Client, message mqtt.Message) {
			n := model.Notification{}
			if err := json.Unmarshal(message.Payload(), &n); err != nil {
				log.Println("[MQTT]", "Fail to unmarshal message", string(message.Payload()))
				return
			}
			log.Println("[MQTT]", "Notification received", string(message.Payload()))
			go handleNotification(n)
		}).Wait()
		log.Println("[MQTT]", "Subscribed to notification topic")
	}
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		log.Panic("[MQTT]", "Fail to connect to message broker", tok.Error())
	}

	defer c.Disconnect(100)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	log.Println("Interrupt signal received. Exiting...")
	os.Exit(0)
}

func handleNotification(n model.Notification) {
	switch n.Type {
	case "SLACK":
		slack.SendMessageToUser(n.SlackUserId, "you got notification")
		break
	}
}
