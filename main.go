package main

import (
	"encoding/json"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/globalsign/mgo/bson"
	"github.com/hako/durafmt"
	"github.com/ndphu/swd-commons/model"
	"github.com/ndphu/swd-commons/service"
	"github.com/ndphu/swd-commons/slack"
	"log"
	"notification-service/config"
	"notification-service/db"
	"os"
	"os/signal"
	"time"
)

func main() {
	opts := service.NewClientOpts(config.Get().MQTTBroker)
	opts.OnConnect = func(client mqtt.Client) {
		log.Println("[MQTT]", "Connected to broker")
		client.Subscribe(model.TopicNotificationBroadcast, 0, func(client mqtt.Client, message mqtt.Message) {
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
	sc := model.SlackConfig{}
	err := dao.Collection("slack_config").Find(bson.M{"userId": n.UserId}).One(&sc)
	if err != nil {
		log.Println("[NOTIFICATION]", "Fail to send notification by error", err.Error())
		return
	}
	switch n.Type {
	case model.NotificationTypeSittingRemind:
		color := model.NotificationSeverityWarning
		if n.SitDuration.Minutes()-float64(n.Rule.IntervalMinutes) > 10 {
			color = model.NotificationSeverityDanger
		}
		slack.SendMessage(sc.SlackUserId, slack.Attachment{
			AuthorName: "Sitting Monitoring Bot",
			Color:      color,
			Title:      "You are sitting for too long time",
			Text:       durafmt.Parse(n.SitDuration.Round(time.Second)).String(),
			Footer:     "To protect your health, please consider to stand up and do some exercises.",
		})
		break
	case model.NotificationTypeDeviceStatusAlert:
		d := model.Device{}
		if err := dao.Collection("device").Find(bson.M{"deviceId": n.DeviceId}).One(&d); err != nil {
			log.Println("[NOTIFICATION]", "Device not found")
		}
		color := model.NotificationSeverityDanger
		slack.SendMessage(sc.SlackUserId, slack.Attachment{
			AuthorName: "Device Monitoring Bot",
			Color:      color,
			Title:      "Device is OFFLINE",
			Text:       "Fail to contact to device " + d.Name,
			Footer:     "Please double check power cable or device configuration to resolve the issue",
		})
	}
}
