package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
)

type AuctionBid struct {
	AgentID   string  `json:"agent_id"`
	TaskID    string  `json:"task_id"`
	Price     float64 `json:"price"`
	Skill     int     `json:"skill"`
	Available bool    `json:"available"`
	Score     float64 `json:"score"`
}

type AuctionTask struct {
	ID           string  `json:"id"`
	Type         string  `json:"type"`
	RequiredSkill int    `json:"required_skill"`
	MaxPrice     float64 `json:"max_price"`
}

type AuctionWinner struct {
	TaskID  string     `json:"task_id"`
	AgentID string     `json:"agent_id"`
	Bid     AuctionBid `json:"bid"`
}

var agentID = "auction_agent_" + randString(6)

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func calculateScore(skill int, price float64) float64 {
	return float64(skill) / price
}

func main() {
	rand.Seed(time.Now().UnixNano())
	
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal("Failed to connect to NATS:", err)
	}
	defer nc.Close()

	log.Printf("Auction Agent %s started", agentID)

	// Subscribe to auction requests
	_, err = nc.Subscribe("auction.request", func(msg *nats.Msg) {
		var task AuctionTask
		if err := json.Unmarshal(msg.Data, &task); err != nil {
			log.Printf("Failed to unmarshal auction task: %v", err)
			return
		}

		// Agent evaluates its capability
		skill := rand.Intn(100) + 1
		price := float64(rand.Intn(80) + 20)

		log.Printf("Agent %s evaluating task %s: skill=%d (need %d), price=%.2f (max %.2f)",
			agentID, task.ID, skill, task.RequiredSkill, price, task.MaxPrice)

		if skill >= task.RequiredSkill && price <= task.MaxPrice {
			bid := AuctionBid{
				AgentID:   agentID,
				TaskID:    task.ID,
				Price:     price,
				Skill:     skill,
				Available: true,
				Score:     calculateScore(skill, price),
			}
			bidData, _ := json.Marshal(bid)
			nc.Publish("auction.bids", bidData)
			log.Printf("💰 Agent %s placed bid for task %s: price=%.2f skill=%d score=%.2f",
				agentID, task.ID, price, skill, bid.Score)
		} else {
			log.Printf("❌ Agent %s cannot bid for task %s (skill or price too high)", agentID, task.ID)
		}
	})
	if err != nil {
		log.Fatal(err)
	}

	// Subscribe to winner notification
	_, err = nc.Subscribe("auction.winner", func(msg *nats.Msg) {
		var winner AuctionWinner
		if err := json.Unmarshal(msg.Data, &winner); err != nil {
			log.Printf("Failed to unmarshal winner: %v", err)
			return
		}
		if winner.AgentID == agentID {
			log.Printf("🏆 Agent %s WON auction for task %s! Processing...", agentID, winner.TaskID)
			// Process the task
			result := map[string]interface{}{
				"task_id": winner.TaskID,
				"status":  "processing",
				"agent":   agentID,
				"price":   winner.Bid.Price,
			}
			resultData, _ := json.Marshal(result)
			nc.Publish("auction.result", resultData)
		}
	})

	select {}
}
