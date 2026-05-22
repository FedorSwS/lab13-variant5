import asyncio
import json
import uuid
import logging
from typing import Dict, List
from collections import defaultdict

import nats
from nats.aio.msg import Msg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("auction-manager")

class AuctionManager:
    def __init__(self, auction_timeout: int = 5):
        self.nc = None
        self.auction_timeout = auction_timeout
        self.pending_auctions: Dict[str, asyncio.Future] = {}
        self.bids: Dict[str, List[dict]] = defaultdict(list)
        
    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        await self.nc.subscribe("auction.bids", cb=self.on_bid)
        await self.nc.subscribe("auction.result", cb=self.on_result)
        logger.info("Auction Manager connected to NATS")
        
    async def on_bid(self, msg: Msg):
        bid = json.loads(msg.data.decode())
        task_id = bid.get("task_id")
        if task_id and task_id in self.pending_auctions:
            self.bids[task_id].append(bid)
            logger.info(f"📥 Received bid for {task_id}: agent={bid['agent_id']} score={bid['score']:.2f}")
    
    async def on_result(self, msg: Msg):
        result = json.loads(msg.data.decode())
        logger.info(f"📋 Task result: {result}")
    
    async def run_auction(self, task: dict) -> dict:
        """Run auction for a task and return winner"""
        task_id = str(uuid.uuid4())
        
        auction_task = {
            "id": task_id,
            "type": task.get("type", "unknown"),
            "required_skill": task.get("required_skill", 50),
            "max_price": task.get("max_price", 100)
        }
        
        auction_future = asyncio.Future()
        self.pending_auctions[task_id] = auction_future
        
        # Publish auction request
        await self.nc.publish("auction.request", json.dumps(auction_task).encode())
        logger.info(f"🔨 Auction started for task {task_id} (skill required: {auction_task['required_skill']})")
        
        # Wait for bids
        await asyncio.sleep(self.auction_timeout)
        
        # Select winner
        bids = self.bids.get(task_id, [])
        
        if not bids:
            logger.warning(f"⚠️ No bids for task {task_id}")
            del self.pending_auctions[task_id]
            return {
                "task_id": task_id,
                "status": "failed",
                "error": "no_bids"
            }
        
        # Winner with highest score (skill/price)
        winner = max(bids, key=lambda b: b["score"])
        
        # Notify winner
        winner_msg = {
            "task_id": task_id,
            "agent_id": winner["agent_id"],
            "bid": winner
        }
        await self.nc.publish("auction.winner", json.dumps(winner_msg).encode())
        
        logger.info(f"🏆 Auction completed: winner={winner['agent_id']} for task {task_id} (score={winner['score']:.2f})")
        
        # Cleanup
        del self.pending_auctions[task_id]
        del self.bids[task_id]
        
        return {
            "task_id": task_id,
            "status": "completed",
            "winner": winner["agent_id"],
            "price": winner["price"],
            "skill": winner["skill"],
            "score": winner["score"],
            "total_bids": len(bids)
        }

async def main():
    manager = AuctionManager()
    await manager.connect()
    
    # Test auction
    test_task = {
        "type": "collect",
        "required_skill": 60,
        "max_price": 80
    }
    
    logger.info("Running test auction...")
    result = await manager.run_auction(test_task)
    logger.info(f"Auction result: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Auction manager stopped")
