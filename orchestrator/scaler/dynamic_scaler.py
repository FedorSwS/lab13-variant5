import asyncio
import logging
import subprocess
import os
import signal
import time
from typing import Dict, List
import aiohttp
from nats.aio.client import Client as NATS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dynamic-scaler")

class DynamicScaler:
    def __init__(self, queue_name: str = "tasks.collect", min_agents: int = 1, max_agents: int = 5):
        self.queue_name = queue_name
        self.min_agents = min_agents
        self.max_agents = max_agents
        self.current_agents: Dict[str, subprocess.Popen] = {}
        self.nats_client = NATS()
        
    async def connect(self):
        await self.nats_client.connect("nats://localhost:4222")
        logger.info("Dynamic Scaler connected to NATS")
        
    async def get_queue_depth(self) -> int:
        """Get NATS queue depth via HTTP monitoring endpoint"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:8222/streaming/channelsz") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        total_msgs = 0
                        for channel in data.get("channels", []):
                            total_msgs += channel.get("msgs", 0)
                        return total_msgs
        except Exception as e:
            logger.warning(f"Failed to get queue depth: {e}")
        
        # Fallback: random simulation
        import random
        return random.randint(0, 50)
    
    def calculate_target_agents(self, queue_depth: int) -> int:
        """Calculate desired number of agents based on queue depth"""
        if queue_depth < 10:
            return self.min_agents
        elif queue_depth < 30:
            return 2
        elif queue_depth < 60:
            return 3
        elif queue_depth < 100:
            return 4
        else:
            return self.max_agents
    
    async def scale_up(self, count: int):
        """Start new agent instances"""
        for i in range(count):
            agent_id = f"collector_{int(time.time())}_{i}"
            
            # Start collector agent as subprocess
            collector_path = os.path.join(os.path.dirname(__file__), "../../agent_go/collector/main.go")
            proc = subprocess.Popen(
                ["go", "run", collector_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
            self.current_agents[agent_id] = proc
            logger.info(f"✅ SCALED UP: started {agent_id} (total: {len(self.current_agents)})")
            
    async def scale_down(self, count: int):
        """Stop excess agent instances"""
        agents_to_stop = list(self.current_agents.keys())[:count]
        for agent_id in agents_to_stop:
            proc = self.current_agents[agent_id]
            try:
                if os.name != 'nt':
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                else:
                    proc.terminate()
                proc.wait(timeout=5)
            except Exception as e:
                logger.warning(f"Failed to stop {agent_id}: {e}")
            finally:
                del self.current_agents[agent_id]
                logger.info(f"✅ SCALED DOWN: stopped {agent_id} (total: {len(self.current_agents)})")
    
    async def scale_loop(self):
        """Main scaling loop"""
        while True:
            queue_depth = await self.get_queue_depth()
            target_count = self.calculate_target_agents(queue_depth)
            current_count = len(self.current_agents)
            
            if target_count > current_count:
                logger.info(f"📊 Queue depth: {queue_depth}, Scaling UP from {current_count} to {target_count}")
                await self.scale_up(target_count - current_count)
            elif target_count < current_count:
                logger.info(f"📊 Queue depth: {queue_depth}, Scaling DOWN from {current_count} to {target_count}")
                await self.scale_down(current_count - target_count)
            else:
                logger.debug(f"📊 Queue depth: {queue_depth}, Current agents: {current_count} (optimal)")
                
            await asyncio.sleep(15)  # Check every 15 seconds

async def main():
    scaler = DynamicScaler(min_agents=1, max_agents=5)
    await scaler.connect()
    logger.info("Starting dynamic scaling loop...")
    await scaler.scale_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Dynamic scaler stopped")
