import asyncio
import json
import logging
from typing import Optional

import aiohttp
import nats
from nats.aio.msg import Msg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("llm-agent")

class LLMAgent:
    def __init__(self, ollama_url: str = "http://localhost:11434", model: str = "llama2"):
        self.ollama_url = ollama_url
        self.model = model
        self.nc = None
        
    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        await self.nc.subscribe("tasks.llm_analyze", cb=self.analyze)
        logger.info(f"🤖 LLM Agent connected, using model: {self.model}")
        
    async def call_llm(self, prompt: str) -> Optional[str]:
        """Call Ollama API for LLM inference"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={"model": self.model, "prompt": prompt, "stream": False}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("response", "")
                    else:
                        logger.error(f"Ollama API error: {resp.status}")
                        return None
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return None
            
    async def analyze(self, msg: Msg):
        task = json.loads(msg.data.decode())
        task_id = task.get("id", "unknown")
        logger.info(f"🧠 LLM Agent analyzing task: {task_id}")
        
        # Create prompt based on task type
        payload = task.get("payload", {})
        
        if task.get("type") == "log_analysis":
            log_data = payload.get("log", "No log data")
            prompt = f"""Analyze this IT infrastructure log and return JSON with:
- severity (INFO/WARNING/ERROR/CRITICAL)
- root_cause (brief explanation)
- recommended_action

Log: {log_data}

Return only valid JSON."""
            
        elif task.get("type") == "anomaly_detection":
            metric = payload.get("metric", "unknown")
            value = payload.get("value", 0)
            threshold = payload.get("threshold", 80)
            
            prompt = f"""Analyze this anomaly in IT infrastructure:
Metric: {metric}
Current Value: {value}
Threshold: {threshold}

Answer:
1. Is this a real anomaly or false positive?
2. What could be the root cause?
3. What action should be taken?

Return as JSON."""
        else:
            prompt = f"Analyze this IT monitoring data and provide insights: {json.dumps(payload)}"
            
        response = await self.call_llm(prompt)
        
        result = {
            "task_id": task_id,
            "success": response is not None,
            "output": response if response else "LLM analysis failed - check Ollama service",
            "model": self.model,
            "timestamp": asyncio.get_event_loop().time()
        }
        
        await self.nc.publish("tasks.llm_analyze.completed", json.dumps(result).encode())
        
        if response:
            logger.info(f"✅ LLM analysis completed for task {task_id}")
            logger.debug(f"Response preview: {response[:200]}...")
        else:
            logger.error(f"❌ LLM analysis failed for task {task_id}")

async def main():
    # Check if Ollama is available
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:11434/api/tags") as resp:
                if resp.status != 200:
                    logger.warning("Ollama service not responding. Please run: docker-compose up -d ollama")
                    logger.warning("Then pull a model: docker exec -it ollama ollama pull llama2")
    except:
        logger.warning("Cannot connect to Ollama. Make sure it's running.")
    
    agent = LLMAgent()
    await agent.connect()
    logger.info("LLM Agent running, waiting for tasks...")
    await asyncio.Future()  # Run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("LLM Agent stopped")
