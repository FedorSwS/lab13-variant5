import asyncio
import json
import uuid
import time
import logging
from typing import Dict, Optional, List

import nats
from nats.aio.msg import Msg
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import Status, StatusCode

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %name)s - %levelname)s - %message)s')
logger = logging.getLogger("orchestrator")

trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))


class AgentOrchestrator:
    def __init__(self):
        self.nc: Optional[nats.NATS] = None
        self.results: Dict[str, asyncio.Future] = {}
        self.subscriptions = []

    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        logger.info("Connected to NATS")

        for topic in ["tasks.collect.completed", "tasks.analyze.completed", 
                      "tasks.alert.completed", "tasks.recover.completed"]:
            sub = await self.nc.subscribe(topic, cb=self.on_result)
            self.subscriptions.append(sub)

    async def on_result(self, msg: Msg):
        result = json.loads(msg.data.decode())
        task_id = result.get("task_id")
        if task_id in self.results and not self.results[task_id].done():
            self.results[task_id].set_result(result)
            logger.info(f"Received result for task {task_id}")

    async def send_task(self, task_type: str, payload: dict, timeout: int = 30, trace_id: str = None) -> dict:
        task_id = str(uuid.uuid4())
        if trace_id is None:
            trace_id = str(uuid.uuid4())

        task = {
            "id": task_id,
            "type": task_type,
            "payload": json.dumps(payload),
            "timestamp": time.time(),
            "trace_id": trace_id
        }

        future = asyncio.Future()
        self.results[task_id] = future

        topic = f"tasks.{task_type}"
        await self.nc.publish(topic, json.dumps(task).encode())
        logger.info(f"Sent task {task_id} to {topic}")

        try:
            result = await asyncio.wait_for(future, timeout)
            return result
        except asyncio.TimeoutError:
            del self.results[task_id]
            raise TimeoutError(f"Task {task_id} timeout after {timeout}s")

    async def run_pipeline(self, services: List[str], thresholds: Dict[str, Dict[str, float]]) -> dict:
        with tracer.start_as_current_span("monitoring_pipeline") as span:
            trace_id = format(span.get_span_context().trace_id, '032x')
            span.set_attribute("pipeline.type", "monitoring")

            collect_payload = {"services": services}
            collect_result = await self.send_task("collect", collect_payload, trace_id=trace_id)

            if not collect_result["success"]:
                span.set_status(Status(StatusCode.ERROR))
                return {"error": "collection failed", "trace_id": trace_id}

            metrics = collect_result["output"]
            alerts_triggered = []
            recovery_results = []

            for metric in metrics:
                svc_thresholds = thresholds.get(metric["service"], {"warning": 70, "critical": 90})
                analyze_payload = {
                    "metric_id": metric["id"],
                    "service": metric["service"],
                    "value": metric["value"],
                    "thresholds": svc_thresholds
                }
                analyze_result = await self.send_task("analyze", analyze_payload, trace_id=trace_id)

                if analyze_result["output"].get("alert_needed", False):
                    alerts_triggered.append(analyze_result["output"])
                    if analyze_result["output"]["severity"] == "critical":
                        recover_payload = {
                            "service": metric["service"],
                            "issue": f"critical_metric_{metric['id']}",
                            "attempts": 1
                        }
                        recover_result = await self.send_task("recover", recover_payload, trace_id=trace_id)
                        recovery_results.append(recover_result)

            return {
                "trace_id": trace_id,
                "metrics_collected": len(metrics),
                "alerts": alerts_triggered,
                "recoveries": recovery_results,
                "status": "success"
            }


async def main():
    orch = AgentOrchestrator()
    await orch.connect()
    logger.info("Orchestrator ready")

    thresholds = {
        "api-gateway": {"warning": 75, "critical": 92},
        "auth-service": {"warning": 70, "critical": 88},
        "payment-service": {"warning": 65, "critical": 85}
    }

    result = await orch.run_pipeline(
        services=["api-gateway", "auth-service", "payment-service"],
        thresholds=thresholds
    )
    logger.info(f"Pipeline result: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(main())
