from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional
import uuid

from orchestrator import AgentOrchestrator

app = FastAPI(title="Monitoring MAS API", version="1.0.0")

class MonitorRequest(BaseModel):
    services: List[str]
    thresholds: Optional[Dict[str, Dict[str, float]]] = None

class PipelineResponse(BaseModel):
    request_id: str
    trace_id: str
    metrics_collected: int
    alerts: List[dict]
    recoveries: List[dict]
    status: str

_orchestrator = None

async def get_orch():
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = AgentOrchestrator()
        await _orchestrator.connect()
    return _orchestrator

@app.post("/api/v1/monitor/run", response_model=PipelineResponse)
async def run_monitoring(request: MonitorRequest):
    orch = await get_orch()
    thresholds = request.thresholds or {}
    for svc in request.services:
        if svc not in thresholds:
            thresholds[svc] = {"warning": 70, "critical": 90}

    try:
        result = await orch.run_pipeline(request.services, thresholds)
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        return PipelineResponse(
            request_id=str(uuid.uuid4()),
            trace_id=result["trace_id"],
            metrics_collected=result["metrics_collected"],
            alerts=result["alerts"],
            recoveries=result["recoveries"],
            status=result["status"]
        )
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/health")
async def health():
    return {"status": "healthy"}
