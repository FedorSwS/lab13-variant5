import pytest
import asyncio
from unittest.mock import AsyncMock, patch

pytest_plugins = ('pytest_asyncio',)

@pytest.mark.asyncio
async def test_send_task_timeout():
    from orchestrator import AgentOrchestrator

    orch = AgentOrchestrator()
    orch.nc = AsyncMock()
    orch.results = {}

    with patch('asyncio.wait_for', side_effect=asyncio.TimeoutError):
        with pytest.raises(TimeoutError):
            await orch.send_task("test", {"data": "test"}, timeout=1)

@pytest.mark.asyncio
async def test_on_result_sets_future():
    orch = AgentOrchestrator()
    orch.results = {}
    future = asyncio.Future()
    orch.results["task-123"] = future

    mock_msg = AsyncMock()
    mock_msg.data.decode.return_value = '{"task_id": "task-123", "success": true}'

    await orch.on_result(mock_msg)
    assert future.done()
    result = future.result()
    assert result["task_id"] == "task-123"
    assert result["success"] is True

@pytest.mark.asyncio
async def test_run_pipeline_success():
    orch = AgentOrchestrator()
    orch.nc = AsyncMock()
    orch.results = {}

    async def fake_send_task(task_type, payload, timeout=30, trace_id=None):
        if task_type == "collect":
            return {
                "success": True,
                "output": [
                    {"id": "m1", "service": "api-gateway", "value": 85, "metric_type": "cpu"},
                    {"id": "m2", "service": "auth-service", "value": 45, "metric_type": "cpu"}
                ]
            }
        elif task_type == "analyze":
            if payload["value"] >= payload["thresholds"]["critical"]:
                return {"output": {"alert_needed": True, "severity": "critical"}}
            return {"output": {"alert_needed": False}}
        elif task_type == "recover":
            return {"output": {"action_taken": "restart_service"}}
        return {"output": {}}

    with patch.object(orch, 'send_task', side_effect=fake_send_task):
        result = await orch.run_pipeline(
            services=["api-gateway", "auth-service"],
            thresholds={"api-gateway": {"warning": 70, "critical": 80}}
        )
        assert result["status"] == "success"
        assert result["metrics_collected"] == 2
