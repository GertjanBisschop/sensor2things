import asyncio

import pytest

from pipeline.error import ArtifactRegistrationError
from pipeline.models import BaseConvertor, BaseSinkConnector, BaseSourceConnector
from pipeline.orchestrator import Orchestrator
from pipeline.registry import CONVERTORS, SINKS, SOURCES, register_pipeline_artifact
from pipeline.utils import classproperty


class DummySource(BaseSourceConnector):
    async def run(self) -> None:
        await self._queue.put("item")

    async def stop(self) -> None:
        pass

    async def health_check(self) -> bool:
        return True


class DummySink(BaseSinkConnector):
    async def run(self) -> None:
        _ = await self._queue.get()

    async def stop(self) -> None:
        pass

    async def health_check(self) -> bool:
        return True


class DummyConvertor(BaseConvertor):
    async def run(self) -> None:
        item = await self._source_queue.get()
        await self._sink_queue.put(f"converted_{item}")

    async def stop(self) -> None:
        pass

    async def health_check(self) -> bool:
        return True


def test_cannot_instantiate_abstract_base_classes():
    with pytest.raises(TypeError):
        BaseSourceConnector(queue=asyncio.Queue())  # type: ignore

    with pytest.raises(TypeError):
        BaseSinkConnector(queue=asyncio.Queue())  # type: ignore

    with pytest.raises(TypeError):
        BaseConvertor(source_queue=asyncio.Queue(), sink_queue=asyncio.Queue())  # type: ignore


@pytest.mark.asyncio
async def test_dummy_implementations_satisfy_contracts():
    src_q = asyncio.Queue()
    sink_q = asyncio.Queue()

    source = DummySource(queue=src_q)
    sink = DummySink(queue=sink_q)
    convertor = DummyConvertor(source_queue=src_q, sink_queue=sink_q)

    assert await source.health_check() is True
    assert await sink.health_check() is True
    assert await convertor.health_check() is True

    await source.run()
    assert src_q.qsize() == 1

    await convertor.run()
    assert sink_q.qsize() == 1

    await sink.run()
    assert sink_q.qsize() == 0


def test_registry_registration_and_orchestrator_variables():
    @register_pipeline_artifact
    class TestCustomConvertor(BaseConvertor):
        @classproperty
        def name(cls) -> str:
            return "test_custom_convertor"

        async def run(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def health_check(self) -> bool:
            return True

    @register_pipeline_artifact
    class TestCustomSource(BaseSourceConnector):
        @classproperty
        def name(cls) -> str:
            return "test_custom_source"

        async def run(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def health_check(self) -> bool:
            return True

    @register_pipeline_artifact
    class TestCustomSink(BaseSinkConnector):
        @classproperty
        def name(cls) -> str:
            return "test_custom_sink"

        async def run(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def health_check(self) -> bool:
            return True

    assert CONVERTORS["test_custom_convertor"] is TestCustomConvertor
    assert Orchestrator._convertors["test_custom_convertor"] is TestCustomConvertor

    assert SOURCES["test_custom_source"] is TestCustomSource
    assert Orchestrator._sources["test_custom_source"] is TestCustomSource

    assert SINKS["test_custom_sink"] is TestCustomSink
    assert Orchestrator._sinks["test_custom_sink"] is TestCustomSink


def test_registry_invalid_artifact():
    class InvalidArtifact:
        def __init__(self):
            pass

    with pytest.raises(ArtifactRegistrationError):
        register_pipeline_artifact(InvalidArtifact)  # type: ignore
