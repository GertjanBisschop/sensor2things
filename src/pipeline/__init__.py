"""Main entry point for SignalK to SensorThings conversion pipeline."""

import asyncio
import logging
import signal
from pathlib import Path

import click

# NOTE: import to ensure artifact registration
from pipeline import convertors, sink_connectors, source_connectors  # noqa: F401
from pipeline.models.config import Config, LogLevel
from pipeline.orchestrator import Orchestrator


def setup_logging(log_level: LogLevel) -> None:
    """Configure structured console logging."""
    logging.basicConfig(
        level=log_level.as_logging_level(),
        format="%(asctime)s [%(threadName)s|%(name)s] %(levelname)-8s: %(message)s",
    )


async def run_pipeline(config: Config) -> None:
    """Run the decoupled pipeline."""
    setup_logging(config.log_level)
    log = logging.getLogger(__name__)
    log.info("===> Begin pipeline orchestration")

    loop = asyncio.get_running_loop()
    orchestrator = Orchestrator(config=config)
    orchestrator_task = asyncio.create_task(orchestrator.run())

    def _handle_signal(sig: signal.Signals) -> None:
        log.info(f"Received exit signal {sig.name}. Initiating graceful shutdown...")
        # Cancelling the main orchestrator task triggers its finally block
        orchestrator_task.cancel()

    # Register OS signal listeners
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    try:
        await orchestrator_task
    except KeyboardInterrupt, asyncio.CancelledError:
        log.info("Pipeline task cancelled successfully.")
    except Exception:
        log.exception("Unhandled error during pipeline run.")
    finally:
        # Guarantee stop is called even if task creation or startup failed
        if orchestrator.running:
            await orchestrator.stop()
        log.info("<=== Pipeline orchestration complete")


@click.command()
@click.option(
    "--config-path", "-c", type=Path, default="config.yaml", help="Path to the configuration file."
)
def pipeline(config_path: Path) -> None:
    """Load config and run the conversion pipeline."""
    config = Config.from_path(config_path)

    asyncio.run(run_pipeline(config))
