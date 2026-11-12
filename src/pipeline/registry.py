"""Artifact registry for convertors, source connectors, and sink connectors."""

from typing import TYPE_CHECKING

from pydantic import ConfigDict, validate_call

from pipeline.error import ArtifactRegistrationError

if TYPE_CHECKING:
    from pipeline.models import BaseConvertor, BaseSinkConnector, BaseSourceConnector

CONVERTORS: dict[str, type[BaseConvertor]] = {}
SOURCES: dict[str, type[BaseSourceConnector]] = {}
SINKS: dict[str, type[BaseSinkConnector]] = {}


def register_pipeline_artifact[T: type](artifact: T) -> T:
    """Register the pipeline artifact with the global artifact registry."""
    from pipeline.models import BaseConvertor, BaseSinkConnector, BaseSourceConnector

    artifact.__init__ = validate_call(config=ConfigDict(arbitrary_types_allowed=True))(
        artifact.__init__
    )

    if issubclass(artifact, BaseConvertor):
        CONVERTORS[artifact.name] = artifact
    elif issubclass(artifact, BaseSourceConnector):
        SOURCES[artifact.name] = artifact
    elif issubclass(artifact, BaseSinkConnector):
        SINKS[artifact.name] = artifact
    else:
        raise ArtifactRegistrationError(f"Artifact {artifact} can't be registered.")

    return artifact
