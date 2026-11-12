class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(PipelineError):
    """An error occurred while loading configuration"""


class ArtifactNotFoundError(PipelineError):
    """Raised when an artifact is not found."""


class ArtifactRegistrationError(PipelineError):
    """Raised when an artifact can't be registered."""


class OperationNotSupportedError(PipelineError):
    """Raised when an operation is not supported."""


class SourceConnectorError(PipelineError):
    """Raised when an error occurs in a source connector."""


class SinkConnectorError(PipelineError):
    """Raised when an error occurs in a sink connector."""


class ConvertorError(PipelineError):
    """Raised when an error occurs in a convertor."""
