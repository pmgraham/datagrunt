"""Shared deprecation notice for the AI/LLM surface (issue #144).

The AI/LLM features are deprecated in 3.3.0 and will be removed in 4.0.0. This
module is the single source of the deprecation message so every deprecated
entry point warns with identical wording.
"""

import warnings

AI_DEPRECATION_MESSAGE = (
    "datagrunt's AI/LLM features are deprecated and will be removed in datagrunt "
    "4.0.0. See https://github.com/pmgraham/datagrunt/issues/144."
)


def warn_ai_deprecated(name, stacklevel=3):
    """Emit a ``DeprecationWarning`` for a deprecated AI/LLM class.

    Args:
        name (str): The deprecated class name, named in the warning.
        stacklevel (int): Stack frames to skip so the warning points at the
            caller's construction site. Defaults to 3 for the
            ``caller -> __init__ -> warn_ai_deprecated`` call chain.
    """
    warnings.warn(
        f"{name} is deprecated: {AI_DEPRECATION_MESSAGE}",
        DeprecationWarning,
        stacklevel=stacklevel,
    )
