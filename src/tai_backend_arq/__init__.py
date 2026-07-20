"""arq execution backend for the TAI ecosystem.

Importing this package registers everything through the global ``tai_app``
handle as a side-effect (there is no entry-point): the :class:`ArqBackend`
(``@tai_app.backends.register_backend``), the ``backend_*`` tool surface, the
``sync_task`` / ``schedule_task`` / ``async_task`` BACKEND-kind tool
extensions, and the shutdown hook closing the shared ArqRedis pool. The host
names this package in its manifest's ``backend_module`` field and imports it at
startup. This package never imports the host skeleton — only ``tai_contract``,
``tai_kit``, and the arq SDK.
"""

from tai_backend_arq import extensions, lifecycle, tools
from tai_backend_arq.backend import ArqBackend
from tai_backend_arq.settings import TaskFailedError

__all__ = ["ArqBackend", "TaskFailedError", "extensions", "lifecycle", "tools"]
