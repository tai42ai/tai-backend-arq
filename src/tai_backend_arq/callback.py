"""Callback glue — chain a follow-up tool after a backend task runs.

:class:`CallbackSchema` completes the contract field shape
(:class:`tai_contract.backend.CallbackSchema`) with ``rendered_condition`` /
``rendered_expr`` methods that render the condition/expression fields through
the host's resource manager (``tai_app.storage.resource_manager``).

``callback_execution`` evaluates the rendered condition over a task result and,
when it passes (an empty condition always passes), renders the expression and
optionally runs a follow-up tool. It returns the follow-up tool's result, the
rendered expression output when no tool is set, or ``None`` when the condition
fails. ``prepare_backend_kwargs`` strips the FastMCP context from a tool's
kwargs and injects the tool name before a backend dispatch.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from tai_contract.app import tai_app
from tai_contract.backend import CallbackSchema as CallbackFields
from tai_kit.utils.data.jq_util import get_compiled_jq

from tai_backend_arq.signatures import exclude_fastmcp_ctx_from_kwargs


class CallbackSchema(CallbackFields):
    """The contract callback field shape plus the render methods.

    The contract carries only the field shape (``tool`` plus the condition/expr
    mixin fields); rendering reaches the live resource manager, which is why the
    methods live here rather than in the pure contract.
    """

    async def rendered_condition(self) -> str:
        return await tai_app.storage.resource_manager.render_by_id_or_content(
            content=self.condition,
            template_id=self.condition_id,
            kwargs=self.condition_kwargs,
        )

    async def rendered_expr(self) -> str:
        return await tai_app.storage.resource_manager.render_by_id_or_content(
            content=self.expr,
            template_id=self.expr_id,
            kwargs=self.expr_kwargs,
        )


async def prepare_backend_kwargs(
    func: Callable[..., Any], tool_name_arg: str, tool_name: str, kwargs: dict[str, Any]
) -> dict[str, Any]:
    """Strip the FastMCP context kwarg and inject the tool name for dispatch."""
    kwargs = exclude_fastmcp_ctx_from_kwargs(func, kwargs)
    kwargs[tool_name_arg] = tool_name
    return kwargs


async def callback_execution(result: Any, callback: CallbackSchema) -> Any:
    """Run ``callback`` over ``result``: gate on the condition, transform with
    the expression, then run the follow-up tool (when one is named)."""
    cond = await callback.rendered_condition()
    if cond:
        cond_output = get_compiled_jq(cond).input(result).first()
        if not cond_output:
            return None

    expr = await callback.rendered_expr()
    # An absent/empty expr is not an error — ``get_compiled_jq("")`` would raise,
    # so an empty expression yields an empty mapping for the follow-up tool.
    expr_output = get_compiled_jq(expr).input(result).first() if expr else {}

    if callback.tool:
        return await tai_app.tools.run_tool(callback.tool, expr_output)
    return expr_output
