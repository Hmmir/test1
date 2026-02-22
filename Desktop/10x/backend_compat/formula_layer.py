import ast
import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_div(a: Any, b: Any, default: Any = 0.0) -> float:
    base = _to_float(b)
    if abs(base) < 1e-12:
        return _to_float(default)
    return _to_float(a) / base


def _clamp(value: Any, low: Any, high: Any) -> float:
    v = _to_float(value)
    lo = _to_float(low)
    hi = _to_float(high)
    if lo > hi:
        lo, hi = hi, lo
    return min(max(v, lo), hi)


def _coalesce(*args: Any) -> Any:
    for item in args:
        if item is None:
            continue
        if isinstance(item, str) and not item.strip():
            continue
        return item
    return None


@dataclass
class FormulaRule:
    field: str
    expr: str


class _SafeEval(ast.NodeVisitor):
    def __init__(self, names: Dict[str, Any], funcs: Dict[str, Callable[..., Any]]):
        self.names = names
        self.funcs = funcs

    def visit(self, node: ast.AST) -> Any:  # type: ignore[override]
        return super().visit(node)

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    def visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id in self.names:
            return self.names[node.id]
        if node.id in self.funcs:
            return self.funcs[node.id]
        return 0.0

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        if isinstance(node.op, ast.Add):
            return _to_float(left) + _to_float(right)
        if isinstance(node.op, ast.Sub):
            return _to_float(left) - _to_float(right)
        if isinstance(node.op, ast.Mult):
            return _to_float(left) * _to_float(right)
        if isinstance(node.op, ast.Div):
            return _safe_div(left, right, 0.0)
        if isinstance(node.op, ast.Pow):
            return _to_float(left) ** _to_float(right)
        raise ValueError(f"Unsupported operator: {type(node.op).__name__}")

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        val = self.visit(node.operand)
        if isinstance(node.op, ast.UAdd):
            return +_to_float(val)
        if isinstance(node.op, ast.USub):
            return -_to_float(val)
        if isinstance(node.op, ast.Not):
            return not bool(val)
        raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:
        result: Any = False
        if isinstance(node.op, ast.And):
            for item in node.values:
                result = self.visit(item)
                if not bool(result):
                    return result
            return result
        if isinstance(node.op, ast.Or):
            for item in node.values:
                result = self.visit(item)
                if bool(result):
                    return result
            return result
        raise ValueError(f"Unsupported bool operator: {type(node.op).__name__}")

    def visit_Compare(self, node: ast.Compare) -> Any:
        left = self.visit(node.left)
        for op, comp in zip(node.ops, node.comparators):
            right = self.visit(comp)
            if isinstance(op, ast.Eq):
                ok = left == right
            elif isinstance(op, ast.NotEq):
                ok = left != right
            elif isinstance(op, ast.Lt):
                ok = _to_float(left) < _to_float(right)
            elif isinstance(op, ast.LtE):
                ok = _to_float(left) <= _to_float(right)
            elif isinstance(op, ast.Gt):
                ok = _to_float(left) > _to_float(right)
            elif isinstance(op, ast.GtE):
                ok = _to_float(left) >= _to_float(right)
            else:
                raise ValueError(f"Unsupported compare operator: {type(op).__name__}")
            if not ok:
                return False
            left = right
        return True

    def visit_IfExp(self, node: ast.IfExp) -> Any:
        cond = self.visit(node.test)
        if bool(cond):
            return self.visit(node.body)
        return self.visit(node.orelse)

    def visit_Call(self, node: ast.Call) -> Any:
        func = self.visit(node.func)
        if not callable(func):
            raise ValueError("Only callable names are allowed")
        args = [self.visit(arg) for arg in node.args]
        return func(*args)

    def generic_visit(self, node: ast.AST) -> Any:
        raise ValueError(f"Unsupported AST node: {type(node).__name__}")


class FormulaLayer:
    def __init__(self, rules: List[FormulaRule]):
        self.rules = rules
        self._funcs: Dict[str, Callable[..., Any]] = {
            "safe_div": _safe_div,
            "clamp": _clamp,
            "coalesce": _coalesce,
            "max": max,
            "min": min,
            "abs": abs,
            "round": round,
            "float": _to_float,
            "int": lambda v: int(round(_to_float(v))),
        }

    @classmethod
    def from_file(cls, path: str) -> "FormulaLayer":
        try:
            with open(path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception:
            payload = {}
        raw_rules = payload.get("rules") if isinstance(payload, dict) else None
        rules: List[FormulaRule] = []
        if isinstance(raw_rules, list):
            for item in raw_rules:
                if not isinstance(item, dict):
                    continue
                field = str(item.get("field") or "").strip()
                expr = str(item.get("expr") or "").strip()
                if field and expr:
                    rules.append(FormulaRule(field=field, expr=expr))
        return cls(rules=rules)

    @classmethod
    def from_env(cls) -> "FormulaLayer":
        path = os.environ.get(
            "BTLZ_CHECKLIST_FORMULA_FILE",
            os.path.join(
                os.path.dirname(__file__), "data", "checklist_formula_rules.json"
            ),
        )
        return cls.from_file(path)

    def eval_expr(self, expr: str, names: Dict[str, Any]) -> Any:
        tree = ast.parse(expr, mode="eval")
        return _SafeEval(names=names, funcs=self._funcs).visit(tree)

    def apply(self, base_values: Dict[str, Any]) -> Dict[str, Any]:
        values: Dict[str, Any] = dict(base_values)
        for rule in self.rules:
            try:
                values[rule.field] = self.eval_expr(rule.expr, values)
            except Exception:
                continue
        return values


DEFAULT_CHECKLIST_OUTPUT_FIELDS = {
    "expected_buyouts_dyn",
    "expected_buyouts_count",
    "expected_buyouts_sum_rub",
    "avg_price_with_spp",
    "profit_without_adv",
    "profit_with_adv",
}


def apply_checklist_formula_layer(
    layer: FormulaLayer, base_values: Dict[str, Any]
) -> Dict[str, Any]:
    merged = layer.apply(base_values)
    out: Dict[str, Any] = {}
    for field in DEFAULT_CHECKLIST_OUTPUT_FIELDS:
        if field in merged:
            out[field] = merged[field]
    return out
