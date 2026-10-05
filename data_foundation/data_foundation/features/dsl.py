# -*- coding: utf-8 -*-
"""dsl.py — F3/F4 表达式引擎: 白名单 AST 求值 + CSE + 血缘 + PIT 传播

选型结论 (实测见 _dsl_probe.py, 结论写入设计文档 v0.5 第 12.3 节)
------------------------------------------------------------------
expr_codegen (0.16.6) **能**消费我们的算子命名空间, 生成可读的、分阶段的、带
CSE 的代码 (决策 6 的"可读代码 ⇒ 血缘可解析"成立), 但它的 pandas 执行模型把
asset/date 当作**列**并自行 groupby 逐片调用算子, 而本项目的算子是**面板模型**
(按索引层自行分组) —— 两套分组模型互斥; 且 PIT 可用时间传播必须知道每个节点的
窗口语义 (expr_codegen 执行时不暴露), 所以无论如何都要自带一次 AST 遍历。

因此引擎定为 **Python 标准库 ast.parse + 严格白名单求值**。决策 1 担心的
"自研 parser 的 bug"在此不成立: 解析交给标准库, 我们只写白名单与遍历, 并用
与参考实现逐点对照的测试兜底 (_test_dsl.py)。

引擎职责 (每个节点同时产出值与 data_available_at)
------------------------------------------------
* 值: 白名单算子 + 面板列 + 常量 + + - * / / 一元负号。
* 可用时间随值同行传播 (设计文档 7.1), 且**传播窗口必须与算子的取数窗口一致**
  (见 _PIT_RULES —— 漏一条就等于凭空放大可用时间, 或误报泄漏)。
* 血缘: 每节点记 (id, 源码, 算子, 参数, 直接父节点) —— DAG 存直接父节点
  (决策 9: O(N) 存储, 完整链路用时遍历)。
* CSE: 结构相同的子表达式只算一次 (按规范化源码 memo, 值与可用时间共享)。

安全: 可用表达式是数据不是代码 —— 只放行白名单算子/列名/常量与四则运算;
下标、属性、比较、布尔、lambda、未知调用一律拒绝。
"""
from __future__ import annotations

import ast
from dataclasses import dataclass, field as dc_field
from typing import Any

import pandas as pd

from . import operators as op
from ..fields import assert_no_leakage, propagate_availability

__all__ = [
    "Node", "CompiledExpr", "FeatureResult", "ExprError",
    "compile_expr", "evaluate", "expr_inputs", "expr_operators",
]


class ExprError(ValueError):
    """表达式非法 (未注册算子/字段、越权语法、窗口不合法)。"""


# ---------------------------------------------------------------------------
# 节点
# ---------------------------------------------------------------------------
@dataclass
class Node:
    nid: str
    kind: str                       # field | neg | binop | call
    code: str                       # 规范化源码 (审计/血缘)
    op_name: str | None = None
    binop: str | None = None        # add | sub | mul | div
    args: list = dc_field(default_factory=list)     # 嵌套 Node 或常量
    parents: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {"nid": self.nid, "kind": self.kind, "code": self.code,
                "op_name": self.op_name, "binop": self.binop,
                "parents": list(self.parents)}


@dataclass
class CompiledExpr:
    expr: str
    root: Node
    fields: tuple[str, ...]         # 直接依赖的字段列
    operators: tuple[str, ...]      # 求值顺序上的算子链

    def steps(self) -> list[Node]:
        """后序 (叶子 -> 根) 的计算节点列表 —— CSE 后的唯一节点。"""
        out, seen = [], set()

        def walk(n: Node):
            if n.nid in seen:
                return
            seen.add(n.nid)
            for a in n.args:
                if isinstance(a, Node):
                    walk(a)
            out.append(n)

        walk(self.root)
        return out

    def lineage(self) -> list[str]:
        chain, cur, seen = [], self.root, set()
        while cur is not None and cur.nid not in seen:
            seen.add(cur.nid)
            chain.append(cur.code)
            nxt = [a for a in cur.args if isinstance(a, Node)]
            if not nxt:
                cur = None
            else:
                # 血缘取第一个数据父节点 (字段/嵌套算子)
                cur = nxt[0]
        return list(reversed(chain))

    def depth(self) -> int:
        memo: dict[str, int] = {}

        def d(n: Node) -> int:
            if n.nid in memo:
                return memo[n.nid]
            memo[n.nid] = 1 + max([d(a) for a in n.args if isinstance(a, Node)],
                                  default=0)
            return memo[n.nid]

        return d(self.root)


@dataclass
class FeatureResult:
    """特征计算结果: 值 + 可用时间 + 血缘 + 输入最大可用时间。"""

    name: str
    values: pd.Series
    avail: pd.Series
    input_avail: pd.Series
    expr: str
    compiled: CompiledExpr

    def lineage(self) -> list[str]:
        return self.compiled.lineage()

    def inputs(self) -> tuple[str, ...]:
        return self.compiled.fields


# ---------------------------------------------------------------------------
# 编译: 白名单校验 + CSE
# ---------------------------------------------------------------------------
_ALLOWED_BINOPS = {"Add": "+", "Sub": "-", "Mult": "*", "Div": "/"}


def compile_expr(expr: str, known_fields: set[str] | None = None) -> CompiledExpr:
    """解析表达式 -> CSE 后的节点树 (根节点)。"""
    if not isinstance(expr, str) or not expr.strip():
        raise ExprError("表达式必须是非空字符串")
    try:
        tree = ast.parse(expr.strip(), mode="eval")
    except SyntaxError as exc:
        raise ExprError(f"表达式语法错误: {expr!r} ({exc})") from exc

    known = set(known_fields or set())
    memo: dict[str, Node] = {}
    counter = {"n": 0}
    field_names: list[str] = []
    op_names: list[str] = []

    def nid() -> str:
        counter["n"] += 1
        return f"n{counter['n']}"

    def build(node: ast.AST) -> Node:
        if isinstance(node, ast.Name):
            name = node.id
            if name in memo:
                return memo[name]
            if name not in known:
                raise ExprError(
                    f"未知字段/标签 {name!r}; 已知字段: {sorted(known)[:10]}"
                    f"{' ...' if len(known) > 10 else ''}")
            n = Node(nid(), "field", name)
            memo[name] = n
            field_names.append(name)
            return n
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float, str, bool)):
                return Node(nid(), "const", ast.unparse(node), args=[node.value])
            raise ExprError(f"不支持的常量: {ast.unparse(node)}")
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            child = build(node.operand)
            code = f"-({child.code})"
            if code in memo:
                return memo[code]
            n = Node(nid(), "neg", code, args=[child], parents=(child.nid,))
            memo[code] = n
            return n
        if isinstance(node, ast.BinOp):
            kind = type(node.op).__name__
            if kind not in _ALLOWED_BINOPS:
                raise ExprError(
                    f"不支持的运算符 {kind}; 只允许 + - * / "
                    f"(比较/布尔运算会引入样本内信息, 特征表达式里一律禁止)")
            left, right = build(node.left), build(node.right)
            sym = _ALLOWED_BINOPS[kind]
            code = f"({left.code} {sym} {right.code})"
            if code in memo:
                return memo[code]
            n = Node(nid(), "binop", code, binop=sym, args=[left, right],
                     parents=(left.nid, right.nid))
            memo[code] = n
            return n
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise ExprError("只允许直接函数调用 (算子名)")
            fname = node.func.id
            if fname not in op.ALL_OPERATORS:
                hint = ""
                typo = op.unknown_operators(fname)
                if typo:
                    hint = f"; 疑似算子拼错: {sorted(typo)}"
                raise ExprError(f"未知算子 {fname!r}{hint}")
            if node.keywords:
                raise ExprError(
                    f"算子 {fname} 只接受位置参数 (关键字参数会让血缘解析"
                    f"多一套路径)")
            if not node.args:
                raise ExprError(f"算子 {fname} 缺少参数")
            args: list = []
            for i, a in enumerate(node.args):
                if i == 0:                        # 数据参数: 列或嵌套表达式
                    args.append(build(a))
                elif isinstance(a, (ast.Name, ast.Call, ast.BinOp, ast.UnaryOp)):
                    args.append(build(a))          # 分组标签列 / 嵌套
                elif isinstance(a, ast.Constant) and isinstance(
                        a.value, (int, float, str, bool)):
                    args.append(a.value)           # 窗口/方法名等常量
                else:
                    raise ExprError(
                        f"算子 {fname} 的第 {i+1} 个参数不合法: "
                        f"{ast.unparse(a)}")
            code = f"{fname}({', '.join(ast.unparse(a) for a in node.args)})"
            if code in memo:
                return memo[code]
            parents = tuple(a.nid for a in args if isinstance(a, Node))
            n = Node(nid(), "call", code, op_name=fname, args=args,
                     parents=parents)
            memo[code] = n
            op_names.append(fname)
            return n
        raise ExprError(
            f"不支持的语法: {type(node).__name__} ({ast.unparse(node)})。"
            f"白名单: 字段 / 白名单算子 / 常量 / + - * / / 一元负号")

    root = build(tree.body)
    if root.kind == "const":
        raise ExprError("表达式不能只是常量")
    # 单字段表达式 = "raw 水平" 变体, 是合法特征 (设计文档 1.4 多口径: raw/zscore/
    # rank 都要有, 检验去决定哪个有效), 不再拒绝。
    return CompiledExpr(expr=expr.strip(), root=root,
                        fields=tuple(dict.fromkeys(field_names)),
                        operators=tuple(op_names))


def expr_inputs(expr: str, known_fields=None) -> tuple[str, ...]:
    return compile_expr(expr, known_fields).fields


def expr_operators(expr: str, known_fields=None) -> tuple[str, ...]:
    return compile_expr(expr, known_fields).operators


# ---------------------------------------------------------------------------
# PIT 传播规则: 传播窗口必须与算子的取数窗口一致
# ---------------------------------------------------------------------------
_TS_FIXED_WINDOW = {
    "ts_mean": 1, "ts_std": 1, "ts_sum": 1, "ts_min": 1, "ts_max": 1,
    "ts_median": 1, "ts_quantile": 1, "ts_rank": 1, "ts_zscore": 1,
    "ts_skew": 1, "ts_kurt": 1, "ts_decay_linear": 1, "ts_corr": 1,
}
_TS_LAG_OPS = {"ts_delay", "ts_delta", "ts_pct_change"}
_TS_UNBOUNDED = {"ts_backfill", "ts_ewma"}
_PP_HISTORY = {"pp_diff", "pp_pct_change"}        # 窗口 = lag+1
_PP_DIST = {"pp_zscore", "pp_minmax", "pp_robust", "pp_quantile_bucket"}


def _default_arg(fname: str, pos: int) -> Any:
    """算子第 pos 个参数的默认值 (表达式省略窗口时用它 —— 与算子行为一致,
    不另设一套默认值, 否则 PIT 传播窗口会与实际取数窗口错位)。"""
    import inspect
    params = list(inspect.signature(op.ALL_OPERATORS[fname].func).parameters.values())
    if pos < len(params) and params[pos].default is not inspect.Parameter.empty:
        return params[pos].default
    return 1


def _node_avail(node: Node, value_avail: pd.Series, group_label=None) -> pd.Series:
    """单数据节点输出的可用时间 —— 依据算子取数窗口传播。"""
    fname = node.op_name
    if fname is None:                        # field/neg/binop: 上游已传播好
        return value_avail
    fam = op.ALL_OPERATORS[fname].family

    def arg_i(pos: int):
        """第 pos 个实参里的 int (窗口类), 没有则用算子默认值。"""
        if len(node.args) > pos and isinstance(node.args[pos], int):
            return int(node.args[pos])
        return _default_arg(fname, pos)

    if fam == "cs":
        return propagate_availability("cs", value_avail)
    if fam == "group":
        return propagate_availability("group", value_avail, g=group_label)
    if fam == "ts":
        if fname in _TS_UNBOUNDED:
            return propagate_availability("ts_unbounded", value_avail)
        if fname in _TS_LAG_OPS:
            return propagate_availability("ts", value_avail, window=arg_i(1) + 1)
        return propagate_availability("ts", value_avail, window=arg_i(1))
    # pp_*
    if fname in _PP_HISTORY:
        return propagate_availability("ts", value_avail, window=arg_i(1) + 1)
    if fname == "pp_frac_diff":
        # (x, d, window=None, log=False) —— window 必须显式给 (算子强制)
        w = (node.args[2] if len(node.args) > 2 and isinstance(node.args[2], int)
             else (_default_arg(fname, 1) if isinstance(node.args[1], int)
                   else _default_arg(fname, 2)))
        return propagate_availability("ts", value_avail, window=int(w))
    if fname == "pp_detrend":
        return propagate_availability("ts", value_avail, window=arg_i(1))
    if fname == "pp_ema":
        return propagate_availability("ts_unbounded", value_avail)
    if fname in _PP_DIST:
        by, win = "time", None
        for a in node.args[1:]:
            if isinstance(a, str):
                by = a
            elif isinstance(a, int):
                win = a
        if by in ("ts", "trailing", "window"):
            return propagate_availability("ts", value_avail,
                                         window=int(win if win else _default_arg(fname, 2)))
        return propagate_availability("cs", value_avail)
    return propagate_availability("point", value_avail)


# ---------------------------------------------------------------------------
# 求值
# ---------------------------------------------------------------------------
def evaluate(compiled: CompiledExpr, values: pd.DataFrame,
             avail: pd.DataFrame, groups: dict[str, pd.Series] | None = None,
             name: str = "feature", group_cols: set[str] | None = None,
             check: bool = True) -> FeatureResult:
    """在面板上求值, 逐节点传播 data_available_at, 并做泄漏自检。

    values / avail : 面板 (MultiIndex base_asset/time) 与其可用时间面板
    groups         : 分组标签 name -> 与面板对齐的 Series (板块/市值层等)
    group_cols     : 面板里本身可当标签的列名 (如 sector 列)
    check          : 是否跑 assert_no_leakage (关掉只用于调试, 生产必须开)
    """
    groups = dict(groups or {})
    group_cols = set(group_cols or set())
    cache: dict[str, tuple[pd.Series, pd.Series]] = {}
    used_inputs: dict[str, pd.Series] = {}

    # ---- 交集网格: 特征只在**它全部输入都有数据**的行上计算 ----------------
    # 面板是多字段联合网格 (1h K 线 + 8h 资金费率 + …), 各字段采样频率不同。
    # 若在联合网格上直接算, 8h 字段的行之间夹着 7 个空行: ts_decay_linear(
    # funding_rate, 21) 会因"窗口里有空行"而全空, ts_rank(funding_rate, 90)
    # 的"90"也会变成 90 小时(=~11 次结算)而非 90 次结算 —— 窗口语义全错。
    # 标准做法 (qlib 等面板引擎): 特征按自身输入的交集网格计算, 再对齐回面板。
    full_index = values.index
    leaf_fields = [f for f in dict.fromkeys(compiled.fields) if f in values.columns]
    if leaf_fields:
        mask = values[leaf_fields].notna().all(axis=1)
        values = values.loc[mask]
        avail = avail.loc[mask]
    else:
        values = values.iloc[0:0]              # 无输入 -> 空算 (下面会报无血缘)

    def resolve_group(node_arg) -> pd.Series:
        if isinstance(node_arg, Node) and node_arg.kind == "field":
            name = node_arg.code
        elif isinstance(node_arg, str):
            name = node_arg
        else:
            raise ExprError(f"分组标签必须是列名, 收到 {node_arg!r}")
        if name in groups:
            g = groups[name]
            return g if g.index.equals(values.index) else g.reindex(values.index)
        if name in group_cols or name in values.columns:
            return values[name]
        raise ExprError(f"未知分组标签 {name!r}")

    def resolve_arg(a):
        if isinstance(a, Node):
            return ev(a)[0]          # 递归求值 (CSE 命中则复用缓存)
        return a

    def ev(n: Node) -> tuple[pd.Series, pd.Series]:
        if n.nid in cache:
            return cache[n.nid]
        if n.kind == "const":
            # 字面量没有"可用时间" (NaT); 与数据同式运算时同行取大即自动跳过
            v = pd.Series(n.args[0], index=values.index)
            av = pd.Series(pd.NaT, index=values.index,
                           dtype="datetime64[ns, UTC]")
            cache[n.nid] = (v, av)
            return cache[n.nid]
        if n.kind == "field":
            if n.code not in values.columns:
                raise ExprError(f"面板没有字段 {n.code!r}")
            v = values[n.code]
            av = avail[n.code].reindex(v.index)
            used_inputs[n.code] = av
            cache[n.nid] = (v, av)
            return cache[n.nid]
        if n.kind == "neg":
            v, av = ev(n.args[0])
            res = (-v, av)
            cache[n.nid] = res
            return res
        if n.kind == "binop":
            lv, la = ev(n.args[0])
            rv, ra = ev(n.args[1])
            fn = {"+": lambda a, b: a + b, "-": lambda a, b: a - b,
                  "*": lambda a, b: a * b, "/": lambda a, b: a / b}[n.binop]
            v = fn(lv, rv)
            av = pd.concat([la, ra], axis=1).max(axis=1)     # 点态: 同行取大
            cache[n.nid] = (v, av)
            return cache[n.nid]
        # call
        fname = n.op_name
        fam = op.ALL_OPERATORS[fname].family
        fn = op.ALL_OPERATORS[fname].func
        data_nodes = [a for a in n.args if isinstance(a, Node)]
        if fam == "group":
            label = resolve_group(n.args[1])
            call = [resolve_arg(n.args[0]), label] + \
                  [resolve_arg(a) for a in n.args[2:]]
            v = fn(*call)
            av = _node_avail(n, ev(data_nodes[0])[1], group_label=label)
        elif len(data_nodes) == 2:            # 双数据输入 (ts_corr)
            left_node, right_node = data_nodes[0], data_nodes[1]
            lv, la = ev(left_node)
            rv, ra = ev(right_node)
            rest = [a for a in n.args
                    if not (isinstance(a, Node) and a.nid in (left_node.nid, right_node.nid))]
            v = fn(lv, rv, *rest)
            # 每个数据参数各自的可用时间 (它自己可能已是嵌套传播的结果), 同行取大
            av = pd.concat([la, ra], axis=1).max(axis=1)
        else:
            v = fn(*[resolve_arg(a) for a in n.args])
            av = _node_avail(n, ev(data_nodes[0])[1])
        cache[n.nid] = (v, av)
        return cache[n.nid]

    v_out, av_out = ev(compiled.root)
    # 引擎不变量: **无可用时间处不产生值**。面板是多字段联合网格 (如只有
    # funding 的时刻没有 close 行), 若某特征在该刻的输入整体不可用, 算子仍会
    # 照常输出一个数 (cs_rank 会给出 1.0 之类), 那等于凭空造值 —— 置 NaN。
    # 注意不能用"输入该行是否缺失"来判: 窗口算子在该行输入缺失时用更早的 bar
    # 出值是合法的, 其可用时间由窗口传播给出 (非 NaT)。
    v_out = v_out.where(av_out.notna())
    if used_inputs:
        in_max = pd.DataFrame(used_inputs).max(axis=1)
    else:
        in_max = pd.Series(pd.NaT, index=v_out.index,
                           dtype="datetime64[ns, UTC]")
    in_max.name = "input_max_available_at"
    if check:
        assert_no_leakage(av_out, used_inputs, name=name, feature_values=v_out)
    # 对齐回完整面板索引 (交集网格之外没有值 -> NaN)
    if not v_out.index.equals(full_index):
        v_out = v_out.reindex(full_index)
        av_out = av_out.reindex(full_index)
        in_max = in_max.reindex(full_index)
    return FeatureResult(name=name, values=v_out, avail=av_out,
                         input_avail=in_max, expr=compiled.expr,
                         compiled=compiled)


if __name__ == "__main__":  # pragma: no cover
    print(__doc__.split("\n")[0])
    print("用法:")
    print('  c = compile_expr("group_rank(ts_zscore(pp_log(close),24), sector)",')
    print('                   known_fields={"close","sector"})')
    print('  r = evaluate(c, panel.values, panel.avail, group_cols={"sector"})')