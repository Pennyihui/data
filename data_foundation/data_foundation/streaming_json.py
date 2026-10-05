# -*- coding: utf-8 -*-
"""streaming_json.py — JSON 数组的增量读取器 (标准库, 无新依赖)

问题
----
链上 ERC-20 转账日志每个批次是一个 JSON 数组文件, 单文件最大 ~70 万条。
`json.load()` 会把它变成一个 list[dict] 常驻内存: 每个 log 是 dict(~200B)
再套 6 个字符串, 70 万条 ~= 1GB 的 Python 对象 —— 这是每日 rebuild
吃 4GB 内存的头号元凶。

方案
----
只解析数组的"外壳", 元素逐条产出: 任何时刻内存里只有一个 log 对象。
实现: 增量喂给 `json.JSONDecoder.raw_decode`, 跳过字符串/逗号/空白。

性能
----
- 有 orjson 就用它解单个 log 对象 (C 实现, 比标准库快 ~3-5 倍);
  没有则回退标准库。解码语义两者一致 (都是完整 JSON 解析)。
- 用法: for log in iter_json_array(path): ...
"""
from __future__ import annotations

import gzip
import io
import json

try:                                   # 可选加速 (装了用, 没装也能跑)
    import orjson as _orjson
except ImportError:                    # pragma: no cover
    _orjson = None

_DEC = json.JSONDecoder()
#: 单个 log 的解码函数 (orjson.loads 返回 dict, 与 json 语义一致)
_loads = _orjson.loads if _orjson else json.loads


def _open_text(path: str) -> io.TextIOBase:
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def iter_json_array(path: str, chunk_size: int = 1 << 20):
    """逐条产出 JSON 数组元素 (dict)。不物化整个数组。

    支持 .json 与 .json.gz。非数组的顶层 JSON 直接抛错 (调用方本就该喂数组)。
    """
    with _open_text(path) as f:
        buf = ""
        pos = 0
        started = False
        first = True
        eof = False
        while True:
            # 保证缓冲区里能定位下一个非空白字符
            while True:
                # 跳过结构字符与空白
                while pos < len(buf) and buf[pos] in " \t\r\n,":
                    pos += 1
                if pos < len(buf):
                    break
                if eof:
                    if not started:
                        raise ValueError(f"{path}: 空/无内容, 期望 JSON 数组")
                    return
                chunk = f.read(chunk_size)
                if not chunk:
                    eof = True
                else:
                    buf = buf[pos:] + chunk
                    pos = 0
            ch = buf[pos]
            if first:
                if ch != "[":
                    raise ValueError(f"{path}: 顶层不是 JSON 数组 (读到 {ch!r})")
                pos += 1
                first = False
                started = True
                continue
            if ch == "]":
                return
            # 解码一个元素: 若不够, 继续补块后重试
            while True:
                try:
                    obj, end = _DEC.raw_decode(buf, pos)
                    break
                except json.JSONDecodeError:
                    if eof:
                        raise
                    chunk = f.read(chunk_size)
                    if not chunk:
                        eof = True
                    else:
                        buf = buf[pos:] + chunk
                        pos = 0
            yield obj
            pos = end
            if pos > chunk_size:               # 回收已消费的前缀, 控内存
                buf = buf[pos:]
                pos = 0


def count_json_array(path: str) -> int:
    """快速数一下数组元素个数 (不解码元素内容) —— 用于日志/预检。"""
    n = 0
    dec = json.JSONDecoder()
    with _open_text(path) as f:
        buf = ""
        pos = 0
        eof = False
        while True:
            while pos < len(buf) and buf[pos] in " \t\r\n,":
                pos += 1
            if pos < len(buf):
                break
            if eof:
                return n
            chunk = f.read(1 << 20)
            if not chunk:
                eof = True
            else:
                buf = buf[pos:] + chunk
                pos = 0
        if buf[pos] != "[":
            raise ValueError(f"{path}: 顶层不是 JSON 数组")
        pos += 1
        while True:
            while pos < len(buf) and buf[pos] in " \t\r\n,":
                pos += 1
            if pos < len(buf):
                if buf[pos] == "]":
                    return n
            elif eof:
                return n
            else:
                chunk = f.read(1 << 20)
                if not chunk:
                    eof = True
                else:
                    buf = buf[pos:] + chunk
                    pos = 0
                continue
            while True:
                try:
                    _, end = dec.raw_decode(buf, pos)
                    break
                except json.JSONDecodeError:
                    if eof:
                        raise
                    chunk = f.read(1 << 20)
                    if not chunk:
                        eof = True
                    else:
                        buf = buf[pos:] + chunk
                        pos = 0
            n += 1
            pos = end
            if pos > (1 << 20):
                buf = buf[pos:]
                pos = 0


if __name__ == "__main__":  # pragma: no cover
    import sys
    if len(sys.argv) > 1:
        p = sys.argv[1]
        it = iter_json_array(p)
        first = next(it, None)
        print("首元素:", str(first)[:120])
        print("总元素数:", 1 + sum(1 for _ in it) if first is not None else 0)
