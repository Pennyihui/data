# -*- coding: utf-8 -*-
"""
alert_webhook.py — 每日任务失败告警 (企业微信/钉钉/通用 webhook)
=================================================================
用法:
  from data_foundation import alert_webhook
  alert_webhook.notify(results)

结果结构 (与 run_daily.main() 的 results 一致):
  {source: {"status": "ok|failed", "elapsed": float, "batches": int,
            "notes": [...], "detail": str}}

行为:
  * 环境变量 ALERT_WEBHOOK_URL 未设置 -> 仅打印日志 (log-only), 不发送;
  * URL 含 qyapi.weixin -> 企业微信格式 {"msgtype":"text","text":{"content":...}};
  * URL 含 oapi.dingtalk -> 钉钉格式   {"msgtype":"text","text":{"content":...}};
  * 其他 -> 通用 JSON POST (同样格式);
  * 统一经 netpath.post_json 发送 (四级链路兜底 + 重试)。
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from . import netpath

_ENV = "ALERT_WEBHOOK_URL"


def _summarize(results: dict) -> str:
    """把 results 汇总成一段纯文本告警内容。"""
    failed = [(k, v) for k, v in results.items() if v.get("status") == "failed"]
    ok = [k for k, v in results.items() if v.get("status") == "ok"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        "data_foundation 每日任务失败告警",
        f"时间: {now}",
        f"失败源 ({len(failed)}): " + (", ".join(k for k, _ in failed) if failed else "无"),
    ]
    for k, v in failed:
        lines.append(f"  - {k}: {v.get('detail') or '未知错误'}")
    if ok:
        lines.append(f"成功源 ({len(ok)}): {', '.join(sorted(ok))}")
    return "\n".join(lines)


def notify(results: dict) -> dict:
    """发送失败告警。返回 {"sent": bool, "reason": str, "content": str}。

    未设置 ALERT_WEBHOOK_URL 时仅打印 (log-only), sent=False。
    """
    url = os.environ.get(_ENV, "").strip()
    content = _summarize(results)

    if not url:
        print(f"[alert_webhook] 未设置环境变量 {_ENV}, 仅记录日志 (log-only):\n{content}")
        return {"sent": False, "reason": "no_webhook_url", "content": content}

    # 按 URL 域名决定消息格式 (企微/钉钉/通用三种格式体完全一致, 仅端点差异)
    payload = {"msgtype": "text", "text": {"content": content}}
    try:
        resp = netpath.post_json(url, json_body=payload)
        print(f"[alert_webhook] 已发送 -> {url}\n{content}\n响应: {str(resp)[:200]}")
        return {"sent": True, "reason": "ok", "content": content, "response": resp}
    except Exception as e:  # noqa: BLE001
        print(f"[alert_webhook] 发送失败 (已降级为日志): {str(e)[:150]}\n{content}")
        return {"sent": False, "reason": f"send_failed: {str(e)[:120]}",
                "content": content}


if __name__ == "__main__":
    # 告警 stub 自测: 无 env 时应 log-only 且不抛错
    r = notify({"demo_source": {"status": "failed", "detail": "RuntimeError: boom",
                                "elapsed": 1.2, "batches": 0, "notes": []},
                "other_source": {"status": "ok", "detail": "", "elapsed": 5.0,
                                 "batches": 3, "notes": []}})
    print("notify 返回:", r)
