# -*- coding: utf-8 -*-
"""alert_webhook 格式检测临时测试 (monkeypatch post_json, 不发真实请求)。"""
import os

from data_foundation import alert_webhook, netpath

captured = []


def fake_post(url, json_body=None, **kw):
    captured.append((url, json_body))
    return {"errcode": 0, "errmsg": "ok"}


netpath.post_json = fake_post
res = {"src_a": {"status": "failed", "detail": "RuntimeError: boom",
                 "elapsed": 9.0, "batches": 0, "notes": []},
       "src_b": {"status": "ok", "elapsed": 3.0, "batches": 2, "notes": []}}

cases = [("wecom", "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"),
         ("dingtalk", "https://oapi.dingtalk.com/robot/send?access_token=xxx"),
         ("generic", "https://hooks.slack.com/services/xxx")]
for name, url in cases:
    os.environ["ALERT_WEBHOOK_URL"] = url
    captured.clear()
    r = alert_webhook.notify(res)
    assert captured, "应发出请求"
    payload = captured[0][1]
    assert payload["msgtype"] == "text"
    assert payload["text"]["content"].startswith("data_foundation")
    print(f"[{name}] sent={r['sent']} payload_type={payload['msgtype']} "
          f"host={captured[0][0].split('/')[2]}")
os.environ.pop("ALERT_WEBHOOK_URL", None)
print("格式检测 OK")
