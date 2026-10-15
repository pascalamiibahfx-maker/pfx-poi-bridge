from flask import Flask, request, jsonify
from threading import Lock
from collections import defaultdict, deque
import os, time

app = Flask(__name__)
LOCK = Lock()
QUEUES = defaultdict(deque)

TOKEN = os.environ.get("PFX_BRIDGE_TOKEN", "CHANGE_ME_LONG_RANDOM_TOKEN")
MAX_QUEUE = int(os.environ.get("PFX_MAX_QUEUE", "100"))
VALID_EVENTS = {"trend_state", "poi_fvg_hit", "poi_fvg_invalidated"}


def normalize_symbol(value):
    return " ".join(str(value or "").strip().upper().split())


def authorized(req):
    supplied = req.headers.get("X-PFX-TOKEN", "") or req.args.get("token", "")
    return supplied == TOKEN


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.post("/webhook")
def webhook():
    if not authorized(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    # Backward compatibility: existing TradingView payloads are hit events.
    event_type = str(data.get("event", "poi_fvg_hit")).strip().lower()
    if event_type not in VALID_EVENTS:
        return jsonify({"ok": False, "error": "invalid_event", "allowed": sorted(VALID_EVENTS)}), 400

    required = ["symbol", "signal", "time"]
    if event_type != "trend_state":
        required += ["type", "high", "low"]
    if any(k not in data for k in required):
        return jsonify({"ok": False, "error": "missing_fields"}), 400

    try:
        signal_value = float(data["signal"])
        if signal_value == 0:
            raise ValueError("signal must be non-zero")
        signal = 1 if signal_value > 0 else -1
        event_time = int(float(data["time"]))
        raw_symbol = str(data["symbol"])
        symbol = normalize_symbol(raw_symbol)
        if event_type == "trend_state":
            ztype, high, low = 0, 0.0, 0.0
        else:
            ztype = 2 if int(data["type"]) == 2 else 1
            high = float(data["high"])
            low = float(data["low"])
    except (ValueError, TypeError, OverflowError):
        return jsonify({"ok": False, "error": "bad_field_types"}), 400

    if not symbol or event_time <= 0:
        return jsonify({"ok": False, "error": "invalid_values"}), 400
    if event_type != "trend_state" and (high <= low):
        return jsonify({"ok": False, "error": "invalid_zone_bounds"}), 400

    event_id = f"{symbol}:{event_type}:{event_time}:{signal}:{ztype}:{high}:{low}"
    event = {
        "id": event_id,
        "event": event_type,
        "symbol": symbol,
        "signal": signal,
        "type": ztype,
        "high": high,
        "low": low,
        "time": event_time,
        "received": int(time.time()),
    }

    with LOCK:
        q = QUEUES[symbol]
        duplicate = any(x["id"] == event["id"] for x in q)
        if not duplicate:
            if event_type == "trend_state":
                # Trend is state, not a trade signal: coalesce old snapshots and
                # put the newest state first so stale queued hits cannot override it.
                retained = deque(x for x in q if x.get("event") != "trend_state")
                retained.appendleft(event)
                QUEUES[symbol] = retained
                q = QUEUES[symbol]
            else:
                q.append(event)
            while len(q) > MAX_QUEUE:
                removable = next((item for item in q if item.get("event") != "trend_state"), None)
                if removable is not None:
                    q.remove(removable)
                else:
                    q.pop()

    print(f"[WEBHOOK] event={event_type} raw={raw_symbol!r} normalized={symbol!r} "
          f"signal={signal} type={ztype} high={high} low={low} "
          f"time={event_time} queued={not duplicate}", flush=True)

    return jsonify({"ok": True, "queued": not duplicate, "id": event["id"],
                    "event": event_type, "symbol": symbol})


@app.get("/poll")
def poll():
    if not authorized(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    raw_symbol = request.args.get("symbol", "")
    symbol = normalize_symbol(raw_symbol)
    if not symbol:
        return jsonify({"ok": False, "error": "missing_symbol"}), 400

    with LOCK:
        q = QUEUES.get(symbol)
        if not q:
            print(f"[POLL] raw={raw_symbol!r} normalized={symbol!r} event_found=False", flush=True)
            return jsonify({"ok": True, "signal": 0, "event": "none"})
        event = q.popleft()
        remaining = len(q)

    print(f"[POLL] event={event['event']} raw={raw_symbol!r} normalized={symbol!r} "
          f"event_found=True signal={event['signal']} remaining={remaining}", flush=True)
    return jsonify(event)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
