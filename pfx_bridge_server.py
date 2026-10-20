from flask import Flask, request, jsonify
from threading import Lock
from collections import defaultdict, deque
import os
import time

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


def reject_webhook(reason, status=400, data=None):
    """Log a safe summary of rejected webhook payloads without logging secrets."""
    event_type = data.get("event", "poi_fvg_hit") if isinstance(data, dict) else None
    symbol = data.get("symbol") if isinstance(data, dict) else None
    keys = sorted(data.keys()) if isinstance(data, dict) else None
    print(
        f"[WEBHOOK_REJECT] reason={reason} event={event_type!r} "
        f"symbol={symbol!r} keys={keys}",
        flush=True,
    )
    return jsonify({"ok": False, "error": reason}), status


@app.get("/health")
def health():
    with LOCK:
        queue_sizes = {symbol: len(q) for symbol, q in QUEUES.items()}
    return jsonify({"ok": True, "queue_sizes": queue_sizes})


@app.post("/webhook")
def webhook():
    if not authorized(request):
        print("[WEBHOOK_REJECT] reason=unauthorized", flush=True)
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return reject_webhook("invalid_json")

    # Legacy payloads without an event field are treated as POI/FVG hit events.
    event_type = str(data.get("event", "poi_fvg_hit")).strip().lower()
    if event_type not in VALID_EVENTS:
        return reject_webhook("invalid_event", data=data)

    required = ["symbol", "signal", "time"]
    if event_type != "trend_state":
        required += ["type", "high", "low"]
    missing = [key for key in required if key not in data]
    if missing:
        print(
            f"[WEBHOOK_REJECT] reason=missing_fields event={event_type!r} "
            f"symbol={data.get('symbol')!r} missing={missing}",
            flush=True,
        )
        return jsonify({"ok": False, "error": "missing_fields", "missing": missing}), 400

    try:
        signal_value = float(data["signal"])
        if signal_value == 0:
            raise ValueError("signal must be non-zero")
        signal = 1 if signal_value > 0 else -1
        event_time = int(float(data["time"]))
        raw_symbol = str(data["symbol"])
        symbol = normalize_symbol(raw_symbol)

        if event_type == "trend_state":
            ztype, high, low, zone_id = 0, 0.0, 0.0, ""
        else:
            ztype = 2 if int(data["type"]) == 2 else 1
            high = float(data["high"])
            low = float(data["low"])
            # Read zone_id from the incoming JSON; never reference an undefined variable.
            zone_id = str(data.get("zone_id", "") or "").strip()
    except (ValueError, TypeError, OverflowError):
        return reject_webhook("bad_field_types", data=data)

    if not symbol or event_time <= 0:
        return reject_webhook("invalid_values", data=data)
    if event_type != "trend_state" and high <= low:
        return reject_webhook("invalid_zone_bounds", data=data)

    event_id = f"{symbol}:{event_type}:{event_time}:{signal}:{ztype}:{high}:{low}:{zone_id}"
    event = {
        "id": event_id,
        "event": event_type,
        "symbol": symbol,
        "signal": signal,
        "type": ztype,
        "high": high,
        "low": low,
        "time": event_time,
        "zone_id": zone_id,
        "received": int(time.time()),
    }

    with LOCK:
        q = QUEUES[symbol]
        duplicate = any(item["id"] == event["id"] for item in q)

        if not duplicate:
            if event_type == "trend_state":
                # Trend is state, not a trade signal: keep only the latest queued trend snapshot.
                retained = deque(item for item in q if item.get("event") != "trend_state")
                retained.appendleft(event)
                QUEUES[symbol] = retained
                q = QUEUES[symbol]
            else:
                q.append(event)

            while len(q) > MAX_QUEUE:
                removable = next(
                    (item for item in q if item.get("event") != "trend_state"),
                    None,
                )
                if removable is not None:
                    q.remove(removable)
                else:
                    q.pop()

    print(
        f"[WEBHOOK] event={event_type} raw={raw_symbol!r} "
        f"normalized={symbol!r} signal={signal} type={ztype} "
        f"high={high} low={low} time={event_time} zone_id={zone_id!r} "
        f"queued={not duplicate}",
        flush=True,
    )

    return jsonify(
        {
            "ok": True,
            "queued": not duplicate,
            "id": event["id"],
            "event": event_type,
            "symbol": symbol,
        }
    )


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
            print(
                f"[POLL] raw={raw_symbol!r} normalized={symbol!r} event_found=False",
                flush=True,
            )
            return jsonify({"ok": True, "signal": 0, "event": "none"})

        # NOTE: this protocol still removes the event before EA acknowledgement.
        # Durable storage and acknowledgement/retry are not implemented here.
        event = q.popleft()
        remaining = len(q)

    print(
        f"[POLL] event={event['event']} raw={raw_symbol!r} "
        f"normalized={symbol!r} event_found=True signal={event['signal']} "
        f"remaining={remaining}",
        flush=True,
    )
    return jsonify(event)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
