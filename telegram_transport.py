"""Telegram HTTP with bounded timeouts and credential-safe errors."""
import json
import secrets
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class TelegramDeliveryError(Exception):
    pass


def telegram_request(token, method, payload, timeout=5):
    request = Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            result = json.load(response)
        if result.get("ok") is not True:
            raise TelegramDeliveryError("Telegram rejected the request")
        return result.get("result")
    except HTTPError as error:
        raise TelegramDeliveryError(f"Telegram HTTP {error.code}") from None
    except TelegramDeliveryError:
        raise
    except Exception:
        raise TelegramDeliveryError("Telegram transport unavailable") from None


def send_html(token, chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup is not None:
        payload["reply_markup"] = markup
    return telegram_request(token, "sendMessage", payload)


def send_photo(token, chat_id, image, caption):
    """Upload a generated PNG in memory; never log token URLs or response text."""
    boundary = "telegram-card-" + secrets.token_hex(16)
    parts = []
    for name, value in (("chat_id", chat_id), ("caption", caption), ("parse_mode", "HTML")):
        parts.append(
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n"
            f"{value}\r\n".encode())
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"photo\"; "
        "filename=\"signal.png\"\r\nContent-Type: image/png\r\n\r\n".encode()
        + image + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    request = Request(
        f"https://api.telegram.org/bot{token}/sendPhoto", data=b"".join(parts),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"}, method="POST")
    try:
        with urlopen(request, timeout=5) as response:
            result = json.load(response)
        if result.get("ok") is not True:
            raise TelegramDeliveryError("Telegram rejected the signal card")
        return result.get("result")
    except HTTPError as error:
        raise TelegramDeliveryError(f"Telegram HTTP {error.code}") from None
    except TelegramDeliveryError:
        raise
    except Exception:
        raise TelegramDeliveryError("Telegram photo transport unavailable") from None
