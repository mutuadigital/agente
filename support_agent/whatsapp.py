import os
import requests
import logging

logger = logging.getLogger(__name__)


def _base_url():
    return os.getenv("EVOLUTION_API_URL", "").rstrip("/")


def _headers():
    return {
        "apikey": os.getenv("EVOLUTION_API_KEY", ""),
        "Content-Type": "application/json",
    }


def _instance():
    return os.getenv("EVOLUTION_INSTANCE", "")


def send_text(phone, text):
    """Send a plain text message via Evolution API."""
    url = f"{_base_url()}/message/sendText/{_instance()}"
    payload = {"number": phone, "text": text}
    try:
        resp = requests.post(url, json=payload, headers=_headers(), timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        logger.error("Falha ao enviar mensagem WhatsApp para %s: %s", phone, exc)


def extract_phone(remote_jid: str) -> str:
    """'5511999999999@s.whatsapp.net' → '5511999999999'"""
    return remote_jid.split("@")[0]


def extract_message_text(data: dict) -> str | None:
    """Extract plain text from Evolution API message payload."""
    msg = data.get("message", {})
    return (
        msg.get("conversation")
        or msg.get("extendedTextMessage", {}).get("text")
        or None
    )
