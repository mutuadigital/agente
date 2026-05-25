import logging
import threading
from flask import Flask, request, jsonify
from .whatsapp import extract_phone, extract_message_text
from . import conversation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.post("/webhook/whatsapp")
def webhook_whatsapp():
    payload = request.get_json(silent=True) or {}
    event = payload.get("event", "")

    if event != "messages.upsert":
        return jsonify({"ok": True})

    data = payload.get("data", {})
    key = data.get("key", {})

    # Ignore messages sent by the bot itself
    if key.get("fromMe"):
        return jsonify({"ok": True})

    remote_jid = key.get("remoteJid", "")

    # Ignore group chats, status broadcasts and newsletters
    if not remote_jid.endswith("@s.whatsapp.net"):
        return jsonify({"ok": True})

    phone = extract_phone(remote_jid)
    text = extract_message_text(data)

    if not phone or not text:
        return jsonify({"ok": True})

    logger.info("Mensagem recebida de %s: %s", phone, text[:80])

    # Process in background so webhook returns immediately (prevents retries)
    def _process():
        try:
            conversation.handle_message(phone, text)
        except Exception:
            logger.exception("Erro ao processar mensagem de %s", phone)

    threading.Thread(target=_process, daemon=True).start()
    return jsonify({"ok": True})


@app.post("/webhook/ticket-update")
def webhook_ticket_update():
    """
    Payload esperado do n8n:
    {
        "ticket_id": 42,
        "status": "em_andamento",
        "message": "Olá, estamos analisando seu chamado.",
        "sender": "suporte"
    }
    """
    payload = request.get_json(silent=True) or {}
    ticket_id = payload.get("ticket_id")
    status = payload.get("status", "em_andamento")
    message = payload.get("message", "")

    if not ticket_id or not message:
        return jsonify({"error": "ticket_id e message são obrigatórios"}), 400

    ticket = db.get_ticket(ticket_id)
    if not ticket:
        return jsonify({"error": "chamado não encontrado"}), 404

    db.add_ticket_message(ticket_id, "suporte", message)
    db.update_ticket_status(ticket_id, status)

    # Find the customer's phone to send notification
    with db.get_conn() as conn:
        user_row = conn.execute(
            "SELECT phone FROM users WHERE id = ?", (ticket["user_id"],)
        ).fetchone()

    if user_row:
        conversation.notify_ticket_update(user_row["phone"], ticket_id, message, status)

    logger.info("Ticket #%s atualizado para '%s'", ticket_id, status)
    return jsonify({"ok": True, "ticket_id": ticket_id})
