import os
import requests
import logging
from . import database as db
from .whatsapp import send_text

logger = logging.getLogger(__name__)

STATUS_LABELS = {
    "aberto": "🟡 Aberto",
    "em_andamento": "🔵 Em andamento",
    "resolvido": "🟢 Resolvido",
    "fechado": "⚫ Fechado",
}

MENU_UNAUTHENTICATED = (
    "👋 Olá! Bem-vindo ao suporte da *Mútua Digital*.\n\n"
    "Como deseja continuar?\n"
    "1️⃣ Fazer login\n"
    "2️⃣ Criar conta"
)

MENU_MAIN = (
    "📋 *Menu principal*\n\n"
    "1️⃣ Abrir chamado\n"
    "2️⃣ Meus chamados\n"
    "3️⃣ Sair"
)


def handle_message(phone: str, text: str):
    session = db.get_session(phone)
    state = session["state"]
    user_id = session["user_id"]
    data = session["data"]
    text = text.strip()

    # Route by state
    if state == "UNAUTHENTICATED":
        _handle_unauthenticated(phone, text, data)

    elif state == "AWAIT_LOGIN_USERNAME":
        _handle_await_login_username(phone, text, data)

    elif state == "AWAIT_LOGIN_PASSWORD":
        _handle_await_login_password(phone, text, data)

    elif state == "AWAIT_REG_NAME":
        _handle_await_reg_name(phone, text, data)

    elif state == "AWAIT_REG_EMAIL":
        _handle_await_reg_email(phone, text, data)

    elif state == "AWAIT_REG_USERNAME":
        _handle_await_reg_username(phone, text, data)

    elif state == "AWAIT_REG_PASSWORD":
        _handle_await_reg_password(phone, text, data)

    elif state == "MAIN_MENU":
        _handle_main_menu(phone, text, user_id, data)

    elif state == "AWAIT_TICKET_TITLE":
        _handle_await_ticket_title(phone, text, user_id, data)

    elif state == "AWAIT_TICKET_DESC":
        _handle_await_ticket_desc(phone, text, user_id, data)

    elif state == "VIEWING_TICKETS":
        _handle_viewing_tickets(phone, text, user_id, data)

    elif state == "VIEWING_TICKET":
        _handle_viewing_ticket(phone, text, user_id, data)

    else:
        db.save_session(phone, "UNAUTHENTICATED")
        send_text(phone, MENU_UNAUTHENTICATED)


# --- State handlers ---

def _handle_unauthenticated(phone, text, data):
    if text == "1":
        db.save_session(phone, "AWAIT_LOGIN_USERNAME")
        send_text(phone, "🔐 Por favor, informe seu *usuário*:")
    elif text == "2":
        db.save_session(phone, "AWAIT_REG_NAME")
        send_text(phone, "📝 Vamos criar sua conta!\n\nQual é o seu *nome completo*?")
    else:
        send_text(phone, MENU_UNAUTHENTICATED)


def _handle_await_login_username(phone, text, data):
    data["login_username"] = text
    db.save_session(phone, "AWAIT_LOGIN_PASSWORD", data=data)
    send_text(phone, "🔑 Agora informe sua *senha*:")


def _handle_await_login_password(phone, text, data):
    username = data.get("login_username", "")
    user = db.get_user_by_username(username)
    if user and db.verify_password(user, text):
        db.save_session(phone, "MAIN_MENU", user_id=user["id"])
        send_text(phone, f"✅ Bem-vindo(a), *{user['name']}*!\n\n{MENU_MAIN}")
    else:
        db.save_session(phone, "UNAUTHENTICATED")
        send_text(phone, "❌ Usuário ou senha incorretos.\n\n" + MENU_UNAUTHENTICATED)


def _handle_await_reg_name(phone, text, data):
    if len(text) < 2:
        send_text(phone, "Por favor, informe um nome válido:")
        return
    data["reg_name"] = text
    db.save_session(phone, "AWAIT_REG_EMAIL", data=data)
    send_text(phone, "📧 Informe seu *e-mail* (opcional — envie *pular* para ignorar):")


def _handle_await_reg_email(phone, text, data):
    data["reg_email"] = None if text.lower() == "pular" else text
    db.save_session(phone, "AWAIT_REG_USERNAME", data=data)
    send_text(phone, "👤 Escolha um *nome de usuário* para seu login:")


def _handle_await_reg_username(phone, text, data):
    if len(text) < 3:
        send_text(phone, "O usuário deve ter ao menos 3 caracteres. Tente novamente:")
        return
    if db.username_exists(text):
        send_text(phone, "⚠️ Esse nome de usuário já está em uso. Escolha outro:")
        return
    data["reg_username"] = text
    db.save_session(phone, "AWAIT_REG_PASSWORD", data=data)
    send_text(phone, "🔒 Crie uma *senha* (mínimo 6 caracteres):")


def _handle_await_reg_password(phone, text, data):
    if len(text) < 6:
        send_text(phone, "A senha deve ter ao menos 6 caracteres. Tente novamente:")
        return
    try:
        user = db.create_user(
            phone=phone,
            username=data["reg_username"],
            password=text,
            name=data["reg_name"],
            email=data.get("reg_email"),
        )
        db.save_session(phone, "MAIN_MENU", user_id=user["id"])
        send_text(phone, f"🎉 Conta criada com sucesso! Bem-vindo(a), *{user['name']}*!\n\n{MENU_MAIN}")
    except Exception as exc:
        logger.error("Erro ao criar usuário %s: %s", phone, exc)
        db.save_session(phone, "UNAUTHENTICATED")
        send_text(phone, "❌ Não foi possível criar a conta. Tente novamente.\n\n" + MENU_UNAUTHENTICATED)


def _handle_main_menu(phone, text, user_id, data):
    if text == "1":
        db.save_session(phone, "AWAIT_TICKET_TITLE", user_id=user_id)
        send_text(phone, "📝 *Abrir chamado*\n\nDescreva o *título/assunto* do seu chamado:")
    elif text == "2":
        _show_ticket_list(phone, user_id)
    elif text == "3":
        db.save_session(phone, "UNAUTHENTICATED")
        send_text(phone, "👋 Até logo!\n\n" + MENU_UNAUTHENTICATED)
    else:
        send_text(phone, MENU_MAIN)


def _handle_await_ticket_title(phone, text, user_id, data):
    if len(text) < 3:
        send_text(phone, "Por favor, informe um título mais descritivo:")
        return
    data["ticket_title"] = text
    db.save_session(phone, "AWAIT_TICKET_DESC", user_id=user_id, data=data)
    send_text(phone, "📄 Agora descreva o problema com mais detalhes:")


def _handle_await_ticket_desc(phone, text, user_id, data):
    if len(text) < 5:
        send_text(phone, "Por favor, forneça mais detalhes sobre o problema:")
        return
    title = data.get("ticket_title", "Sem título")
    ticket = db.create_ticket(user_id=user_id, title=title, description=text)
    _notify_n8n(ticket, phone)
    db.save_session(phone, "MAIN_MENU", user_id=user_id)
    send_text(
        phone,
        f"✅ *Chamado #{ticket['id']} aberto com sucesso!*\n\n"
        f"📌 *Assunto:* {title}\n"
        f"🔄 *Status:* {STATUS_LABELS['aberto']}\n\n"
        "Nossa equipe retornará em breve.\n\n" + MENU_MAIN,
    )


def _handle_viewing_tickets(phone, text, user_id, data):
    if text.lower() in ("0", "voltar", "menu"):
        db.save_session(phone, "MAIN_MENU", user_id=user_id)
        send_text(phone, MENU_MAIN)
        return
    try:
        ticket_id = int(text)
        ticket = db.get_ticket(ticket_id)
        if ticket and ticket["user_id"] == user_id:
            data["viewing_ticket_id"] = ticket_id
            db.save_session(phone, "VIEWING_TICKET", user_id=user_id, data=data)
            _show_ticket_detail(phone, ticket)
        else:
            send_text(phone, "❌ Chamado não encontrado. Informe o número do chamado ou *0* para voltar:")
    except ValueError:
        send_text(phone, "⚠️ Informe o número do chamado ou *0* para voltar ao menu:")


def _handle_viewing_ticket(phone, text, user_id, data):
    if text.lower() in ("0", "voltar", "menu"):
        db.save_session(phone, "MAIN_MENU", user_id=user_id)
        send_text(phone, MENU_MAIN)
    elif text.lower() == "atualizar":
        ticket_id = data.get("viewing_ticket_id")
        ticket = db.get_ticket(ticket_id)
        if ticket:
            _show_ticket_detail(phone, ticket)
        else:
            db.save_session(phone, "MAIN_MENU", user_id=user_id)
            send_text(phone, MENU_MAIN)
    else:
        send_text(phone, "Digite *atualizar* para ver as últimas mensagens ou *0* para voltar ao menu.")


# --- Helpers ---

def _show_ticket_list(phone, user_id):
    tickets = db.get_tickets_by_user(user_id)
    if not tickets:
        db.save_session(phone, "MAIN_MENU", user_id=user_id)
        send_text(phone, "📭 Você ainda não possui chamados.\n\n" + MENU_MAIN)
        return
    lines = ["📋 *Seus chamados:*\n"]
    for t in tickets:
        status = STATUS_LABELS.get(t["status"], t["status"])
        lines.append(f"• *#{t['id']}* — {t['title']}\n  {status}")
    lines.append("\nDigite o *número* do chamado para ver detalhes ou *0* para voltar:")
    db.save_session(phone, "VIEWING_TICKETS", user_id=user_id)
    send_text(phone, "\n".join(lines))


def _show_ticket_detail(phone, ticket):
    messages = db.get_ticket_messages(ticket["id"])
    status = STATUS_LABELS.get(ticket["status"], ticket["status"])
    lines = [
        f"🎫 *Chamado #{ticket['id']}*",
        f"📌 *Assunto:* {ticket['title']}",
        f"🔄 *Status:* {status}",
        f"📅 *Aberto em:* {ticket['created_at'][:16]}",
        "",
        "💬 *Mensagens:*",
    ]
    for msg in messages:
        prefix = "👤 Você" if msg["sender"] == "cliente" else "🛠️ Suporte"
        lines.append(f"\n{prefix} ({msg['created_at'][11:16]}):\n{msg['message']}")
    lines.append("\nDigite *atualizar* para ver novas mensagens ou *0* para voltar ao menu.")
    send_text(phone, "\n".join(lines))


def _notify_n8n(ticket, phone):
    url = os.getenv("N8N_WEBHOOK_URL", "")
    if not url:
        return
    user = db.get_user_by_phone(phone)
    payload = {
        "ticket_id": ticket["id"],
        "user_name": user["name"] if user else "",
        "user_phone": phone,
        "user_username": user["username"] if user else "",
        "title": ticket["title"],
        "description": ticket["description"],
        "created_at": ticket["created_at"],
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as exc:
        logger.error("Falha ao notificar n8n para ticket #%s: %s", ticket["id"], exc)


def notify_ticket_update(phone: str, ticket_id: int, message: str, status: str):
    """Called when the support team sends a response via n8n."""
    status_label = STATUS_LABELS.get(status, status)
    send_text(
        phone,
        f"🔔 *Atualização no chamado #{ticket_id}*\n\n"
        f"🔄 *Status:* {status_label}\n\n"
        f"🛠️ *Suporte:*\n{message}\n\n"
        "Responda *atualizar* para ver o chamado completo ou *0* para o menu.",
    )
