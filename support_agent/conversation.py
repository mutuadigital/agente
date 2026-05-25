import os
import requests
import logging
from datetime import datetime, timezone, timedelta
from . import database as db
from .whatsapp import send_text
from . import portfolio as pf

logger = logging.getLogger(__name__)

_SAO_PAULO = timezone(timedelta(hours=-3))


def _fmt_dt(ts: str) -> str:
    """'2026-05-25 00:36:00' (UTC) → '24/05/2026 21:36' (São Paulo)"""
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc).astimezone(_SAO_PAULO)
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ts[:16]


def _fmt_time(ts: str) -> str:
    """'2026-05-25 00:36:00' (UTC) → '21:36' (São Paulo)"""
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc).astimezone(_SAO_PAULO)
        return dt.strftime("%H:%M")
    except Exception:
        return ts[11:16]

STATUS_LABELS = {
    "aberto": "🟡 Aberto",
    "em_andamento": "🔵 Em andamento",
    "resolvido": "🟢 Resolvido",
    "fechado": "⚫ Fechado",
}

MENU_LOGIN = (
    "🔐 *Área do cliente*\n\n"
    "1️⃣ Fazer login\n"
    "2️⃣ Falar com atendimento"
)

MENU_MAIN = (
    "📋 *Menu principal*\n\n"
    "1️⃣ Abrir chamado\n"
    "2️⃣ Meus chamados\n"
    "3️⃣ Sair"
)

_SIM = {"sim", "s", "yes", "y", "1"}
_NAO = {"não", "nao", "n", "no", "2"}


def handle_message(phone: str, text: str):
    session = db.get_session(phone)
    state = session["state"]
    user_id = session["user_id"]
    data = session["data"]
    text = text.strip()

    dispatch = {
        "AWAIT_IS_CLIENT":        _await_is_client,
        "AWAIT_HAS_REGISTRATION": _await_has_registration,
        "AWAIT_LOGIN_EMAIL":      _await_login_email,
        "AWAIT_LOGIN_KEY":        _await_login_key,
        "AWAIT_REG_NAME":         _await_reg_name,
        "AWAIT_REG_EMAIL":        _await_reg_email,
        "AWAIT_REG_KEY":          _await_reg_key,
        "UNAUTHENTICATED":        _unauthenticated,
        "MAIN_MENU":              _main_menu,
        "AWAIT_TICKET_TITLE":     _await_ticket_title,
        "AWAIT_TICKET_DESC":      _await_ticket_desc,
        "VIEWING_TICKETS":        _viewing_tickets,
        "VIEWING_TICKET":         _viewing_ticket,
        "NON_CLIENT_NEED":        _non_client_need,
    }

    handler = dispatch.get(state)
    if handler:
        handler(phone, text, user_id, data)
    else:
        db.save_session(phone, "AWAIT_IS_CLIENT")
        _send_is_client(phone)


# ── First-contact flow ─────────────────────────────────────────────────────────

def _send_is_client(phone):
    send_text(
        phone,
        "👋 Olá! Seja bem-vindo(a) à *Mútua Digital*!\n\n"
        "Você já é nosso cliente?\n"
        "• Responda *Sim* ou *Não*",
    )


def _await_is_client(phone, text, user_id, data):
    if text.lower() in _SIM:
        db.save_session(phone, "AWAIT_HAS_REGISTRATION", data=data)
        send_text(
            phone,
            "Ótimo! 😊\n\n"
            "Você já possui cadastro no nosso sistema de atendimento?\n"
            "• Responda *Sim* ou *Não*",
        )
    elif text.lower() in _NAO:
        db.save_session(phone, "NON_CLIENT_NEED", data=data)
        send_text(phone, pf.APRESENTACAO_MUTUA)
        send_text(
            phone,
            "Ficou com interesse? Conta pra mim o que você está procurando "
            "e te mostro exemplos do nosso trabalho! 🎯",
        )
    else:
        _send_is_client(phone)


def _await_has_registration(phone, text, user_id, data):
    if text.lower() in _SIM:
        # Client with existing account → login
        db.save_session(phone, "AWAIT_LOGIN_EMAIL", data=data)
        send_text(phone, "📧 Informe seu *e-mail* cadastrado:")
    elif text.lower() in _NAO:
        # Client without account → register
        db.save_session(phone, "AWAIT_REG_NAME", data=data)
        send_text(
            phone,
            "Vamos criar seu cadastro rapidinho! 🚀\n\n"
            "Qual é o seu *nome completo*?",
        )
    else:
        send_text(
            phone,
            "Por favor, responda *Sim* se já tem cadastro ou *Não* se ainda não tem.",
        )


# ── Non-client flow ────────────────────────────────────────────────────────────

def _non_client_need(phone, text, user_id, data):
    servicos = pf.encontrar_servicos(text)
    if servicos:
        linhas = ["✨ *Encontrei serviços relacionados ao que você precisa:*\n"]
        for s in servicos[:3]:
            linhas.append(pf.formatar_servico(s))
            linhas.append("")
        linhas.append(
            "💬 Gostaria de conversar com nossa equipe sobre este serviço?\n"
            "Responda *Sim* e entraremos em contato, ou descreva melhor sua necessidade."
        )
        send_text(phone, "\n".join(linhas))
    elif text.lower() in _SIM:
        send_text(
            phone,
            "Ótimo! 🙌 Nossa equipe vai entrar em contato em breve.\n\n"
            "Enquanto isso, conheça nosso trabalho:\n"
            "🌐 https://mutua.digital",
        )
    else:
        send_text(
            phone,
            "Não encontrei um serviço específico para o que você mencionou, "
            "mas nossa equipe pode te ajudar!\n\n"
            "Descreva com mais detalhes o que você precisa, ou acesse:\n"
            "🌐 https://mutua.digital\n\n"
            "Quer que a equipe entre em contato? Responda *Sim*.",
        )


# ── Login flow ─────────────────────────────────────────────────────────────────

def _unauthenticated(phone, text, user_id, data):
    if text == "1":
        db.save_session(phone, "AWAIT_LOGIN_EMAIL", data=data)
        send_text(phone, "📧 Informe seu *e-mail* cadastrado:")
    elif text == "2":
        db.save_session(phone, "NON_CLIENT_NEED", data=data)
        send_text(
            phone,
            "Claro! Conte o que você precisa e te ajudo 😊",
        )
    else:
        send_text(phone, MENU_LOGIN)


def _await_login_email(phone, text, user_id, data):
    data["login_email"] = text.lower().strip()
    db.save_session(phone, "AWAIT_LOGIN_KEY", data=data)
    send_text(phone, "🔑 Informe sua *chave de segurança*:")


def _await_login_key(phone, text, user_id, data):
    email = data.get("login_email", "")
    user = db.get_user_by_email(email)
    if user and db.verify_security_key(user, text):
        db.save_session(phone, "MAIN_MENU", user_id=user["id"])
        send_text(phone, f"✅ Bem-vindo(a) de volta, *{user['name']}*!\n\n{MENU_MAIN}")
    else:
        db.save_session(phone, "UNAUTHENTICATED")
        send_text(
            phone,
            "❌ E-mail ou chave de segurança incorretos.\n\n" + MENU_LOGIN,
        )


# ── Registration flow ──────────────────────────────────────────────────────────

def _await_reg_name(phone, text, user_id, data):
    if len(text) < 2:
        send_text(phone, "Por favor, informe um nome válido:")
        return
    data["reg_name"] = text
    db.save_session(phone, "AWAIT_REG_EMAIL", data=data)
    send_text(phone, "📧 Informe seu *e-mail*:")


def _await_reg_email(phone, text, user_id, data):
    email = text.lower().strip()
    if "@" not in email or "." not in email:
        send_text(phone, "Por favor, informe um e-mail válido:")
        return
    if db.email_exists(email):
        send_text(
            phone,
            "⚠️ Este e-mail já possui cadastro.\n\n"
            "Por favor, informe outro e-mail ou volte para fazer login:",
        )
        return
    data["reg_email"] = email
    db.save_session(phone, "AWAIT_REG_KEY", data=data)
    send_text(
        phone,
        "🔒 Crie sua *chave de segurança*\n"
        "(mínimo 4 caracteres — pode ser uma palavra, número ou combinação):",
    )


def _await_reg_key(phone, text, user_id, data):
    if len(text) < 4:
        send_text(phone, "A chave de segurança deve ter ao menos 4 caracteres. Tente novamente:")
        return
    try:
        user = db.create_user(
            phone=phone,
            email=data["reg_email"],
            security_key=text,
            name=data["reg_name"],
        )
        db.save_session(phone, "MAIN_MENU", user_id=user["id"])
        send_text(
            phone,
            f"🎉 *Cadastro realizado com sucesso!*\n"
            f"Bem-vindo(a), *{user['name']}*!\n\n"
            f"Seus dados de acesso:\n"
            f"📧 E-mail: {user['email']}\n"
            f"🔒 Chave de segurança: a que você criou\n\n"
            + MENU_MAIN,
        )
    except Exception:
        logger.exception("Erro ao criar usuário %s", phone)
        db.save_session(phone, "UNAUTHENTICATED")
        send_text(
            phone,
            "❌ Não foi possível criar o cadastro. Tente novamente.\n\n" + MENU_LOGIN,
        )


# ── Main menu flow ─────────────────────────────────────────────────────────────

def _main_menu(phone, text, user_id, data):
    if text == "1":
        db.save_session(phone, "AWAIT_TICKET_TITLE", user_id=user_id)
        send_text(phone, "📝 *Abrir chamado*\n\nQual é o *assunto* do seu chamado?")
    elif text == "2":
        _show_ticket_list(phone, user_id)
    elif text == "3":
        db.save_session(phone, "UNAUTHENTICATED")
        send_text(phone, "👋 Até logo!\n\n" + MENU_LOGIN)
    else:
        send_text(phone, MENU_MAIN)


def _await_ticket_title(phone, text, user_id, data):
    if len(text) < 3:
        send_text(phone, "Por favor, informe um título mais descritivo:")
        return
    data["ticket_title"] = text
    db.save_session(phone, "AWAIT_TICKET_DESC", user_id=user_id, data=data)
    send_text(phone, "📄 Descreva o problema com mais detalhes:")


def _await_ticket_desc(phone, text, user_id, data):
    if len(text) < 5:
        send_text(phone, "Por favor, forneça mais detalhes:")
        return
    title = data.get("ticket_title", "Sem título")
    ticket = db.create_ticket(user_id=user_id, title=title, description=text)
    _notify_n8n(ticket, phone)
    db.save_session(phone, "MAIN_MENU", user_id=user_id)
    send_text(
        phone,
        f"✅ *Chamado #{ticket['id']} aberto!*\n\n"
        f"📌 *Assunto:* {title}\n"
        f"🔄 *Status:* {STATUS_LABELS['aberto']}\n\n"
        "Nossa equipe responderá em breve.\n\n" + MENU_MAIN,
    )


# ── Ticket viewing ─────────────────────────────────────────────────────────────

def _viewing_tickets(phone, text, user_id, data):
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
            send_text(phone, "❌ Chamado não encontrado. Informe o número ou *0* para voltar:")
    except ValueError:
        send_text(phone, "⚠️ Informe o número do chamado ou *0* para voltar ao menu:")


def _viewing_ticket(phone, text, user_id, data):
    if text.lower() in ("0", "voltar", "menu"):
        db.save_session(phone, "MAIN_MENU", user_id=user_id)
        send_text(phone, MENU_MAIN)
    elif text.lower() == "atualizar":
        ticket = db.get_ticket(data.get("viewing_ticket_id"))
        if ticket:
            _show_ticket_detail(phone, ticket)
        else:
            db.save_session(phone, "MAIN_MENU", user_id=user_id)
            send_text(phone, MENU_MAIN)
    else:
        send_text(phone, "Digite *atualizar* para ver novas mensagens ou *0* para voltar.")


# ── Helpers ────────────────────────────────────────────────────────────────────

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
        f"📅 *Aberto em:* {_fmt_dt(ticket['created_at'])}",
        "",
        "💬 *Mensagens:*",
    ]
    for msg in messages:
        prefix = "👤 Você" if msg["sender"] == "cliente" else "🛠️ Suporte"
        lines.append(f"\n{prefix} ({_fmt_time(msg['created_at'])}):\n{msg['message']}")
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
        "user_email": user["email"] if user else "",
        "user_phone": phone,
        "title": ticket["title"],
        "description": ticket["description"],
        "created_at": ticket["created_at"],
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception:
        logger.exception("Falha ao notificar n8n para ticket #%s", ticket["id"])


def notify_ticket_update(phone: str, ticket_id: int, message: str, status: str):
    """Sends support team response to the customer via WhatsApp."""
    status_label = STATUS_LABELS.get(status, status)
    send_text(
        phone,
        f"🔔 *Atualização no chamado #{ticket_id}*\n\n"
        f"🔄 *Status:* {status_label}\n\n"
        f"🛠️ *Suporte:*\n{message}\n\n"
        "Responda *atualizar* para ver o chamado completo ou *0* para o menu.",
    )
