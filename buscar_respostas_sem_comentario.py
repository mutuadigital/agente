#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Consulta perguntas/respostas do TutorLMS e identifica threads sem resposta do professor.

O script foi pensado para o site ethoscomunicacaoearte.com.br, mas funciona em
qualquer instalação do TutorLMS que exponha as rotas REST. Ele autentica com
usuário/senha de Aplicação do WordPress, busca perguntas do Q&A e lista apenas
as que ainda não receberam comentário de instrutores ou administradores.
"""

import argparse
import json
import os
import sys
from typing import Dict, Iterable, List, Optional, Set

import requests

TEACHER_ROLES = {
    "teacher",
    "instructor",
    "tutor_instructor",
    "administrator",
    "admin",
}
DEFAULT_QNA_ENDPOINT = "wp-json/tutor/v1/qna"
DEFAULT_ANSWERS_TEMPLATE = "wp-json/tutor/v1/qna/{question_id}/answers"


def parse_csv_arg(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def is_instructor_answer(
    answer: Dict,
    instructor_ids: Set[str],
    instructor_usernames: Set[str],
) -> bool:
    role = str(
        answer.get("user_role")
        or answer.get("author_role")
        or answer.get("role")
        or answer.get("user_role_name")
        or ""
    ).lower()
    if role in TEACHER_ROLES:
        return True

    uid = str(answer.get("user_id") or answer.get("author_id") or "").strip()
    if uid and uid in instructor_ids:
        return True

    username = str(
        answer.get("user_login")
        or answer.get("author_name")
        or answer.get("display_name")
        or ""
    ).lower()
    if username and username in instructor_usernames:
        return True

    return bool(answer.get("is_instructor") or answer.get("is_admin"))


def build_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    suffix = path.lstrip("/")
    return f"{base}/{suffix}"


def fetch_json(url: str, auth, params: Optional[Dict] = None) -> Dict:
    resp = requests.get(
        url,
        params=params or {},
        auth=auth,
        headers={"User-Agent": "TutorQnaScanner/1.0"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def collect_questions(
    base_url: str,
    endpoint: str,
    auth,
    course_id: Optional[int],
    per_page: int,
    page_limit: int,
) -> List[Dict]:
    questions: List[Dict] = []
    for page in range(1, page_limit + 1):
        params = {"page": page, "per_page": per_page}
        if course_id:
            params["course_id"] = course_id
        url = build_url(base_url, endpoint)
        data = fetch_json(url, auth, params=params)
        if not data:
            break
        if isinstance(data, dict) and "data" in data:
            payload = data.get("data") or []
        else:
            payload = data
        if not payload:
            break
        questions.extend(payload)
        if len(payload) < per_page:
            break
    return questions


def collect_answers_for_question(
    base_url: str,
    answers_endpoint_template: str,
    auth,
    question: Dict,
) -> List[Dict]:
    if question.get("answers"):
        return question["answers"]

    qid = question.get("ID") or question.get("id") or question.get("question_id")
    if not qid:
        return []
    endpoint = answers_endpoint_template.format(question_id=qid)
    url = build_url(base_url, endpoint)
    data = fetch_json(url, auth)
    if isinstance(data, dict) and "data" in data:
        return data.get("data") or []
    if isinstance(data, list):
        return data
    return []


def find_questions_without_instructor_comment(
    questions: Iterable[Dict],
    answers_endpoint_template: str,
    base_url: str,
    auth,
    instructor_ids: Set[str],
    instructor_usernames: Set[str],
) -> List[Dict]:
    pending = []
    for question in questions:
        answers = collect_answers_for_question(
            base_url, answers_endpoint_template, auth, question
        )
        instructor_replied = any(
            is_instructor_answer(ans, instructor_ids, instructor_usernames)
            for ans in answers
        )
        if not instructor_replied:
            pending.append({
                "question": question,
                "answers": answers,
            })
    return pending


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lista perguntas do TutorLMS que ainda não receberam comentário do professor.",
    )
    parser.add_argument(
        "base_url",
        nargs="?",
        default=os.getenv("TUTOR_BASE_URL"),
        help=(
            "URL base do site (ex.: https://ethoscomunicacaoearte.com.br). "
            "Pode ser informado por TUTOR_BASE_URL."
        ),
    )
    parser.add_argument(
        "username",
        nargs="?",
        default=os.getenv("TUTOR_USERNAME"),
        help="Usuário WordPress com permissão para ler o Q&A (ou variável TUTOR_USERNAME)",
    )
    parser.add_argument(
        "application_password",
        nargs="?",
        default=os.getenv("TUTOR_APP_PASSWORD"),
        help="Senha de Aplicação do WordPress (ou variável TUTOR_APP_PASSWORD)",
    )
    parser.add_argument("--course-id", type=int, default=None, help="Filtrar perguntas de um curso específico")
    parser.add_argument(
        "--qna-endpoint",
        default=DEFAULT_QNA_ENDPOINT,
        help=f"Endpoint para listar perguntas (padrão: {DEFAULT_QNA_ENDPOINT})",
    )
    parser.add_argument(
        "--answers-endpoint-template",
        default=DEFAULT_ANSWERS_TEMPLATE,
        help="Template de endpoint para respostas; use {question_id} como placeholder",
    )
    parser.add_argument("--per-page", type=int, default=20, help="Itens por página na paginação do TutorLMS")
    parser.add_argument("--page-limit", type=int, default=10, help="Máximo de páginas a buscar")
    parser.add_argument(
        "--instructor-ids",
        default=None,
        help="IDs de usuário de instrutores separados por vírgula (opcional)",
    )
    parser.add_argument(
        "--instructor-usernames",
        default=None,
        help="Usernames de instrutores separados por vírgula (opcional)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Caminho opcional para salvar o resultado em JSON",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Também imprime o JSON completo no stdout (útil para integrar em automações como o n8n)",
    )
    return parser.parse_args()


def ensure_required_args(args: argparse.Namespace) -> None:
    missing = []
    if not args.base_url:
        missing.append("base_url ou TUTOR_BASE_URL")
    if not args.username:
        missing.append("username ou TUTOR_USERNAME")
    if not args.application_password:
        missing.append("application_password ou TUTOR_APP_PASSWORD")
    if missing:
        parser_name = os.path.basename(sys.argv[0]) or "script"
        missing_str = ", ".join(missing)
        raise SystemExit(
            f"Faltaram parâmetros obrigatórios ({missing_str}). "
            f"Use `{parser_name} --help` para detalhes."
        )


def main():
    args = parse_args()
    ensure_required_args(args)
    instructor_ids = parse_csv_arg(args.instructor_ids)
    instructor_usernames = parse_csv_arg(args.instructor_usernames)

    auth = (args.username, args.application_password)
    questions = collect_questions(
        base_url=args.base_url,
        endpoint=args.qna_endpoint,
        auth=auth,
        course_id=args.course_id,
        per_page=args.per_page,
        page_limit=args.page_limit,
    )

    pending = find_questions_without_instructor_comment(
        questions=questions,
        answers_endpoint_template=args.answers_endpoint_template,
        base_url=args.base_url,
        auth=auth,
        instructor_ids=instructor_ids,
        instructor_usernames=instructor_usernames,
    )

    print("Perguntas sem resposta do professor:")
    for item in pending:
        q = item["question"]
        qid = q.get("ID") or q.get("id") or q.get("question_id")
        title = q.get("title") or q.get("question") or q.get("post_title")
        author = q.get("author_name") or q.get("author") or q.get("user_name")
        print(f"- ID {qid}: {title} (autor: {author})")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fp:
            json.dump(pending, fp, ensure_ascii=False, indent=2)
        print(f"Resultado salvo em {args.output}")

    if args.json:
        json.dump(pending, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
