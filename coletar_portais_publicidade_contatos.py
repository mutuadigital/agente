#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv
import time
import json
import random
import logging
import requests
import pandas as pd
import tldextract
import phonenumbers

from io import StringIO
from datetime import datetime, timezone
from dateutil import parser as dtparser
from urllib.parse import urlparse, urljoin, parse_qs
from bs4 import BeautifulSoup

# ========================= CONFIG GERAL =========================
OUT_DIR = "saida_publicidade"
os.makedirs(OUT_DIR, exist_ok=True)
OUTPUT_CSV = os.path.join(
    OUT_DIR, f"portais_publicidade_contatos_{datetime.now().date().isoformat()}.csv"
)

REQUEST_TIMEOUT = 40
SLEEP_RANGE = (0.4, 1.0)
PER_DOMAIN_MAX_PAGES = 6
PER_DOMAIN_MAX_SECONDS = 60

SEARCH_QUERIES = [
    "veiculações autorizadas",
    "planejamento de mídia SICOM",
    "publicidade internet",
    "veiculação internet",
    "publicidade digital",
    "campanha publicitária internet",
]

DIGITAL_HINTS = ["internet", "digital", "web", "online", "site", "portal"]
BANNER_HINTS  = ["banner", "display", "programático", "programatico"]

CONTACT_HINTS = [
    "contato", "fale", "atendimento", "anuncie", "publicidade",
    "quem-somos", "sobre", "comercial", "media", "advertise", "contact"
]

CKAN_VEHICLE_COL_HINTS = ["veicul", "site", "portal", "url", "dominio", "veículo"]

# ============== CATÁLOGOS CKAN (base_url termina em /api/3/action) ==============
CKAN_CATALOGS = [
    # Federal (SECOM / Presidência)
    "https://dadosabertos.presidencia.gov.br/api/3/action",
    # Metacatálogo nacional
    "https://dados.gov.br/api/3/action",

    # Estaduais (exemplos com CKAN ativo)
    "https://dadosabertos.sp.gov.br/api/3/action",     # SP
    "https://dadosabertos.rj.gov.br/api/3/action",     # RJ
    "https://dados.mg.gov.br/api/3/action",            # MG
    "https://dados.rs.gov.br/api/3/action",            # RS
    "https://dados.pe.gov.br/api/3/action",            # PE
    "https://dados.df.gov.br/api/3/action",            # DF
    "https://dados.ba.gov.br/api/3/action",            # BA
    "https://dadosabertos.go.gov.br/api/3/action",     # GO
    "https://dados.sc.gov.br/api/3/action",            # SC
    "https://dados.rn.gov.br/api/3/action",            # RN
    "https://dados.ro.gov.br/api/3/action",            # RO
]

# ======== PREFERRED (fallback federal direto por slug conhecido no dados.gov.br) ========
PREFERRED_FEDERAL_BASE = "https://dados.gov.br/api/3/action"
PREFERRED_DATASET_NAMES = [
    # slug mais comum/recente mapeado pela SECOM:
    "https-gestaosecom-mcom-gov-br-gestaosecom-seguranca-dados-abertos-veiculacoes-autorizadas",
    # variações históricas/espelhamentos (se existirem, mantemos como tentativa):
    "veiculacoes-autorizadas",  # genérico (caso exista em alguma publicação)
]

# ======== PLANO B (PORTAIS DE TRANSPARÊNCIA) ========
TRANSPARENCIA_PAGES = [
    "https://www.portaltransparencia.gov.br/",
    "https://www.transparencia.sp.gov.br/",
    "https://www.transparencia.rj.gov.br/",
    "https://www.transparencia.mg.gov.br/",
    "https://transparencia.rs.gov.br/",
    "https://www.portaltransparencia.pe.gov.br/",
    "https://www.transparencia.df.gov.br/",
    "https://www.transparencia.ba.gov.br/",
    "https://www.transparencia.go.gov.br/",
    "https://www.transparencia.sc.gov.br/",
    "https://www.transparencia.rn.gov.br/",
    "https://www.transparencia.ro.gov.br/",
]

# ========================= LOGGING =========================
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")


# ========================= HELPERS WEB =========================
def sleep_a_bit():
    time.sleep(random.uniform(*SLEEP_RANGE))

def safe_get(url, session, headers=None):
    sleep_a_bit()
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT, headers=headers, allow_redirects=True)
        r.raise_for_status()
        if int(r.headers.get("Content-Length") or 0) > 6_000_000:
            return None
        if len(r.content) > 6_000_000:
            return None
        return r
    except Exception as e:
        logging.debug(f"GET falhou {url}: {e}")
        return None

def to_soup(resp):
    try:
        return BeautifulSoup(resp.text, "lxml")
    except Exception:
        return BeautifulSoup(resp.text, "html.parser")

def extract_domain_any(val: str) -> str:
    if not val:
        return ""
    s = str(val).strip()
    if re.search(r"https?://", s, re.I):
        try:
            host = urlparse(s).netloc.split(":")[0]
            return host.lower()
        except Exception:
            pass
    m = re.search(r"([a-z0-9.-]+\.[a-z]{2,})(?:/|$)", s, re.I)
    if m:
        return m.group(1).lower()
    return s.lower()

def looks_digital_text(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in DIGITAL_HINTS) or any(k in t for k in BANNER_HINTS)

def looks_digital_row(row: pd.Series) -> bool:
    text = " ".join([str(x).lower() for x in row.dropna().values.tolist()])
    return looks_digital_text(text)

def normalize_phone_br(raw):
    cleaned = re.sub(r"[^\d+]", "", raw or "")
    try:
        if cleaned.startswith("+"):
            num = phonenumbers.parse(cleaned, None)
        else:
            num = phonenumbers.parse(cleaned, "BR")
        if phonenumbers.is_possible_number(num) and phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None
    return None

def extract_contacts_from_soup(soup, base_url):
    emails, phones, whatsapps = set(), set(), set()
    text = soup.get_text(separator=" ", strip=True)
    for mail in set(re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, flags=re.I)):
        emails.add(mail.lower())
    for raw in set(re.findall(r"(?:\+?\d[\d\s().-]{8,}\d)", text)):
        e164 = normalize_phone_br(raw)
        if e164:
            phones.add(e164)
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        low = href.lower()
        if low.startswith("mailto:"):
            emails.add(low.split(":", 1)[1])
        elif low.startswith("tel:"):
            e164 = normalize_phone_br(href.split(":",1)[1])
            if e164: phones.add(e164)
        elif "wa.me/" in low or "api.whatsapp.com" in low:
            if "wa.me/" in low:
                path = urlparse(href).path.strip("/")
                digits = re.sub(r"[^\d]", "", path)
                e164 = normalize_phone_br("+" + digits if not digits.startswith("+") else digits)
                if e164:
                    whatsapps.add(e164); phones.add(e164)
            else:
                q = parse_qs(urlparse(href).query)
                phone_param = q.get("phone", [None])[0]
                if phone_param:
                    e164 = normalize_phone_br(phone_param)
                    if e164:
                        whatsapps.add(e164); phones.add(e164)
    return emails, phones, whatsapps

def crawl_domain_for_contacts(domain, session):
    start = time.time()
    visited = set()
    found = {"emails": set(), "phones": set(), "whatsapps": set()}
    sample_url = None
    base_urls = [f"{scheme}{domain}" for scheme in ("https://","http://")]

    def enqueue_links(base_url, soup):
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("#"): continue
            full = urljoin(base_url, href)
            a_ext = tldextract.extract(full)
            d_ext = tldextract.extract(base_url)
            if (a_ext.domain, a_ext.suffix) == (d_ext.domain, d_ext.suffix):
                links.append((full, href.lower()))
        links.sort(key=lambda x: -sum(1 for h in CONTACT_HINTS if h in x[1]))
        ordered, seen = [], set()
        for u,_ in links:
            if u not in seen:
                seen.add(u); ordered.append(u)
        return ordered[:4]

    for root in base_urls:
        if time.time() - start > PER_DOMAIN_MAX_SECONDS: break
        if len(visited) >= PER_DOMAIN_MAX_PAGES: break
        headers = {"User-Agent":"Mozilla/5.0 (compatible; outreach-bot/1.0)"}
        resp = safe_get(root, session, headers=headers)
        if not resp or "text/html" not in (resp.headers.get("Content-Type") or ""): continue
        soup = to_soup(resp)
        emails, phones, wpp = extract_contacts_from_soup(soup, root)
        if (emails or phones or wpp) and not sample_url: sample_url = root
        found["emails"] |= emails; found["phones"] |= phones; found["whatsapps"] |= wpp
        visited.add(root)
        for link in enqueue_links(root, soup):
            if time.time() - start > PER_DOMAIN_MAX_SECONDS: break
            if len(visited) >= PER_DOMAIN_MAX_PAGES: break
            if link in visited: continue
            visited.add(link)
            resp2 = safe_get(link, session, headers=headers)
            if not resp2 or "text/html" not in (resp2.headers.get("Content-Type") or ""): continue
            soup2 = to_soup(resp2)
            emails2, phones2, wpp2 = extract_contacts_from_soup(soup2, link)
            if (emails2 or phones2 or wpp2) and not sample_url: sample_url = link
            found["emails"] |= emails2; found["phones"] |= phones2; found["whatsapps"] |= wpp2
    return found, sample_url


# ========================= CKAN ADAPTER =========================
def ckan_package_search(base, query, rows=50):
    url = f"{base}/package_search"
    r = requests.get(url, params={"q": query, "rows": rows}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if not j.get("success"): return []
    return j["result"]["results"]

def ckan_package_show(base, ident):
    url = f"{base}/package_show"
    r = requests.get(url, params={"id": ident}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if not j.get("success"): return None
    return j["result"]

def pick_dataset(candidates):
    if not candidates: return None
    def score(p):
        t = (p.get("title") or "").lower()
        s = 0
        if "veicula" in t: s += 3
        if "planejamento" in t: s += 2
        if "sicom" in t: s += 1
        s += len(p.get("resources") or [])
        return s
    return sorted(candidates, key=score, reverse=True)[0]

def pick_latest_resource(resources):
    if not resources: return None
    def rscore(r):
        fmt = ((r.get("format") or r.get("mimetype") or "")).lower()
        score = 0
        if "csv" in fmt: score += 3
        if "json" in fmt: score += 2
        dt_str = r.get("last_modified") or r.get("created")
        try: dt = dtparser.parse(dt_str)
        except Exception: dt = datetime(1970,1,1, tzinfo=timezone.utc)
        return (score, dt)
    return sorted(resources, key=rscore, reverse=True)[0]

def download_text(url):
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text, (r.headers.get("Content-Type") or "").lower()

def df_from_resource(text, content_type):
    if "csv" in content_type or "," in text[:200]:
        try: return pd.read_csv(StringIO(text))
        except Exception: pass
    try:
        data = json.loads(text)
        if isinstance(data, dict) and ("result" in data or "records" in data):
            recs = data.get("result") or data.get("records") or []
            return pd.DataFrame(recs)
        elif isinstance(data, list):
            return pd.DataFrame(data)
        else:
            return pd.DataFrame([data])
    except Exception:
        try: return pd.read_csv(StringIO(text))
        except Exception: return pd.DataFrame()

def extract_domains_from_ckan_df(df):
    if df.empty: return set()
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    veh_cols = [c for c in df.columns if any(k in c for k in CKAN_VEHICLE_COL_HINTS)]
    df["_digital"] = df.apply(looks_digital_row, axis=1)
    dfd = df[df["_digital"]].copy()
    if dfd.empty: return set()
    domains = set()
    if not veh_cols:
        dfd["_join"] = dfd.apply(lambda r: " ".join([str(x) for x in r.values]), axis=1)
        for v in dfd["_join"].astype(str).tolist():
            dom = extract_domain_any(v);  
            if dom: domains.add(dom)
    else:
        for col in veh_cols:
            for v in dfd[col].astype(str).tolist():
                dom = extract_domain_any(v);  if dom: domains.add(dom)
    return domains


# ========================= TRANSPARÊNCIA ADAPTER =========================
def extract_domains_from_transparency_page(url, session):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; outreach-bot/1.0)"}
    resp = safe_get(url, session, headers=headers)
    if not resp: return set()
    soup = to_soup(resp)
    text = soup.get_text(separator=" ", strip=True)
    domains = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#"): continue
        dom = extract_domain_any(href)
        if dom and dom not in ("www.portaltransparencia.gov.br", "dados.gov.br"):
            domains.add(dom)
    for m in set(re.findall(r"([a-z0-9.-]+\.[a-z]{2,})", text, flags=re.I)):
        if len(m) < 6: continue
        dom = extract_domain_any(m)
        if dom: domains.add(dom)
    return domains


# ========================= CORE (com fallback federal) =========================
def process_specific_dataset(base, ident):
    """Tenta baixar um dataset específico por ID/slug e extrair domínios."""
    try:
        pkg = ckan_package_show(base, ident)
        if not pkg:
            return set(), None, None
        resources = pkg.get("resources") or []
        res = pick_latest_resource(resources)
        if not res:
            return set(), pkg.get("title") or "", None
        text, ctype = download_text(res.get("url"))
        df = df_from_resource(text, ctype)
        domains = extract_domains_from_ckan_df(df)
        return domains, (pkg.get("title") or ""), res
    except Exception:
        return set(), None, None


def main():
    session = requests.Session()
    all_rows = []
    seen_domains = set()

    # ---------- 0) Fallback federal direto (dados.gov.br + slug preferido) ----------
    logging.info("[CKAN] Tentando fallback federal direto (dados.gov.br + slug conhecido)…")
    got_any_federal = False
    for ident in PREFERRED_DATASET_NAMES:
        domains, title, res = process_specific_dataset(PREFERRED_FEDERAL_BASE, ident)
        if domains:
            got_any_federal = True
            logging.info(f"  dataset: {title} | recurso: {res.get('name') or res.get('format')}")
            for dom in sorted(domains):
                if dom in seen_domains: continue
                seen_domains.add(dom)
                all_rows.append({
                    "source": "CKAN",
                    "catalog_or_portal": PREFERRED_FEDERAL_BASE.replace("/api/3/action",""),
                    "dataset_or_page": title,
                    "domain": dom,
                    "emails": "",
                    "phones": "",
                    "whatsapps": "",
                    "first_seen": datetime.now().isoformat(timespec="seconds"),
                    "sample_url": ""
                })
            break
    if not got_any_federal:
        logging.info("  fallback federal não retornou domínios — seguirei com buscas genéricas.")

    # ---------- 1) CKAN (busca genérica nos catálogos) ----------
    for base in CKAN_CATALOGS:
        logging.info(f"[CKAN] {base}")
        candidates = []
        for q in SEARCH_QUERIES:
            try:
                candidates += ckan_package_search(base, q, rows=50)
            except Exception as e:
                logging.debug(f"package_search falhou {base}: {e}")
        pkg_meta = pick_dataset(candidates)
        if not pkg_meta:
            logging.info("  nenhum dataset relevante aqui.")
            continue
        try:
            pkg = ckan_package_show(base, pkg_meta.get("id") or pkg_meta.get("name"))
        except Exception as e:
            logging.debug(f"package_show falhou {base}: {e}")
            continue
        title = pkg.get("title") or ""
        res = pick_latest_resource(pkg.get("resources") or [])
        if not res:
            logging.info("  dataset sem recurso útil.")
            continue
        # >>> FIX do NameError aqui:
        logging.info(f"  dataset: {title} | recurso: {res.get('name') or res.get('format')}")
        try:
            text, ctype = download_text(res.get("url"))
            df = df_from_resource(text, ctype)
            domains = extract_domains_from_ckan_df(df)
        except Exception as e:
            logging.debug(f"processamento falhou: {e}")
            domains = set()
        for dom in sorted(domains):
            if dom in seen_domains: continue
            seen_domains.add(dom)
            all_rows.append({
                "source": "CKAN",
                "catalog_or_portal": base.replace("/api/3/action",""),
                "dataset_or_page": title,
                "domain": dom,
                "emails": "",
                "phones": "",
                "whatsapps": "",
                "first_seen": datetime.now().isoformat(timespec="seconds"),
                "sample_url": ""
            })

    # ---------- 2) PLANO B: TRANSPARÊNCIA ----------
    logging.info("[Plano B] Portais da Transparência…")
    for url in TRANSPARENCIA_PAGES:
        logging.info(f"  {url}")
        try:
            domains = extract_domains_from_transparency_page(url, session)
        except Exception as e:
            logging.debug(f"transparência falhou {url}: {e}")
            domains = set()
        for dom in sorted(domains):
            if dom in seen_domains: continue
            if any(dom.endswith(sfx) for sfx in (".gov.br",".sp.gov.br",".rj.gov.br",".rs.gov.br",".df.gov.br",
                                                 ".pe.gov.br",".mg.gov.br",".ba.gov.br",".sc.gov.br",".go.gov.br",
                                                 ".rn.gov.br",".ro.gov.br")):
                if not any(k in dom for k in ("noticia","news","jornal","economico","gazeta","diario","portal")):
                    continue
            seen_domains.add(dom)
            all_rows.append({
                "source": "Transparencia",
                "catalog_or_portal": url,
                "dataset_or_page": "Página inicial/contratos (heurística)",
                "domain": dom,
                "emails": "",
                "phones": "",
                "whatsapps": "",
                "first_seen": datetime.now().isoformat(timespec="seconds"),
                "sample_url": ""
            })

    # ---------- 3) CONTATOS ----------
    logging.info("[Contatos] coletando e-mails/telefones/whatsapp…")
    for row in all_rows:
        dom = row["domain"]
        try:
            contacts, sample = crawl_domain_for_contacts(dom, session)
        except Exception:
            contacts, sample = ({"emails": set(), "phones": set(), "whatsapps": set()}, None)
        row["emails"] = "; ".join(sorted(contacts["emails"])) if contacts["emails"] else ""
        row["phones"] = "; ".join(sorted(contacts["phones"])) if contacts["phones"] else ""
        row["whatsapps"] = "; ".join(sorted(contacts["whatsapps"])) if contacts["whatsapps"] else ""
        if sample: row["sample_url"] = sample

    # ---------- 4) SALVAR ----------
    unique = {}
    for r in all_rows:
        key = (r["source"], r["catalog_or_portal"], r["domain"])
        if key not in unique:
            unique[key] = r
        else:
            base = unique[key]
            def merge(a,b):
                sa = set(filter(None, (a or "").split("; ")))
                sb = set(filter(None, (b or "").split("; ")))
                return "; ".join(sorted(sa|sb))
            base["emails"] = merge(base["emails"], r["emails"])
            base["phones"] = merge(base["phones"], r["phones"])
            base["whatsapps"] = merge(base["whatsapps"], r["whatsapps"])
            if not base["sample_url"] and r["sample_url"]:
                base["sample_url"] = r["sample_url"]

    rows = list(unique.values())
    fieldnames = ["source","catalog_or_portal","dataset_or_page","domain","emails","phones","whatsapps","first_seen","sample_url"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows: w.writerow(r)

    logging.info(f"Concluído! Registros em: {OUTPUT_CSV}")
    logging.info(f"Total de domínios únicos: {len(rows)}")

if __name__ == "__main__":
    main()
