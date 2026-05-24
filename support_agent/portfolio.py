"""
Catálogo de serviços e portfólio da Mútua Digital.
Base: cases-mutua-digital-base-atendimento.md
"""

PORTFOLIO_URL = "https://mutua.digital/novo/cases"

SERVICOS = [
    {
        "nome": "Sistemas e Aplicativos",
        "descricao": (
            "Desenvolvemos apps personalizados, sistemas de gestão (AppSheet, "
            "CRM, ERP) e ferramentas internas que digitalizam operações completas — "
            "produção, estoque, vendas, RH, pedidos e muito mais."
        ),
        "palavras_chave": [
            "sistema", "app", "aplicativo", "appsheet", "crm", "erp", "gestão",
            "controle", "estoque", "produção", "pedido", "inventário", "operação",
            "digitalizar", "planilha", "formulário", "dashboard", "relatório",
            "interno", "ferramenta", "software",
        ],
        "cases": [
            {
                "cliente": "Grupo Marques (Agronegócio)",
                "descricao": "Apps para piscicultura, abatedouro, RH, produção e estoque",
                "url": "https://grupomarques.com.br",
            },
            {
                "cliente": "HAOA Seguros (Gestão de Fretes)",
                "descricao": "Sistema financeiro de fretes com dashboards e conciliação",
                "url": "https://haoaseguros.com.br",
            },
            {
                "cliente": "Flora Resinas (CRM + Operação)",
                "descricao": "CRM e sistema de produção, pedidos, estoque e envios",
                "url": "https://crm.floraresinas.com.br",
            },
            {
                "cliente": "HauxHaux (E-commerce)",
                "descricao": "Sistema AppSheet para estoque, vendas e operação interna",
                "url": "https://hauxhaux.com.br",
            },
        ],
    },
    {
        "nome": "Automação de Processos",
        "descricao": (
            "Automatizamos fluxos com N8N, Python e Apps Script — "
            "relatórios automáticos, integrações com Google Drive, Sheets, Docs, "
            "WhatsApp e muito mais. Eliminamos tarefas manuais e repetitivas."
        ),
        "palavras_chave": [
            "automação", "automatizar", "n8n", "python", "script", "fluxo",
            "integração", "webhook", "api", "robô", "bot", "agendado",
            "recorrente", "manual", "repetitivo", "tarefa", "processo",
            "google sheets", "google docs", "drive", "apps script",
        ],
        "cases": [
            {
                "cliente": "Mato Grosso Econômico (Portal de Notícias)",
                "descricao": "Automações de relatórios com N8N, Python e Google Drive",
                "url": "https://mteconomico.com.br",
            },
            {
                "cliente": "AP Geradores (Energia)",
                "descricao": "Automação de captura de leads e integração com funil comercial",
                "url": "https://apgeradores.com.br",
            },
            {
                "cliente": "Diedrum (Engenharia)",
                "descricao": "Automação de envio de NF e PDFs via WhatsApp com Excel 365",
                "url": "https://diedrum.com.br",
            },
            {
                "cliente": "Instituto Luz (Terceiro Setor)",
                "descricao": "Check-in automatizado em eventos e gestão de voluntários",
                "url": "https://institutoluz.org",
            },
        ],
    },
    {
        "nome": "Atendimento com IA e WhatsApp",
        "descricao": (
            "Criamos agentes inteligentes no WhatsApp para pré-atendimento, "
            "reservas, suporte e automações conversacionais — reduzindo trabalho "
            "manual e acelerando o processo comercial."
        ),
        "palavras_chave": [
            "whatsapp", "atendimento", "agente", "bot", "ia", "inteligência artificial",
            "chatbot", "reserva", "suporte", "automático", "mensagem", "conversa",
            "evolution", "pré-atendimento", "comercial", "lead", "qualificação",
        ],
        "cases": [
            {
                "cliente": "MAC Transporte Executivo",
                "descricao": "Agente WhatsApp para atendimento e reservas integrado a planilhas",
                "url": PORTFOLIO_URL,
            },
            {
                "cliente": "Bantini Engenharia",
                "descricao": "Atendimento via WhatsApp integrado a gestão de projetos",
                "url": "https://bantiniengenharia.com.br",
            },
            {
                "cliente": "Flora Resinas",
                "descricao": "Pré-atendimento e briefing de pedidos via WhatsApp",
                "url": "https://floraresinas.com.br",
            },
            {
                "cliente": "Kern Marcelli",
                "descricao": "Automação de atendimento e prospecção via WhatsApp",
                "url": "https://kernmarcelli.com",
            },
        ],
    },
    {
        "nome": "Marketing Digital e Performance",
        "descricao": (
            "Estratégia completa de marketing digital: tráfego pago (Google Ads e Meta Ads), "
            "SEO, produção de conteúdo, analytics, funis de vendas e geração de leads."
        ),
        "palavras_chave": [
            "marketing", "tráfego", "anúncio", "ads", "google ads", "meta ads",
            "facebook", "instagram", "seo", "lead", "funil", "conversão",
            "campanha", "conteúdo", "analytics", "performance", "resultado",
            "venda", "cliente novo", "prospecção", "alcance", "segmentação",
        ],
        "cases": [
            {
                "cliente": "AP Geradores",
                "descricao": "SEO + conteúdo + automação de leads com aumento de tráfego orgânico",
                "url": "https://apgeradores.com.br",
            },
            {
                "cliente": "Juquehy Praia Hotel",
                "descricao": "Tráfego pago + e-mail marketing com aumento de reservas diretas",
                "url": "https://juquehypraiahotel.com.br",
            },
            {
                "cliente": "Fita Filmes",
                "descricao": "Tráfego pago, conteúdo e planejamento estratégico",
                "url": "https://fitafilmes.com",
            },
            {
                "cliente": "Daniel Artillo",
                "descricao": "Reestruturação de Google Ads com aumento de conversão",
                "url": "https://danielartillo.com",
            },
        ],
    },
    {
        "nome": "Landing Pages e Conversão",
        "descricao": (
            "Criamos landing pages focadas em conversão para campanhas, "
            "lançamentos, captação de leads e vendas de produtos digitais — "
            "com estrutura, copy e design orientados a resultado."
        ),
        "palavras_chave": [
            "landing page", "landing", "página de vendas", "captura", "captação",
            "formulário", "lead", "lançamento", "oferta", "produto digital",
            "infoproduto", "curso", "conversão", "campanha", "cta",
        ],
        "cases": [
            {
                "cliente": "AP Geradores",
                "descricao": "Landing page de captação de leads para locação de geradores",
                "url": "https://lp.apgeradores.com.br",
            },
            {
                "cliente": "William Costa (Infoprodutos)",
                "descricao": "LP para tráfego pago com venda via Eduzz",
                "url": "https://williamcosta.com.br",
            },
            {
                "cliente": "Rafa Violão / Toca aí!",
                "descricao": "Landing page para curso de violão com identidade visual",
                "url": "https://rafaviolao.com.br",
            },
            {
                "cliente": "Excel Solução — Pulse Curso Gestão",
                "descricao": "LP de curso corporativo integrada ao ecossistema Pulse",
                "url": "https://excelsolucao.com.br/pulse-curso-gestao/",
            },
        ],
    },
    {
        "nome": "Sites e Presença Digital",
        "descricao": (
            "Criamos sites institucionais, portfólios e e-commerces com foco em "
            "identidade da marca, usabilidade e estrutura para conversão — "
            "principalmente em WordPress e Elementor."
        ),
        "palavras_chave": [
            "site", "website", "institucional", "portfólio", "presença digital",
            "wordpress", "elementor", "hospedagem", "domínio", "página",
            "vitrine", "apresentação", "online", "internet", "criar site",
        ],
        "cases": [
            {
                "cliente": "Reune Psicologia",
                "descricao": "Site institucional + conteúdo + captação de pacientes",
                "url": "https://reunepsicologia.com.br",
            },
            {
                "cliente": "MVK Segurança e Gestão",
                "descricao": "Site + branding + e-mail profissional + materiais institucionais",
                "url": "https://mvksegurancaegestao.com.br",
            },
            {
                "cliente": "SAES Construtora",
                "descricao": "Site institucional responsivo com hospedagem e DNS",
                "url": "https://saesconstrutora.com.br",
            },
            {
                "cliente": "Estrela Náutica",
                "descricao": "Site de anúncios de embarcações e serviços náuticos",
                "url": "https://estrelanautica.com.br",
            },
        ],
    },
    {
        "nome": "Infraestrutura, Manutenção e Performance",
        "descricao": (
            "Cuidamos de servidores, performance, uptime, migrações, "
            "WordPress, WooCommerce, plugins, pagamentos e APIs — "
            "para sua operação digital funcionar sem interrupções."
        ),
        "palavras_chave": [
            "manutenção", "servidor", "hospedagem", "performance", "lento",
            "erro", "bug", "site fora", "uptime", "migração", "wordpress",
            "woocommerce", "plugin", "pagamento", "api", "ssl", "certificado",
            "banco de dados", "backup", "suporte técnico", "corrigir",
        ],
        "cases": [
            {
                "cliente": "HSMAI Brasil & Latam",
                "descricao": "Manutenção WordPress/WooCommerce + pagamentos + APIs hoteleiras",
                "url": "https://hsmaibrasil.org",
            },
            {
                "cliente": "Ethos Comunicação e Arte",
                "descricao": "Manutenção de plataforma de cursos online (Tutor LMS + Pix)",
                "url": "https://ethoscomunicacaoearte.com.br",
            },
            {
                "cliente": "Mato Grosso Econômico",
                "descricao": "Migração e monitoramento de uptime de portal de notícias",
                "url": "https://mteconomico.com.br",
            },
            {
                "cliente": "Gabriela Otto",
                "descricao": "Manutenção técnica + integrações de newsletter/CRM",
                "url": "https://gabrielaotto.com.br",
            },
        ],
    },
    {
        "nome": "Branding e Identidade Visual",
        "descricao": (
            "Criamos ou renovamos marcas completas: logotipo, paleta de cores, "
            "tipografia, manual de identidade visual, materiais gráficos "
            "e apresentações institucionais."
        ),
        "palavras_chave": [
            "logo", "logotipo", "marca", "branding", "identidade visual",
            "design", "cor", "tipografia", "manual", "material", "cartão",
            "apresentação", "visual", "estética", "criar marca", "reformular marca",
        ],
        "cases": [
            {
                "cliente": "CMS Consultancy",
                "descricao": "Logotipo + identidade visual + site + materiais comerciais",
                "url": "https://cmsconsultancy.com.br",
            },
            {
                "cliente": "MVK Segurança e Gestão",
                "descricao": "Logotipo + materiais institucionais + posts para Instagram",
                "url": "https://mvksegurancaegestao.com.br",
            },
            {
                "cliente": "Rafa Violão",
                "descricao": "Identidade visual + direção criativa para curso online",
                "url": "https://rafaviolao.com.br",
            },
        ],
    },
    {
        "nome": "Treinamento, EAD e Comunidade",
        "descricao": (
            "Estruturamos plataformas EAD, trilhas de aprendizado, "
            "capacitações em IA e automações, e comunidades com suporte "
            "humano e por IA."
        ),
        "palavras_chave": [
            "curso", "treinamento", "ead", "ensino", "aprendizado", "capacitação",
            "comunidade", "trilha", "aula", "plataforma educacional", "lms",
            "educação", "formação", "certificado", "aluno", "professor",
        ],
        "cases": [
            {
                "cliente": "Comunidade Pulse",
                "descricao": "Comunidade de aprendizado em IA e automações com EAD e suporte",
                "url": "https://comunidade.pulsegestao.com",
            },
            {
                "cliente": "Excel Solução — Ferramentas Pulse",
                "descricao": "Apps de gestão produtizados + landing page de conversão",
                "url": "https://excelsolucao.com.br/ferramentas-pulse/v4/",
            },
            {
                "cliente": "Ethos Comunicação e Arte",
                "descricao": "Plataforma de cursos online em WordPress/WooCommerce/Tutor LMS",
                "url": "https://ethoscomunicacaoearte.com.br",
            },
        ],
    },
    {
        "nome": "Estratégia de Conteúdo e UX",
        "descricao": (
            "Analisamos e reorganizamos arquitetura de informação, "
            "usabilidade, SEO e estrutura de navegação — para que o site "
            "funcione melhor para o usuário e para o Google."
        ),
        "palavras_chave": [
            "ux", "usabilidade", "conteúdo", "seo", "arquitetura", "navegação",
            "menu", "estrutura", "organizar", "reformular", "melhorar site",
            "experiência do usuário", "acessibilidade", "blog", "palavra-chave",
        ],
        "cases": [
            {
                "cliente": "Núcleo de Pesquisas",
                "descricao": "Reorganização de conteúdo, SEO e UX em WordPress/Elementor",
                "url": "https://nucleodepesquisas.com.br",
            },
            {
                "cliente": "AP Geradores",
                "descricao": "SEO + conteúdo estratégico com aumento de tráfego orgânico",
                "url": "https://apgeradores.com.br",
            },
        ],
    },
]

APRESENTACAO_MUTUA = (
    "🚀 *Mútua Digital — Tecnologia, IA e Marketing que geram resultado*\n\n"
    "Somos uma agência especializada em soluções digitais sob medida. "
    "Atendemos desde pequenas empresas até operações complexas, com foco em "
    "automatizar, digitalizar e escalar negócios.\n\n"
    "*O que fazemos:*\n"
    "⚙️ Sistemas e Aplicativos personalizados (AppSheet, CRM, ERP)\n"
    "🤖 Automação de Processos (N8N, Python, integrações)\n"
    "💬 Atendimento com IA e WhatsApp\n"
    "🎯 Marketing Digital e Performance (Google Ads, Meta Ads, SEO)\n"
    "📄 Landing Pages e Conversão\n"
    "🌐 Sites e Presença Digital\n"
    "🔧 Infraestrutura, Manutenção e Performance\n"
    "🎨 Branding e Identidade Visual\n"
    "📚 Treinamento, EAD e Comunidade\n"
    "🗺️ Estratégia de Conteúdo e UX\n\n"
    "🌐 *Conheça nosso portfólio:* https://mutua.digital/novo/cases"
)


def encontrar_servicos(texto: str) -> list[dict]:
    """Return services whose keywords appear in the given text."""
    texto_lower = texto.lower()
    encontrados = []
    for servico in SERVICOS:
        if any(k in texto_lower for k in servico["palavras_chave"]):
            encontrados.append(servico)
    return encontrados


def formatar_servico(servico: dict) -> str:
    linhas = [f"*{servico['nome']}*", servico["descricao"]]
    if servico.get("cases"):
        linhas.append("\n📎 *Exemplos do nosso trabalho:*")
        for case in servico["cases"][:2]:
            linhas.append(f"  → {case['cliente']}: {case['url']}")
    return "\n".join(linhas)
