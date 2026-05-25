# Como publicar o Agente Mútua Digital

## Opção 1 — Docker Compose (recomendado)

Requisito: servidor Linux com Docker instalado (DigitalOcean, AWS, Hetzner, etc.)

```bash
# 1. Clone o repositório
git clone https://github.com/mutuadigital/agente.git
cd agente

# 2. Copie e preencha o .env
cp .env.example .env
nano .env   # preencha com os dados reais

# 3. Suba o container
docker compose up -d

# 4. Verifique os logs
docker compose logs -f
```

O agente estará acessível em `http://SEU_IP:5000`.

---

## Opção 2 — VPS sem Docker (systemd)

> **Nota:** Ubuntu 23+ bloqueia `pip3` global. Use sempre um virtualenv.

```bash
# 1. Clone o repositório (já clonado? pule este passo)
git clone https://github.com/mutuadigital/agente.git /var/www/agente

# 2. Instale python3-venv se necessário
sudo apt install python3-venv -y

# 3. Crie o ambiente virtual e instale as dependências
python3 -m venv /var/www/agente/venv
/var/www/agente/venv/bin/pip install -r /var/www/agente/requirements.txt

# 4. Configure o .env
cp /var/www/agente/.env.example /var/www/agente/.env
nano /var/www/agente/.env   # preencha com os dados reais

# 5. Instale o serviço systemd
sudo cp /var/www/agente/deploy/agente.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable agente
sudo systemctl start agente

# 6. Verifique o status
sudo systemctl status agente
journalctl -u agente -f
```

---

## Expor com HTTPS (Nginx + Certbot)

```bash
# Instale o Nginx e o Certbot
sudo apt install nginx certbot python3-certbot-nginx -y

# Crie o arquivo de configuração do Nginx
sudo nano /etc/nginx/sites-available/agente

# Cole o conteúdo abaixo (troque seu-dominio.com):
```

```nginx
server {
    server_name agente.seu-dominio.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/agente /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx

# Gere o certificado SSL
sudo certbot --nginx -d agente.seu-dominio.com
```

---

## Configurar webhook na Evolution API

```bash
curl -X POST https://wa.mutua.digital/webhook/set/mutua-rafa \
  -H "apikey: EDDFD7255C4F-4978-9679-F8B0205630EB" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://agente.seu-dominio.com/webhook/whatsapp",
    "webhook_by_events": false,
    "webhook_base64": false,
    "events": ["MESSAGES_UPSERT"]
  }'
```

---

## Configurar os fluxos no n8n

1. Acesse `https://n8n.mutua.digital`
2. Vá em **Workflows → Import from File**
3. Importe os dois arquivos da pasta `n8n/`:
   - `workflow-receber-chamado.json`
   - `workflow-responder-chamado.json`
4. Ative os dois workflows
5. No workflow *Receber Chamado*, configure o nó de e-mail com as credenciais do suporte
