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

```bash
# 1. Clone o repositório
git clone https://github.com/mutuadigital/agente.git /opt/agente
cd /opt/agente

# 2. Configure o .env
cp .env.example .env
nano .env

# 3. Instale as dependências
pip3 install -r requirements.txt

# 4. Instale o serviço systemd
sudo cp deploy/agente.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable agente
sudo systemctl start agente

# 5. Verifique o status
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
