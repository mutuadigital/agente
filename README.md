# agente

Scripts auxiliares para automações.

## Busca de perguntas do TutorLMS sem resposta do professor

O script `buscar_respostas_sem_comentario.py` consulta o Q&A do TutorLMS (incluindo TutorLMS Pro) via API REST e lista perguntas que ainda não receberam comentário de instrutores ou administradores. Ele foi pensado para o site ethoscomunicacaoearte.com.br, mas funciona em qualquer instalação com as rotas REST habilitadas.

### Pré-requisitos
- A instância do WordPress/TutorLMS precisa aceitar requisições REST autenticadas.
- Crie uma "Senha de Aplicação" no WordPress para o usuário com permissão de leitura do Q&A.
- Python 3 com a biblioteca `requests` instalada (`pip install requests`).

### Uso
```
python buscar_respostas_sem_comentario.py \
  https://ethoscomunicacaoearte.com.br \
  usuario_wp \
  SENHA_DE_APLICACAO \
  --course-id 123 \
  --instructor-ids 5,12 \
  --instructor-usernames prof.maria,prof.joao \
  --output pendentes.json
```

Parâmetros úteis:
- `base_url`: URL do site WordPress.
- `username` e `application_password`: credenciais de autenticação básica.
- `--course-id`: filtra perguntas de um curso específico.
- `--qna-endpoint` e `--answers-endpoint-template`: permitem ajustar as rotas caso a instalação use caminhos diferentes.
- `--output`: salva o resultado em JSON.
- `--json`: imprime o JSON no stdout (útil para pipelines como o n8n ou para redirecionar com `>`).

O resultado no terminal exibe o ID, título e autor das perguntas que ainda não receberam comentário do professor.

Também é possível definir as variáveis de ambiente `TUTOR_BASE_URL`, `TUTOR_USERNAME` e `TUTOR_APP_PASSWORD` para evitar passar
os dados sensíveis na linha de comando (ou problemas com senhas contendo espaços). Exemplo com as credenciais fornecidas:

```
export TUTOR_BASE_URL=https://ethoscomunicacaoearte.com.br
export TUTOR_USERNAME="sourenato@gmail.com"
export TUTOR_APP_PASSWORD="L6Io OfQt Q9GT 2TkU rRge xkJQ"
python buscar_respostas_sem_comentario.py --json --page-limit 2 --per-page 10
```

### Integração rápida com n8n
1. Crie um nó **Execute Command** rodando o script com `--json` (preferencialmente usando as variáveis de ambiente acima no próprio nó ou em credenciais) e capture a saída como JSON.
2. Opcionalmente use um nó **Set** para transformar/renomear os campos `question` e `answers` do retorno conforme a necessidade.
3. Encadeie com outros nós (por exemplo, **HTTP Request** ou **Email**) para notificar sobre perguntas pendentes.

A saída em JSON é uma lista de objetos no formato:

```
[
  {
    "question": {...},
    "answers": [...]
  },
  ...
]
```
