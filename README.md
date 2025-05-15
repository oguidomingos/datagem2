<<<<<<< HEAD
# datagem2
# datagem2
=======
# Tap Executor

Serviço Python para execução de taps Singer, com suporte inicial para WooCommerce.

## Configuração

1. Copie o arquivo de exemplo para criar o .env:
```bash
cp .env.example .env
```

2. Configure o Supabase:
- Para desenvolvimento local com Supabase CLI, use as credenciais padrão do .env.example
- Para produção, atualize com suas credenciais do projeto Supabase:
  ```
  SUPABASE_URL=https://seu-projeto.supabase.co
  SUPABASE_KEY=sua_chave_de_servico
  ```

3. Instale as dependências:
```bash
pip install -e .
```

4. Instale o tap-woocommerce:
```bash
pip install tap-woocommerce
```

## Executando o servidor

1. Certifique-se que o Supabase está rodando (se estiver usando localmente)

2. Inicie o servidor:
```bash
python -m tap_executor
```

O servidor irá iniciar na porta 8000. Você verá mensagens de log indicando:
- Status da configuração do Supabase
- Criação da configuração de teste
- URL da API (http://localhost:8000)

## Troubleshooting

Se encontrar o erro "Failed to fetch" ao tentar sincronizar:

1. Verifique se o servidor está rodando em http://localhost:8000
2. Confirme que as variáveis SUPABASE_URL e SUPABASE_KEY estão configuradas
3. Verifique os logs do servidor para mensagens de erro específicas

## Desenvolvimento

- O servidor usa FastAPI com reload automático
- As rotas da API estão em src/tap_executor/api.py
- Os logs detalhados são habilitados em modo DEBUG# datagem2
>>>>>>> 50652d70b28c9a74aa695a6dc17665d2beea994d
