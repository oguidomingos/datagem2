"""API FastAPI para o executor de taps."""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from typing import Optional
import logging
import os
import json
from pathlib import Path

from .sync import sync_connection
from .storage import SupabaseConfig, SupabaseStorage

app = FastAPI(
    title="Tap Executor API",
    description="API para execução de taps Singer"
)

# Configurar CORS - permitir todas as origens em desenvolvimento
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, especifique as origens permitidas
    allow_credentials=False,  # Como estamos usando credentials: 'omit' no frontend
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Accept"],
    expose_headers=["*"],
    max_age=3600,  # Cache preflight por 1 hora
)

# Log para debug de CORS
logger = logging.getLogger(__name__)
logger.info("✅ CORS configurado:")
logger.info("  - Origins: *")
logger.info("  - Methods: GET, POST, PUT, DELETE, OPTIONS")
logger.info("  - Headers: Content-Type, Accept")

# Rota OPTIONS para debugging
@app.options("/{full_path:path}")
async def options_route(full_path: str):
    logger.debug(f"Recebido request OPTIONS para: /{full_path}")
    return {}

# Configurar logging
# Configurar logging mais detalhado
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Configurar níveis específicos
logging.getLogger('tap_executor.sync').setLevel(logging.DEBUG)
logging.getLogger('tap_executor.tap_runner').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

@app.post("/api/connections/{connection_id}/sync", status_code=200)
async def trigger_sync(connection_id: str):
    """
    Inicia a sincronização de uma conexão.
    
    Args:
        connection_id: ID da conexão a ser sincronizada
        
    Returns:
        Mensagem de sucesso ou erro
    """
    logger.info("="*80)
    logger.info("INICIANDO SINCRONIZAÇÃO")
    logger.info(f"ID da Conexão: {connection_id}")
    logger.info("="*80)
    
    try:
        # Configura Supabase
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            error_msg = "Configuração do Supabase não encontrada. Verifique as variáveis de ambiente."
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
            
        supabase_config = SupabaseConfig(
            url=supabase_url,
            key=supabase_key
        )
        
        # 1. Inicializa e testa Supabase
        logger.info("1. Testando conexão com Supabase")
        supabase = SupabaseStorage(supabase_config)
        if not await supabase.test_connection():
            error_msg = "Erro ao conectar com Supabase. Verifique as credenciais."
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        logger.info("✅ Conexão com Supabase estabelecida")
        
        # 2. Busca dados da conexão
        logger.info("1. Buscando dados da conexão")
        try:
            connection_data = await supabase.client.table('connections') \
                .select('*') \
                .eq('id', connection_id) \
                .single() \
                .execute()
                
            if connection_data.error:
                error_msg = f"Conexão não encontrada: {connection_id}"
                logger.error(error_msg)
                raise HTTPException(status_code=404, detail=error_msg)
                
            logger.info("✅ Dados da conexão encontrados")
            
        except Exception as e:
            error_msg = f"Erro ao buscar dados da conexão: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(status_code=500, detail=error_msg)
            
        connection = connection_data.data
        
        logger.info(f"🔄 Iniciando sincronização:")
        logger.info(f"  - Conexão: {connection['name']}")
        logger.info(f"  - Tipo: {connection['type']}")
        logger.info(f"  - Projeto: {connection['project_id']}")
        
        # Aguarda a conclusão da sincronização e retorna o resultado detalhado
        sync_result = await sync_connection(
            user_id=connection['user_id'],
            project_id=connection['project_id'],
            connection_id=connection_id,
            supabase_config=supabase_config,
            connection_data=connection
        )
        
        logger.info("✅ Sincronização concluída na função sync_connection")
        return sync_result # Retorna o dicionário completo retornado por sync_connection
            
    except HTTPException:
        # Re-lança exceções HTTP já formatadas
        raise
    except Exception as e:
        error_msg = f"Erro inesperado ao sincronizar conexão {connection_id}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=error_msg
        )
    finally:
        logger.info("="*80)

def start_server():
    """Inicia o servidor FastAPI."""
    # Verificar variáveis de ambiente
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_KEY"):
        logger.error("❌ Erro: SUPABASE_URL e SUPABASE_KEY são obrigatórios")
        logger.error("Configure as variáveis de ambiente antes de iniciar o servidor")
        return
    
    # Criar diretório base se não existir
    base_dir = Path('users_private')
    base_dir.mkdir(exist_ok=True)
    logger.info("✓ Diretório base verificado")
    
    logger.info("="*80)
    logger.info("🚀 Iniciando Tap Executor API")
    logger.info("-"*80)
    logger.info("URLs:")
    logger.info("  - API: http://localhost:8000")
    logger.info("  - Docs: http://localhost:8000/docs")
    logger.info("="*80)
    
    uvicorn.run(
        "tap_executor.api:app",
        host="0.0.0.0",  # Permite acesso externo
        port=8000,
        reload=True,
        log_level="debug"
    )

if __name__ == "__main__":
    start_server()