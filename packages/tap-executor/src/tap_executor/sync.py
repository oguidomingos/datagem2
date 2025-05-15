"""Módulo principal de sincronização."""

import logging
import json
import time
import os
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
from pprint import pformat

from .storage import SupabaseStorage, SupabaseConfig
from .tap_runner import run_tap_woocommerce, validate_tap_config, generate_catalog

logger = logging.getLogger(__name__)

CHUNK_SIZE = 500  # Número de registros por inserção em lote

def process_tap_output(
    output_lines: List[str],
    user_id: str,
    project_id: str,
    connection_id: str
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Processa as linhas de saída do tap e retorna registros para inserção e contagem por stream.

    Args:
        output_lines: Linhas de saída do tap em formato JSON
        user_id: ID do usuário
        project_id: ID do projeto
        connection_id: ID da conexão

    Returns:
        Tuple[List[Dict[str, Any]], Dict[str, int]]: Lista de registros para inserção no Supabase e dicionário com a contagem de registros por stream.
    """
    records = []
    stream_counts = {}

    logger.info(f"Iniciando processamento de {len(output_lines)} linhas")

    for i, line in enumerate(output_lines):
        try:
            logger.debug(f"Processando linha {i+1}: {line[:200]}...")
            message = json.loads(line)

            # Log do tipo da mensagem
            msg_type = message.get('type')
            logger.debug(f"Tipo da mensagem: {msg_type}")

            if msg_type == 'RECORD':
                stream = message.get('stream')
                record = message.get('record', {})

                logger.debug(f"Encontrado registro do stream '{stream}': {json.dumps(record)[:200]}...")

                # Contabiliza registros por stream
                stream_counts[stream] = stream_counts.get(stream, 0) + 1

                # Prepara registro para inserção
                new_record = {
                    'user_id': user_id,
                    'project_id': project_id,
                    'connection_id': connection_id,
                    'stream': stream,
                    'record': record
                }
                records.append(new_record)

                if len(records) % 100 == 0:
                    logger.info(f"Processados {len(records)} registros até agora")

        except json.JSONDecodeError:
            logger.warning(f"Ignorando linha inválida: {line[:100]}...")
        except Exception as e:
            logger.error(f"Erro ao processar linha: {str(e)}", exc_info=True)

    # Loga contagem por stream
    logger.info("Contagem de registros por stream:")
    for stream, count in stream_counts.items():
        logger.info(f"  - {stream}: {count} registros")

    if not records:
        logger.warning("⚠️ ALERTA: Nenhum registro foi coletado do output do tap!")

    return records, stream_counts

def insert_records_batch(
    records: List[Dict[str, Any]],
    supabase_client: Any
) -> bool:
    """
    Insere registros em lote no Supabase.

    Args:
        records: Lista de registros para inserir
        supabase_client: Cliente do Supabase

    Returns:
        bool: True se sucesso, False se erro
    """
    try:
        total_inserted = 0

        # Verifica configuração do cliente
        logger.info(f"Supabase URL: {supabase_client.supabase_url}")
        logger.info("Verificando autenticação...")

        # Testa inserção com um registro (opcional, pode ser removido em produção)
        # logger.info("Tentando inserção de teste:")
        # test_record = {
        #     "user_id": "test_user",
        #     "project_id": "test_project",
        #     "connection_id": "test_connection_id",
        #     "stream": "test_stream",
        #     "record": {"test": True}
        # }
        # try:
        #     test_result = supabase_client.table("raw_connection_data").insert([test_record]).execute()
        #     logger.info(f"Resultado do teste: {pformat(test_result)}")
        # except Exception as e:
        #     logger.error(f"Erro no teste de inserção: {str(e)}", exc_info=True)
        #     # Não levantamos exceção aqui para não parar a sincronização real
        #     logger.warning("Teste de inserção falhou, continuando com a inserção real...")


        # Insere registros reais em lotes
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i:i + CHUNK_SIZE]
            chunk_size = len(chunk)

            logger.info(f"Inserindo lote de {chunk_size} registros:")
            # logger.info(f"Primeiro registro do lote: {pformat(chunk[0])}") # Pode ser muito verboso

            try:
                result = supabase_client.table('raw_connection_data').insert(chunk).execute()
                # logger.info(f"Resposta do Supabase: {pformat(result)}") # Pode ser muito verboso
                total_inserted += chunk_size
                logger.info(f"Inseridos {chunk_size} registros (total: {total_inserted})")
            except Exception as e:
                logger.error(f"❌ Erro ao inserir lote: {str(e)}", exc_info=True)
                # Decide se quer parar a sincronização ou continuar
                # raise # Descomente para parar em caso de erro de lote
                pass # Continue mesmo com erro em um lote

        logger.info(f"Inserção concluída. Total: {total_inserted} registros")
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao inserir registros: {str(e)}", exc_info=True)
        return False

def sync_connection(
    user_id: str,
    project_id: str,
    connection_id: str,
    supabase_config: SupabaseConfig,
    connection_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Executa a sincronização completa de uma conexão WooCommerce.

    Args:
        user_id: ID do usuário
        project_id: ID do projeto
        connection_id: ID da conexão
        supabase_config: Configuração do Supabase
        connection_data: Dados da conexão do Supabase

    Returns:
        Dict[str, Any]: Resposta JSON com o status da sincronização e contagem por stream.

    Raises:
        Exception: Se houver erro na sincronização.
    """
    start_time = time.time()
    logger.info("="*80)
    logger.info(f"Iniciando sincronização completa:")
    logger.info(f"  - User ID: {user_id}")
    logger.info(f"  - Project ID: {project_id}")
    logger.info(f"  - Connection ID: {connection_id}")
    logger.info("="*80)

    def log_step(step: str):
        logger.info("-"*40)
        logger.info(f"ETAPA: {step}")
        logger.info("-"*40)

    try:
        # 1. Verifica configuração do Supabase
        log_step("1. Verificando Supabase")
        logger.info(f"  - URL: {supabase_config.url}")
        logger.info(f"  - Key type: {'service_role' if 'service_role' in supabase_config.key else 'anon'}")

        # 2. Prepara diretórios e caminhos de arquivo
        log_step("2. Preparando ambiente")
        connection_type = connection_data['type']
        base_path = Path(
            "users_private",
            user_id,
            project_id,
            "connections",
            connection_type
        )
        config_path = base_path / "config.json"
        catalog_path = base_path / "catalog.json"
        state_path = base_path / "state.json" # Caminho para o arquivo de estado

        os.makedirs(base_path, exist_ok=True)
        logger.info(f"✅ Diretórios criados/verificados em: {base_path}")

        # 3. Cria/atualiza config.json
        log_step("3. Criando arquivo de configuração")
        with open(config_path, 'w') as f:
            json.dump(connection_data['config'], f, indent=2)
        logger.info(f"✅ Config salvo em: {config_path}")

        # 4. Valida configuração
        log_step("4. Validando configuração")
        if not validate_tap_config(str(config_path)): # Passa como string
            logger.error("❌ Configuração inválida")
            raise ValueError(f"Configuração inválida: {config_path}")
        logger.info("✅ Configuração validada com sucesso")

        # 5. Gera/atualiza catalog.json e salva no Supabase
        log_step("5. Gerando e salvando catalog")
        catalog_generated = False
        if not os.path.exists(catalog_path):
            logger.info("⚙️ Catalog.json não encontrado localmente. Gerando...")
            if not generate_catalog(str(config_path), str(catalog_path)): # Passa como string
                logger.error("❌ Falha ao gerar catalog.json")
                raise RuntimeError("Falha ao gerar catalog.json")
            logger.info("✅ Catalog.json gerado localmente com sucesso")
            catalog_generated = True
        else:
            logger.info("✅ Catalog.json encontrado localmente")

        # Lê o conteúdo do catalog.json gerado/existente
        try:
            with open(catalog_path, 'r') as f:
                catalog_data = json.load(f)
            logger.info("✅ Catalog.json lido com sucesso")
        except Exception as e:
            logger.error(f"❌ Erro ao ler catalog.json: {str(e)}")
            raise RuntimeError(f"Falha ao ler catalog.json: {catalog_path}")

        # Salva o catalog no Supabase
        try:
            supabase = SupabaseStorage(supabase_config)
            update_result = supabase.client.table('connections') \
                .update({'catalog': catalog_data}) \
                .eq('id', connection_id) \
                .execute()

            if update_result.error:
                logger.error(f"❌ Erro ao salvar catalog no Supabase: {update_result.error}")
                raise RuntimeError(f"Falha ao salvar catalog no Supabase: {update_result.error.message}")

            logger.info("✅ Catalog salvo no Supabase")
        except Exception as e:
            logger.error(f"❌ Erro inesperado ao salvar catalog no Supabase: {str(e)}", exc_info=True)
            raise RuntimeError(f"Falha inesperada ao salvar catalog no Supabase: {str(e)}")


        # 6. Executa o tap com catalog e state
        log_step("6. Executando tap com catalog e state")
        logger.info(f"🚀 Iniciando extração de dados via tap-{connection_type}")

        # Verifica se existe um arquivo de estado anterior
        initial_state = None
        if os.path.exists(state_path):
            try:
                with open(state_path, 'r') as f:
                    initial_state = json.load(f)
                logger.info(f"✅ Arquivo de estado anterior encontrado e lido: {state_path}")
                # logger.debug(f"Conteúdo do estado inicial: {pformat(initial_state)}") # Pode ser muito verboso
            except Exception as e:
                logger.warning(f"⚠️ Erro ao ler arquivo de estado anterior {state_path}: {str(e)}. Prosseguindo sem estado.")
                initial_state = None # Garante que não usamos um estado inválido

        output_lines, last_state = run_tap_woocommerce(str(config_path), str(catalog_path), str(state_path)) # Passa como string
        logger.info(f"✅ Tap concluído. {len(output_lines)} linhas de saída. Último estado recebido: {'Sim' if last_state else 'Não'}")

        # 7. Processa registros
        log_step("7. Processando registros")
        records, stream_counts = process_tap_output(output_lines, user_id, project_id, connection_id)
        logger.info(f"✅ Processados {len(records)} registros no total")

        # 8. Insere no Supabase
        log_step("8. Persistindo dados")
        if records:
            logger.info("🔄 Iniciando inserção em raw_connection_data")
            supabase = SupabaseStorage(supabase_config) # Re-inicializa para garantir cliente correto
            insert_success = insert_records_batch(records, supabase.client)
            if not insert_success:
                 logger.warning("⚠️ ALERTA: Falha na inserção de alguns lotes de registros.")
            logger.info("✅ Inserção de dados concluída")
        else:
            logger.info("⏩ Nenhum registro para inserir. Pulando etapa de persistência.")
            insert_success = True # Considera sucesso se não há registros para inserir


        # 9. Salva o último estado
        log_step("9. Salvando último estado")
        if last_state:
            try:
                with open(state_path, 'w') as f:
                    json.dump(last_state, f, indent=2)
                logger.info(f"✅ Último estado salvo em: {state_path}")
            except Exception as e:
                logger.error(f"❌ Erro ao salvar arquivo de estado {state_path}: {str(e)}")
                # Decide se quer falhar a sincronização ou apenas logar o erro
                # raise # Descomente para falhar se não conseguir salvar o estado
                pass # Continue mesmo se não conseguir salvar o estado
        else:
            logger.info("⏩ Nenhum estado final recebido do tap. Arquivo de estado não atualizado.")


        # Loga tempo total
        elapsed_time = time.time() - start_time
        logger.info(f"⏱️ Sincronização concluída em {elapsed_time:.2f} segundos")

        # Retorna resposta JSON
        return {
          "message": "Sync concluído com sucesso",
          "streams": stream_counts,
          "total_records_processed": len(records),
          "elapsed_time_seconds": round(elapsed_time, 2)
        }

    except Exception as e:
        logger.error(f"❌ Erro na sincronização: {str(e)}", exc_info=True)
        # Em caso de erro, você pode querer retornar um formato de erro específico
        # ou re-levantar a exceção para ser tratada pela API (como já está configurado)
        raise # Re-lança a exceção para ser tratada pela rota da API