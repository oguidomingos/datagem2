"""Módulo para executar taps Singer."""

import subprocess
import logging
import json
import os
from typing import List, Optional, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

def generate_catalog(config_path: str, output_path: str) -> bool:
    """
    Executa o tap-woocommerce no modo discover para gerar o catalog.json.

    Args:
        config_path: Caminho para o arquivo de configuração
        output_path: Caminho onde o catalog.json será salvo

    Returns:
        bool: True se o catalog foi gerado com sucesso
    """
    try:
        logger.info("="*60)
        logger.info("Iniciando geração do catalog.json")
        logger.info(f"Config: {config_path}")
        logger.info(f"Output: {output_path}")
        logger.info("="*60)

        # Executa o tap no modo discover
        command = ["tap-woocommerce", "--config", config_path, "--discover"]
        logger.info(f"Executando comando: {' '.join(command)}")

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        output, error = process.communicate()

        if process.returncode != 0:
            logger.error("❌ Falha ao executar discover")
            logger.error(f"Código de retorno: {process.returncode}")
            logger.error("Detalhes do erro:")
            for line in error.splitlines():
                logger.error(f"  {line}")
            return False

        # Valida o JSON de saída
        try:
            catalog = json.loads(output)
            streams = catalog.get('streams', [])
            logger.info("📊 Streams descobertos:")
            for stream in streams:
                logger.info(f"  - {stream.get('tap_stream_id')}")
        except json.JSONDecodeError:
            logger.error("❌ Saída do discover não é um JSON válido")
            logger.error(f"Conteúdo: {output[:200]}...")
            return False

        # Salva o output no arquivo catalog.json
        try:
            with open(output_path, 'w') as f:
                f.write(output)
            logger.info(f"✅ Catalog.json salvo em: {output_path}")
            logger.info(f"   Tamanho: {len(output)} bytes")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao salvar catalog.json: {str(e)}")
            return False

    except Exception as e:
        logger.error("❌ Erro inesperado na geração do catalog")
        logger.error(str(e), exc_info=True)
        return False

def run_tap_woocommerce(config_path: str, catalog_path: Optional[str] = None, state_path: Optional[str] = None) -> tuple[List[str], Optional[Dict[str, Any]]]:
    """
    Executa o tap-woocommerce com um arquivo de configuração, catalog e opcionalmente um state.

    Args:
        config_path: Caminho para o arquivo de configuração
        catalog_path: Caminho para o arquivo de catalog (opcional)
        state_path: Caminho para o arquivo de state (opcional)

    Returns:
        tuple[List[str], Optional[Dict[str, Any]]]: Lista de linhas de saída do tap e o último estado emitido.

    Raises:
        RuntimeError: Se houver erro na execução do tap
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

    try:
        # Monta o comando base
        command = ["tap-woocommerce", "--config", config_path]

        # Adiciona o catalog se fornecido
        if catalog_path:
            if not os.path.exists(catalog_path):
                raise FileNotFoundError(f"Arquivo de catalog não encontrado: {catalog_path}")
            command.extend(["--catalog", catalog_path])

        # Adiciona o state se fornecido
        if state_path and os.path.exists(state_path):
             command.extend(["--state", state_path])
             logger.info(f"Usando arquivo de estado: {state_path}")
        elif state_path and not os.path.exists(state_path):
             logger.info(f"Arquivo de estado não encontrado: {state_path}. Iniciando sincronização completa.")


        # Configura o processo
        logger.info(f"Executando comando: {' '.join(command)}")

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )

        output_lines = []
        error_lines = []
        parsed_records = 0
        last_state = None # Initialize last_state

        # Lê a saída linha por linha para evitar bloqueio
        assert process.stdout is not None
        assert process.stderr is not None

        while True:
            # Lê stdout
            output = process.stdout.readline()
            if output:
                line = output.strip()
                output_lines.append(line)
                try:
                    # Tenta parsear como JSON para log formatado
                    msg = json.loads(line)
                    msg_type = msg.get('type', 'UNKNOWN')
                    if msg_type == 'RECORD':
                        parsed_records += 1
                        stream = msg.get('stream', 'unknown')
                        if parsed_records % 100 == 0:
                            logger.info(f"Processados {parsed_records} registros ({stream})")
                    elif msg_type == 'STATE':
                        logger.info("Estado recebido do tap")
                        last_state = msg.get('value') # Capture the state value
                    elif msg_type == 'SCHEMA':
                        stream = msg.get('stream', 'unknown')
                        logger.info(f"Schema recebido para stream: {stream}")
                    else:
                        logger.debug(f"Mensagem tipo {msg_type}: {line[:200]}...")
                except json.JSONDecodeError:
                    # Se não for JSON, loga como texto
                    logger.debug(f"TAP OUTPUT: {line}")
                except Exception as e:
                    logger.warning(f"Erro ao processar linha: {str(e)}")

            # Lê stderr
            error = process.stderr.readline()
            if error:
                error_line = error.strip()
                error_lines.append(error_line)
                logger.error(f"TAP ERROR: {error_line}")

            # Verifica se o processo terminou
            if process.poll() is not None:
                logger.info("Processo do tap finalizado")
                break

        # Lê qualquer saída remanescente
        output, error = process.communicate()
        if output:
            output_lines.extend(output.splitlines())
        if error:
            error_lines.extend(error.splitlines())

        # Verifica o código de retorno
        if process.returncode != 0:
            error_msg = "\n".join(error_lines)
            logger.error(f"Tap falhou com código {process.returncode}")
            logger.error("Detalhes do erro:")
            for line in error_lines:
                logger.error(f"  {line}")
            raise RuntimeError(
                f"Tap falhou com código {process.returncode}: {error_msg}"
            )

        logger.info(f"✅ Tap executado com sucesso. Processados {parsed_records} registros.")

        return output_lines, last_state # Return output_lines and last_state

    except Exception as e:
        logger.error(f"Erro ao executar tap-woocommerce: {str(e)}")
        raise RuntimeError(f"Falha ao executar tap: {str(e)}")

def validate_tap_config(config_path: str) -> bool:
    """
    Valida o arquivo de configuração do tap.

    Args:
        config_path: Caminho para o arquivo de configuração

    Returns:
        bool: True se a configuração é válida
    """
    logger.info("="*60)
    logger.info(f"Validando configuração: {config_path}")

    if not os.path.exists(config_path):
        logger.error("❌ Arquivo de configuração não encontrado")
        return False

    try:
        with open(config_path) as f:
            file_content = f.read()

        try:
            config = json.loads(file_content)
        except json.JSONDecodeError as e:
            logger.error("❌ JSON inválido no arquivo de configuração")
            logger.error(f"Erro: {str(e)}")
            logger.error(f"Posição: linha {e.lineno}, coluna {e.colno}")
            logger.error(f"Conteúdo próximo ao erro: {file_content[max(0, e.pos-50):e.pos+50]}")
            return False

        # Valida campos obrigatórios
        required_fields = ["site_url", "consumer_key", "consumer_secret", "start_date"]
        missing_fields = [field for field in required_fields if field not in config]

        if missing_fields:
            logger.error("❌ Campos obrigatórios faltando:")
            for field in missing_fields:
                logger.error(f"  - {field}")
            return False

        # Valida formato dos campos
        if not config["site_url"].startswith(("http://", "https://")):
            logger.error("❌ site_url deve começar com http:// ou https://")
            return False

        if not config["start_date"].endswith("Z"):
            logger.warning("⚠️ start_date deve estar em formato UTC (terminando com Z)")
            config["start_date"] += "Z"
            logger.info(f"✓ start_date ajustado para: {config['start_date']}")

            # Salva configuração atualizada
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)

        logger.info("✅ Configuração validada com sucesso")
        logger.info(f"  - URL: {config['site_url']}")
        logger.info(f"  - Data inicial: {config['start_date']}")
        return True

    except Exception as e:
        logger.error("❌ Erro ao validar configuração")
        logger.error(str(e), exc_info=True)
        return False