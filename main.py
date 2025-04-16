# -*- coding: utf-8 -*-
"""
Script para processar dados de estatísticas judiciais do CNJ por região.

Funcionalidades:
1. Descompacta arquivos ZIP de dados de cada região.
2. Lê arquivos CSV em chunks.
3. Filtra processos cujo campo de assunto contenha (mesmo entre múltiplos valores)
   pelo menos um código CNJ relacionado à Saúde Pública (excluindo Saúde Suplementar).
4. Seleciona colunas relevantes.
5. Exporta um CSV filtrado para cada região.
6. Consolida todos os CSVs regionais filtrados em um único arquivo final.
"""

import pandas as pd
from pathlib import Path
import zipfile
import logging
import sys
import re
import gc

# --- Configuração do Logging --- (Igual)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# --- Configuração Principal ---
PASTA_BASE_DADOS = Path("./AnaliseBR")
REGIOES = ['NE', 'NO', 'SE', 'SU', 'CO', 'TRFs']
PASTA_SAIDA_REGIONAL = Path("./Output_AnaliseBR_Saude")
ARQUIVO_SAIDA_CONSOLIDADO = Path("./DADOS_CNJ_FILTRADOS_SAUDE_CONSOLIDADO.csv")
CHUNKSIZE_LEITURA = 50000

# --- CONFIGURAÇÃO CRÍTICA: Assuntos e Colunas ---
# ==============================================================================
# ATENÇÃO: AJUSTE MANUAL NECESSÁRIO AQUI! (Mesmo de antes)
# Lista de códigos numéricos de Saúde Pública (sem Saúde Suplementar)
CODIGOS_SAUDE_RELEVANTES = {
    12480, 12521, 12520, 12507, 12508,
    12509, 12510, 12481, 12485, 12498,
    12497, 12499, 12484, 12496, 12492,
    12495, 12494, 12493, 12483, 12505,
    12506, 12511, 12518, 12512, 12513,
    12514, 12515, 12516, 12517, 14759,
    12491, 12501, 12502, 12503, 12500,
    12504, 12519, 12482, 12486, 12490,
    12487, 12488, 12489, 14760
    # Adicione todos os outros códigos relevantes aqui
}

# ATENÇÃO: AJUSTE MANUAL NECESSÁRIO AQUI! (Mesmo de antes)
# Nomes EXATOS das colunas no CSV do CNJ
COLUNA_ASSUNTO_CODIGO = 'Codigos assuntos' # Ajuste o nome real! (Ex: pode ser 'Assunto Código')
COLUNAS_RELEVANTES = [
    'Tribunal',             # Exemplo - ajuste o nome real
    'Processo',             # Exemplo - ajuste o nome real
    'Ano',                # Exemplo - ajuste o nome real
    COLUNA_ASSUNTO_CODIGO,         # Mantém a coluna original com múltiplos assuntos
    'Polo ativo',             # Exemplo - ajuste o nome real (pode ter múltiplos também?)
    'Polo ativo - Natureza juridica',# Exemplo - ajuste o nome real (pode ter múltiplos também?)
    'Polo passivo',           # Exemplo - ajuste o nome real (pode ter múltiplos também?)
    'Polo passivo - Natureza juridica' # Exemplo - ajuste o nome real (pode ter múltiplos também?)
]
if COLUNA_ASSUNTO_CODIGO not in COLUNAS_RELEVANTES:
     COLUNAS_RELEVANTES.append(COLUNA_ASSUNTO_CODIGO)
# ==============================================================================
# Garante que a coluna de assunto está na lista (já estava)
# ==============================================================================

# --- Definição das Funções Auxiliares e Principais ---

# --- Funções Auxiliares ---
# (encontrar_zip, descompactar_e_encontrar_csv, verificar_assuntos - Mantidas como antes)
def encontrar_zip(pasta_regiao: Path) -> Path | None:
    """Encontra o primeiro arquivo .zip dentro da pasta da região."""
    # Esta função agora é menos relevante se processarmos todos os zips no loop main
    # Poderíamos até removê-la e usar pasta_regiao.glob('*.zip') direto no main.
    # Mas vamos mantê-la por enquanto, caso seja útil para encontrar um zip específico se necessário.
    logger.debug(f"Procurando arquivo ZIP em: {pasta_regiao}")
    try:
        zip_files = list(pasta_regiao.glob('*.zip'))
        if not zip_files: logger.warning(f"Nenhum .zip encontrado em {pasta_regiao}."); return None
        if len(zip_files) > 1: logger.warning(f"Múltiplos .zip encontrados em {pasta_regiao}. Função encontrar_zip pegará o primeiro: {zip_files[0].name}")
        logger.debug(f"Arquivo ZIP encontrado (pela função encontrar_zip): {zip_files[0].name}")
        return zip_files[0]
    except Exception as e: logger.error(f"Erro ao procurar ZIP em {pasta_regiao}: {e}"); return None

def descompactar_e_encontrar_csv(caminho_zip: Path, pasta_destino_csv: Path) -> list[Path]:
    """
    Descompacta um arquivo ZIP e retorna uma LISTA de todos os arquivos .csv encontrados.
    """
    logger.info(f"Descompactando e procurando CSVs em: {caminho_zip.name}")
    pasta_extracao = caminho_zip.parent
    arquivos_csv_extraidos = []
    try:
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            nomes_no_zip = zip_ref.namelist()
            csvs_no_zip = [nome for nome in nomes_no_zip if nome.lower().endswith('.csv')]

            if not csvs_no_zip:
                 logger.warning(f"Nenhum arquivo .csv encontrado diretamente em {caminho_zip.name}. Tentando extrair tudo...")
                 zip_ref.extractall(path=pasta_extracao)
                 # Procura recursivamente por CSVs após extrair tudo
                 arquivos_csv_extraidos = list(pasta_extracao.rglob(f'**/*.csv')) # Procura em subpastas também
                 if not arquivos_csv_extraidos:
                     logger.error(f"Nenhum CSV encontrado mesmo após extração completa de {caminho_zip.name}")
                     return [] # Retorna lista vazia
                 else:
                     logger.info(f"CSVs encontrados após extração completa: {[p.name for p in arquivos_csv_extraidos]}")
                     return arquivos_csv_extraidos
            else:
                # Extrai apenas os arquivos CSV encontrados
                for nome_csv in csvs_no_zip:
                    try:
                        zip_ref.extract(nome_csv, path=pasta_extracao)
                        caminho_csv_extraido = pasta_extracao / nome_csv
                        arquivos_csv_extraidos.append(caminho_csv_extraido)
                        logger.info(f"CSV extraído para: {caminho_csv_extraido}")
                    except Exception as extract_err:
                        logger.error(f"Erro ao extrair {nome_csv} de {caminho_zip.name}: {extract_err}")
                return arquivos_csv_extraidos

    except zipfile.BadZipFile: logger.error(f"Erro: {caminho_zip.name} não é ZIP válido."); return []
    except Exception as e: logger.error(f"Erro ao descompactar/encontrar CSV em {caminho_zip.name}: {e}"); return []


def verificar_assuntos(texto_assuntos: str | float, codigos_relevantes: set) -> bool:
    """Verifica se algum código relevante está na string de assuntos."""
    if pd.isna(texto_assuntos): return False
    try:
        texto_assuntos_str = str(texto_assuntos)
        codigos_extraidos_str = re.findall(r'\d+', texto_assuntos_str)
        if not codigos_extraidos_str: return False
        codigos_extraidos_int = set()
        for codigo_str in codigos_extraidos_str:
            try: codigos_extraidos_int.add(int(codigo_str))
            except ValueError: continue
        return not codigos_extraidos_int.isdisjoint(codigos_relevantes)
    except Exception as e: logger.debug(f"Erro ao processar assunto '{texto_assuntos}': {e}"); return False


# --- Função de Filtragem Revisada ---
def filtrar_csv_por_assunto(
    caminho_csv: Path,
    caminho_saida_csv: Path,
    codigos_assunto_relevantes: set,
    coluna_assunto: str,
    colunas_manter: list, # Nomes como definidos pelo usuário
    chunksize: int
):
    """Lê CSV em chunks, verifica cabeçalho, filtra por assuntos e salva."""
    logger.info(f"Iniciando processamento de: {caminho_csv.name}")
    logger.info(f"Salvando resultado filtrado em: {caminho_saida_csv.name}")

    primeiro_chunk = True
    total_linhas_lidas = 0
    total_linhas_filtradas = 0
    caminho_saida_csv.parent.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Ler APENAS o cabeçalho para verificar os nomes reais das colunas
        try:
            # Tenta ler com separador e encoding mais prováveis
            df_header = pd.read_csv(caminho_csv, sep=';', encoding='utf-8', nrows=0, low_memory=False)
            colunas_reais_csv = [col.strip() for col in df_header.columns] # Remove espaços extras
            logger.info(f"Colunas reais encontradas em '{caminho_csv.name}': {colunas_reais_csv}")
        except Exception as header_err:
            logger.error(f"Falha ao ler cabeçalho de '{caminho_csv.name}'. Verifique separador (;) e encoding (utf-8). Erro: {header_err}")
            return # Não pode continuar sem o cabeçalho

        # 2. Validar as colunas solicitadas contra as colunas reais
        colunas_manter_ajustado = []
        colunas_faltantes = []
        mapa_nomes = {col.strip(): col.strip() for col in colunas_reais_csv} # Mapeamento simples inicial

        # Verifica se todas as colunas relevantes existem (ignorando espaços extras)
        for col_solicitada in colunas_manter:
            if col_solicitada in mapa_nomes:
                colunas_manter_ajustado.append(col_solicitada)
            else:
                colunas_faltantes.append(col_solicitada)

        if colunas_faltantes:
            logger.error(f"ERRO DE COLUNA em '{caminho_csv.name}'!")
            logger.error(f"Colunas solicitadas em COLUNAS_RELEVANTES não encontradas no CSV: {colunas_faltantes}")
            logger.error(f"Colunas disponíveis no CSV (sem espaços extras): {colunas_reais_csv}")
            logger.error("Ajuste a lista COLUNAS_RELEVANTES no script e tente novamente.")
            return # Interrompe o processamento deste arquivo

        # Verifica a coluna de assunto separadamente
        if coluna_assunto not in mapa_nomes:
            logger.error(f"ERRO: Coluna de assunto '{coluna_assunto}' definida em COLUNA_ASSUNTO_CODIGO não encontrada no CSV '{caminho_csv.name}'.")
            logger.error(f"Colunas disponíveis: {colunas_reais_csv}")
            return

        # Colunas que realmente precisamos ler (filtro + manter)
        colunas_para_ler = list(set([coluna_assunto] + colunas_manter_ajustado))

        # 3. Definir dtypes (ler tudo como string é mais seguro inicialmente)
        dtypes_leitura = {col: 'str' for col in colunas_para_ler}

        # 4. Ler o arquivo em Chunks com colunas validadas
        logger.info(f"Lendo e filtrando '{caminho_csv.name}'...")
        iterador_csv = pd.read_csv(
            caminho_csv,
            sep=';',            # ASSUMINDO ; - AJUSTE SE NECESSÁRIO
            encoding='utf-8', # ASSUMINDO Latin-1 - AJUSTE SE NECESSÁRIO
            usecols=colunas_para_ler, # Usa a lista validada
            dtype=dtypes_leitura,     # Lê como string
            chunksize=chunksize,
            low_memory=False,
            on_bad_lines='warn'
        )

        # 5. Processar Chunks
        for i, chunk in enumerate(iterador_csv):
            total_linhas_lidas += len(chunk)
            logger.debug(f"Processando chunk {i+1}...")

            condicao = chunk[coluna_assunto].apply(
                verificar_assuntos,
                args=(codigos_assunto_relevantes,)
            )

            # Seleciona apenas as colunas finais desejadas APÓS o filtro
            chunk_filtrado = chunk.loc[condicao, colunas_manter_ajustado].copy()

            if not chunk_filtrado.empty:
                total_linhas_filtradas += len(chunk_filtrado)
                mode = 'w' if primeiro_chunk else 'a'
                header = primeiro_chunk
                chunk_filtrado.to_csv(
                    caminho_saida_csv, sep=';', encoding='utf-8', index=False,
                    mode=mode, header=header
                )
                primeiro_chunk = False

            del chunk, chunk_filtrado, condicao
            if i % 5 == 0: gc.collect()

        logger.info(f"Filtragem de {caminho_csv.name} concluída.")
        logger.info(f"Total de linhas lidas: {total_linhas_lidas}")
        logger.info(f"Total de linhas filtradas (Saúde Pública): {total_linhas_filtradas}")
        # (Remoção de arquivo vazio - mantida como antes)
        if total_linhas_filtradas == 0 and caminho_saida_csv.exists():
            if caminho_saida_csv.stat().st_size < 100 : # Se for muito pequeno (provavelmente só cabeçalho)
                 try: caminho_saida_csv.unlink(); logger.info(f"Arquivo vazio/cabeçalho removido: {caminho_saida_csv.name}")
                 except OSError as e: logger.warning(f"Não remover arquivo vazio {caminho_saida_csv.name}: {e}")

    except Exception as e:
        logger.error(f"Erro inesperado durante leitura/filtragem de '{caminho_csv.name}': {e}", exc_info=True)


# (Função consolidar_csvs_regionais - Mantida como antes)
def consolidar_csvs_regionais(pasta_csvs_regionais: Path, arquivo_saida_consolidado: Path, chunksize: int):
    """Lê todos os CSVs filtrados por região e os consolida em um único arquivo."""
    logger.info("--- Iniciando Consolidação dos CSVs Regionais Filtrados ---")
    logger.info(f"Procurando arquivos CSV em: {pasta_csvs_regionais}")
    logger.info(f"Salvando consolidado em: {arquivo_saida_consolidado}")
    arquivos_csv_regionais = list(pasta_csvs_regionais.glob("*.csv"))
    if not arquivos_csv_regionais: logger.warning("Nenhum CSV regional encontrado."); return
    primeiro_arquivo = True; total_linhas_consolidadas = 0
    arquivo_saida_consolidado.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(arquivo_saida_consolidado, 'w', encoding='utf-8', newline='') as f_out:
            for caminho_csv_regional in arquivos_csv_regionais:
                logger.info(f"Processando arquivo regional: {caminho_csv_regional.name}")
                try:
                    iterador_regional = pd.read_csv(caminho_csv_regional, sep=';', encoding='utf-8', chunksize=chunksize, low_memory=False, dtype=str) # Ler como string por segurança
                    for i, chunk in enumerate(iterador_regional):
                        if chunk.empty: continue
                        total_linhas_consolidadas += len(chunk)
                        header = primeiro_arquivo and i == 0
                        chunk.to_csv(f_out, sep=';', encoding='utf-8', index=False, header=header)
                        primeiro_arquivo = False
                        del chunk; gc.collect() if i % 10 == 0 else None
                    del iterador_regional; gc.collect()
                except Exception as e_inner: logger.error(f"Erro ao processar regional '{caminho_csv_regional.name}': {e_inner}", exc_info=True)
        logger.info(f"Consolidação concluída. Total de {total_linhas_consolidadas} linhas salvas em {arquivo_saida_consolidado.name}")
    except Exception as e_outer: logger.error(f"Erro inesperado na consolidação: {e_outer}", exc_info=True)


# --- Função Main Revisada ---
def main():
    """Função principal que orquestra a descompactação, filtragem e consolidação."""
    logger.info(">>> INICIANDO PROCESSAMENTO CNJ - FILTRO SAÚDE PÚBLICA <<<")
    PASTA_SAIDA_REGIONAL.mkdir(parents=True, exist_ok=True)

    # 1. Processa cada região
    for regiao in REGIOES:
        logger.info(f"--- Processando Região: {regiao} ---")
        pasta_regiao_atual = PASTA_BASE_DADOS / regiao

        if not pasta_regiao_atual.is_dir():
            logger.warning(f"Pasta da região {regiao} não encontrada. Pulando.")
            continue

        # *** NOVO: Itera sobre TODOS os arquivos ZIP na pasta ***
        arquivos_zip_na_regiao = list(pasta_regiao_atual.glob('*.zip'))
        if not arquivos_zip_na_regiao:
            logger.warning(f"Nenhum arquivo ZIP encontrado em {pasta_regiao_atual}. Pulando região.")
            continue

        for caminho_zip in arquivos_zip_na_regiao:
            logger.info(f"Processando arquivo ZIP: {caminho_zip.name}")

            # Descompacta o ZIP e encontra TODOS os CSVs dentro dele
            lista_csvs_extraidos = descompactar_e_encontrar_csv(caminho_zip, pasta_regiao_atual)

            if not lista_csvs_extraidos:
                logger.error(f"Não foi possível encontrar ou extrair CSVs do ZIP {caminho_zip.name}. Pulando este ZIP.")
                continue

            # *** NOVO: Itera sobre TODOS os CSVs extraídos de um ZIP ***
            for caminho_csv_original in lista_csvs_extraidos:
                logger.info(f"Processando arquivo CSV: {caminho_csv_original.name}")

                # Define nome do arquivo de saída (inclui nome original para evitar colisão)
                nome_saida = f"dados_saude_{regiao}_{caminho_csv_original.stem}.csv" # Usa o nome do csv sem extensão
                arquivo_saida_regional = PASTA_SAIDA_REGIONAL / nome_saida

                # Filtra o CSV pelos códigos de assunto
                filtrar_csv_por_assunto(
                    caminho_csv_original,
                    arquivo_saida_regional,
                    CODIGOS_SAUDE_RELEVANTES,
                    COLUNA_ASSUNTO_CODIGO,
                    COLUNAS_RELEVANTES,
                    CHUNKSIZE_LEITURA
                )
                # Opcional: Remover CSV original após processamento bem-sucedido
                # try:
                #     caminho_csv_original.unlink()
                #     logger.info(f"CSV original removido: {caminho_csv_original.name}")
                # except OSError as e:
                #     logger.warning(f"Não remover CSV original {caminho_csv_original.name}: {e}")

            gc.collect() # Tenta limpar memória entre ZIPs

    # 2. Consolida os arquivos regionais filtrados
    consolidar_csvs_regionais(PASTA_SAIDA_REGIONAL, ARQUIVO_SAIDA_CONSOLIDADO, CHUNKSIZE_LEITURA)

    logger.info(">>> PROCESSAMENTO CONCLUÍDO <<<")


# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    main()