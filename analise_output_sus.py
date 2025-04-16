# analise_cnj_saude_entes_publicos.py
# -*- coding: utf-8 -*-
"""
Script secundário para análise dos dados CNJ de Saúde filtrados,
focando em processos contra ENTES PÚBLICOS. USA O MÓDULO relatorio_export.
"""

import pandas as pd
from pathlib import Path
import logging
import sys
import re
from collections import Counter
import gc

# --- NOVO: Importa o módulo de exportação ---
try:
    import relatorio_export
except ImportError:
    logging.error("Erro: Módulo 'relatorio_export.py' não encontrado.")
    logging.error("Certifique-se de que o arquivo 'relatorio_export.py' está no mesmo diretório ou no Python Path.")
    sys.exit(1) # Termina se não encontrar o módulo

# --- Configuração do Logging --- (Igual)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# --- Configuração Principal --- (Igual)
PASTA_DADOS_REGIONAIS = Path("./Output_AnaliseBR_Saude")
ARQUIVO_DADOS_CONSOLIDADO = Path("./DADOS_CNJ_FILTRADOS_SAUDE_CONSOLIDADO.csv")
COLUNA_ASSUNTO_CODIGO = 'Codigos assuntos'
COLUNA_POLO_ATIVO = 'Polo ativo'
COLUNA_NATJUR_ATIVO = 'Polo ativo - Natureza juridica'
COLUNA_POLO_PASSIVO = 'Polo passivo'
COLUNA_NATJUR_PASSIVO = 'Polo passivo - Natureza juridica'
COLUNAS_ANALISE = [COLUNA_ASSUNTO_CODIGO, COLUNA_POLO_ATIVO, COLUNA_NATJUR_ATIVO, COLUNA_POLO_PASSIVO, COLUNA_NATJUR_PASSIVO]
COLUNAS_LEITURA_MINIMA = list(set(COLUNAS_ANALISE + [COLUNA_NATJUR_PASSIVO]))
TOP_N = 10
CHUNKSIZE_ANALISE = 100000
PALAVRAS_CHAVE_ENTES_PUBLICOS = {'ORGAO PUBLICO', 'ESTADO OU DISTRITO FEDERAL', 'MUNICIPIO', 'AUTARQUIA', 'FUNDACAO PUBLICA', 'UNIAO', 'SECRETARIA', 'PROCURADORIA', 'FAZENDA', 'FEDERAL', 'ESTADUAL', 'MUNICIPAL'}

# --- NOVOS: Caminhos de Saída da Análise ---
ANALISE_SAIDA_CSV = Path("./analise_saude_entes_publicos.csv")
ANALISE_SAIDA_PDF = Path("./analise_saude_entes_publicos.pdf")


# --- Funções Auxiliares ---
# (parse_multi_valor e eh_ente_publico - Mantidas como antes)
def parse_multi_valor(valor_celula: str | float) -> list[str]:
    """Extrai valores individuais de uma célula ('{Val1, Val2, ...}')."""
    if pd.isna(valor_celula): return []
    texto_str = str(valor_celula).strip()
    if not texto_str: return []
    if texto_str.startswith('{') and texto_str.endswith('}'): texto_interno = texto_str[1:-1]
    else: texto_interno = texto_str
    valores = [item.strip() for item in re.split(r'\s*,\s*', texto_interno) if item.strip()]
    return valores

def eh_ente_publico(texto_natjur: str | float, palavras_chave: set) -> bool:
    """Verifica se alguma natureza jurídica na célula corresponde a um ente público."""
    if pd.isna(texto_natjur): return False
    lista_naturezas = parse_multi_valor(texto_natjur)
    if not lista_naturezas: return False
    for natureza in lista_naturezas:
        natureza_upper = natureza.upper()
        for chave in palavras_chave:
            if chave in natureza_upper: return True
    return False


# (analisar_frequencias_entes_publicos - Mantida como antes)
def analisar_frequencias_entes_publicos(caminho_csv: Path, colunas_analise: list, coluna_natjur_passivo: str, palavras_chave_entes_publicos: set) -> dict | None:
    """Lê um CSV, filtra por ente público no polo passivo e calcula frequências."""
    logger.info(f"Analisando (com filtro Ente Público): {caminho_csv.name}")
    if not caminho_csv.exists(): logger.error(f"Arquivo não encontrado: {caminho_csv}"); return None
    colunas_para_ler = list(set(colunas_analise + [coluna_natjur_passivo]))
    dtypes_analise = {col: 'str' for col in colunas_para_ler}
    contadores = {col: Counter() for col in colunas_analise}
    contagem_sigiloso = {col: 0 for col in colunas_analise}
    linhas_lidas_total = 0; linhas_filtradas_entes_publicos = 0
    try:
        try:
            df_peek = pd.read_csv(caminho_csv, sep=';', encoding='utf-8', nrows=1)
            colunas_presentes = df_peek.columns.tolist()
            colunas_analise_existentes = [col for col in colunas_para_ler if col in colunas_presentes]
            if not all(col in colunas_presentes for col in colunas_para_ler): logger.warning(f"Colunas ausentes em {caminho_csv.name}: {set(colunas_para_ler) - set(colunas_presentes)}. Analisando apenas existentes.")
            if not colunas_analise_existentes or coluna_natjur_passivo not in colunas_analise_existentes: logger.error(f"Colunas essenciais não encontradas em {caminho_csv.name}. Pulando."); return None
        except Exception as peek_err: logger.error(f"Erro cabeçalho {caminho_csv.name}: {peek_err}."); return None

        iterador_csv = pd.read_csv(caminho_csv, sep=';', encoding='utf-8', dtype=dtypes_analise, chunksize=CHUNKSIZE_ANALISE, usecols=colunas_analise_existentes, low_memory=False)
        for i, chunk in enumerate(iterador_csv):
            logger.debug(f"Processando chunk análise {i+1}..."); linhas_lidas_total += len(chunk)
            mascara_ente_publico = chunk[coluna_natjur_passivo].apply(eh_ente_publico, args=(palavras_chave_entes_publicos,))
            chunk_filtrado_inicial = chunk[mascara_ente_publico]
            linhas_filtradas_entes_publicos += len(chunk_filtrado_inicial)
            if chunk_filtrado_inicial.empty: del chunk, mascara_ente_publico, chunk_filtrado_inicial; gc.collect() if i%5==0 else None; continue
            for coluna in colunas_analise:
                 if coluna not in chunk_filtrado_inicial.columns: continue # Pula se coluna não foi lida
                 col_data = chunk_filtrado_inicial[coluna].dropna()
                 if not col_data.empty:
                    items_parsed = col_data.apply(parse_multi_valor)
                    for lista_items in items_parsed:
                        for item in lista_items:
                            item_strip_upper = item.strip().upper()
                            if item_strip_upper == 'SIGILOSO': contagem_sigiloso[coluna] += 1
                            elif item_strip_upper: contadores[coluna][item] += 1 # Conta se não for vazio após strip
            del chunk, mascara_ente_publico, chunk_filtrado_inicial, col_data, items_parsed; gc.collect() if i%5==0 else None
        logger.info(f"Análise {caminho_csv.name} concluída. Lidas: {linhas_lidas_total}, Com Ente Público: {linhas_filtradas_entes_publicos}")
        resultados_finais = {}
        for coluna in colunas_analise:
            if coluna not in colunas_presentes: resultados_finais[coluna] = {'contagens': pd.Series(dtype=int),'total_ocorrencias': 0,'contagem_sigiloso': 0}; continue # Se coluna nem existia no arquivo
            contador = contadores[coluna]; sigilosos = contagem_sigiloso[coluna]; total_ocorrencias_geral = sum(contador.values()) + sigilosos
            if total_ocorrencias_geral == 0: resultados_finais[coluna] = {'contagens': pd.Series(dtype=int),'total_ocorrencias': 0,'contagem_sigiloso': 0}; continue
            series_contagem = pd.Series(contador).sort_values(ascending=False)
            resultados_finais[coluna] = {'contagens': series_contagem, 'total_ocorrencias': total_ocorrencias_geral, 'contagem_sigiloso': sigilosos}
        return resultados_finais
    except Exception as e: logger.error(f"Erro inesperado ao analisar '{caminho_csv.name}': {e}", exc_info=True); return None

# --- Função exibir_resultados REMOVIDA (sua lógica agora está no módulo) ---

# --- Função Principal de Orquestração da Análise ---
def main_analise():
    """Função principal que orquestra a análise e CHAMA A EXPORTAÇÃO."""
    logger.info(">>> INICIANDO SCRIPT DE ANÁLISE (ENTES PÚBLICOS) <<<")

    resultados_gerais = {} # Dicionário para guardar todos os resultados

    # 1. Analisar o Arquivo Consolidado (Brasil)
    logger.info("--- Analisando Dados Consolidados (Brasil) - Filtro Entes Públicos ---")
    resultados_completos_brasil = analisar_frequencias_entes_publicos(
        ARQUIVO_DADOS_CONSOLIDADO,
        COLUNAS_ANALISE,
        COLUNA_NATJUR_PASSIVO,
        PALAVRAS_CHAVE_ENTES_PUBLICOS
    )
    if resultados_completos_brasil:
        resultados_gerais["Brasil Consolidado (vs Entes Públicos)"] = resultados_completos_brasil
    else:
        logger.error("Não foi possível gerar a análise consolidada do Brasil.")

    # 2. Analisar Arquivos Regionais Individualmente
    logger.info("\n--- Analisando Dados Regionais (vs Entes Públicos) ---")
    arquivos_regionais = sorted(list(PASTA_DADOS_REGIONAIS.glob("*.csv")))

    if not arquivos_regionais:
        logger.warning(f"Nenhum arquivo CSV encontrado em {PASTA_DADOS_REGIONAIS} para análise regional.")
    else:
        for caminho_csv_regional in arquivos_regionais:
            try:
                 nome_analise = caminho_csv_regional.stem.replace('dados_saude_', '')
                 titulo_contexto = f"Regional {nome_analise} (vs Entes Públicos)"
            except Exception:
                 titulo_contexto = f"Regional {caminho_csv_regional.name} (vs Entes Públicos)"

            resultados_completos_regional = analisar_frequencias_entes_publicos(
                caminho_csv_regional,
                COLUNAS_ANALISE,
                COLUNA_NATJUR_PASSIVO,
                PALAVRAS_CHAVE_ENTES_PUBLICOS
            )
            if resultados_completos_regional:
                resultados_gerais[titulo_contexto] = resultados_completos_regional
            else:
                 logger.error(f"Falha ao analisar o arquivo regional: {caminho_csv_regional.name}")
            gc.collect()

    # 3. Exportar os resultados coletados para CSV e PDF
    if resultados_gerais:
        logger.info("\n--- Exportando Resultados da Análise ---")
        relatorio_export.exportar_analises_csv(resultados_gerais, ANALISE_SAIDA_CSV, TOP_N)
        relatorio_export.exportar_analises_pdf(resultados_gerais, ANALISE_SAIDA_PDF, TOP_N)
    else:
        logger.warning("Nenhum resultado de análise foi gerado para exportar.")


    logger.info(">>> ANÁLISE (ENTES PÚBLICOS) CONCLUÍDA <<<")

# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    main_analise()