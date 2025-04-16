# -*- coding: utf-8 -*-
"""
Script secundário para análise dos dados CNJ de Saúde filtrados.

Funcionalidades:
1. Lê o arquivo CSV consolidado e os arquivos CSV regionais.
2. Analisa colunas multi-valoradas (assuntos, polos, naturezas).
3. Extrai valores individuais.
4. Calcula a frequência, incluindo totais e percentuais.
5. Apresenta os Top 10 itens + 'Outros' em formato de tabela.
"""

import pandas as pd
from pathlib import Path
import logging
import sys
import re
from collections import Counter
import gc

# --- Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
        # logging.FileHandler("analise_cnj.log", mode='w', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# --- Configuração Principal ---
PASTA_DADOS_REGIONAIS = Path("./Output_AnaliseBR_Saude") # Onde estão os CSVs regionais filtrados
ARQUIVO_DADOS_CONSOLIDADO = Path("./DADOS_CNJ_FILTRADOS_SAUDE_CONSOLIDADO.csv") # CSV consolidado
# Colunas a serem analisadas (verifique os nomes exatos no seu CSV final!)
COLUNAS_ANALISE = [
    'Codigos assuntos',
    'Polo ativo',
    'Polo ativo - Natureza juridica',
    'Polo passivo',
    'Polo passivo - Natureza juridica'
]
TOP_N = 10
CHUNKSIZE_ANALISE = 100000

# --- Funções Auxiliares ---

def parse_multi_valor(valor_celula: str | float) -> list[str]:
    """Extrai valores individuais de uma célula ('{Val1, Val2, ...}')."""
    if pd.isna(valor_celula): return []
    texto_str = str(valor_celula).strip()
    if not texto_str: return []
    if texto_str.startswith('{') and texto_str.endswith('}'):
        texto_interno = texto_str[1:-1]
    else:
        texto_interno = texto_str
    # Usa expressão regular para encontrar valores separados por vírgula,
    # tratando espaços extras e evitando itens vazios se houver vírgulas duplas.
    valores = [item.strip() for item in re.split(r'\s*,\s*', texto_interno) if item.strip()]
    return valores


# *** FUNÇÃO DE ANÁLISE MODIFICADA ***
def analisar_frequencias(caminho_csv: Path, colunas_analise: list) -> dict | None:
    """
    Lê um arquivo CSV e calcula a frequência COMPLETA dos itens para colunas especificadas.

    Args:
        caminho_csv: Path do arquivo CSV a ser analisado.
        colunas_analise: Lista de nomes das colunas a analisar.

    Returns:
        Dicionário onde cada chave é uma coluna_analise e o valor é outro dicionário
        contendo: {'contagens': pd.Series (todas contagens), 'total_ocorrencias': int}.
        Retorna None em caso de erro na leitura do arquivo.
    """
    logger.info(f"Analisando frequências em: {caminho_csv.name}")
    if not caminho_csv.exists():
        logger.error(f"Arquivo não encontrado: {caminho_csv}")
        return None

    contadores = {col: Counter() for col in colunas_analise}
    linhas_lidas_total = 0
    colunas_presentes_no_arquivo = [] # Para verificar se as colunas existem

    try:
        # Verifica as colunas realmente presentes no arquivo antes de ler tudo
        try:
            df_peek = pd.read_csv(caminho_csv, sep=';', encoding='utf-8', nrows=1)
            colunas_presentes_no_arquivo = df_peek.columns.tolist()
            # Filtra colunas_analise para incluir apenas as que existem no arquivo
            colunas_analise_existentes = [col for col in colunas_analise if col in colunas_presentes_no_arquivo]
            if len(colunas_analise_existentes) < len(colunas_analise):
                colunas_faltantes = set(colunas_analise) - set(colunas_presentes_no_arquivo)
                logger.warning(f"Colunas de análise não encontradas em {caminho_csv.name}: {colunas_faltantes}. Analisando apenas as existentes.")
            if not colunas_analise_existentes:
                 logger.error(f"Nenhuma das colunas de análise foi encontrada em {caminho_csv.name}. Pulando análise.")
                 return None # Retorna None se nenhuma coluna de análise existe
        except Exception as peek_err:
             logger.error(f"Erro ao verificar cabeçalho de {caminho_csv.name}: {peek_err}. Pulando análise.")
             return None


        dtypes_analise = {col: 'str' for col in colunas_analise_existentes} # Dtype apenas para colunas existentes

        iterador_csv = pd.read_csv(
            caminho_csv,
            sep=';',
            encoding='utf-8',
            dtype=dtypes_analise,
            chunksize=CHUNKSIZE_ANALISE,
            usecols=colunas_analise_existentes, # Usa apenas colunas existentes
            low_memory=False
        )

        for i, chunk in enumerate(iterador_csv):
            logger.debug(f"Processando chunk de análise {i+1}...")
            linhas_lidas_total += len(chunk)
            for coluna in colunas_analise_existentes: # Itera apenas sobre colunas que existem
                 # Otimização: Processa apenas colunas não vazias no chunk
                 col_data = chunk[coluna].dropna()
                 if not col_data.empty:
                     # Aplica o parse, explode e conta
                     items_exploded = col_data.apply(parse_multi_valor).explode().dropna()
                     if not items_exploded.empty:
                         contadores[coluna].update(items_exploded.tolist()) # Atualiza counter

            del chunk, col_data, items_exploded # Libera memória
            gc.collect() if i % 5 == 0 else None

        logger.info(f"Análise de {caminho_csv.name} concluída. Total de linhas lidas: {linhas_lidas_total}")

        # Processa os resultados finais
        resultados_finais = {}
        for coluna in colunas_analise_existentes: # Usa a lista de existentes
            contador = contadores[coluna]
            if not contador:
                logger.warning(f"Nenhum valor encontrado ou processado para a coluna '{coluna}' em {caminho_csv.name}")
                resultados_finais[coluna] = {
                    'contagens': pd.Series(dtype=int),
                    'total_ocorrencias': 0
                }
                continue

            series_contagem = pd.Series(contador).sort_values(ascending=False)
            total_ocorrencias = series_contagem.sum()
            resultados_finais[coluna] = {
                'contagens': series_contagem, # Retorna TODAS as contagens
                'total_ocorrencias': total_ocorrencias
            }

        return resultados_finais

    except Exception as e:
        logger.error(f"Erro inesperado ao analisar '{caminho_csv.name}': {e}", exc_info=True)
        return None


# *** FUNÇÃO DE EXIBIÇÃO MODIFICADA ***
def exibir_resultados(titulo: str, resultados_completos: dict, top_n: int):
    """
    Exibe os resultados da análise, incluindo Top N, Outros, Totais e Percentuais.
    """
    print("\n" + "=" * 90) # Ajusta largura para nova coluna
    print(f" {titulo.upper()} ".center(90, "="))
    print("=" * 90)

    if not resultados_completos:
        print("Nenhum resultado para exibir.")
        print("=" * 90 + "\n")
        return

    for coluna, dados_contagem in resultados_completos.items():
        print(f"\n--- Análise da Coluna: '{coluna}' ---")

        contagens_completas = dados_contagem['contagens']
        total_ocorrencias = dados_contagem['total_ocorrencias']
        total_itens_unicos = len(contagens_completas)

        if total_ocorrencias == 0 or contagens_completas.empty:
            print(" (Nenhum dado encontrado/processado para esta coluna)")
            print("-" * (len(coluna) + 23))
            continue

        print(f"(Total de Ocorrências Individuais: {total_ocorrencias})")
        print(f"(Total de Itens Únicos: {total_itens_unicos})")

        # Pega o Top N
        top_n_contagens = contagens_completas.head(top_n)

        # Calcula 'Outros'
        soma_top_n = top_n_contagens.sum()
        contagem_outros = total_ocorrencias - soma_top_n

        # Prepara DataFrame para exibição
        df_tabela = pd.DataFrame({
            'Item': top_n_contagens.index.astype(str), # Garante que item seja string
            'Contagem': top_n_contagens.values
        })

        # Calcula Percentual para Top N
        df_tabela['Percentual'] = (df_tabela['Contagem'] / total_ocorrencias) * 100
        df_tabela['Percentual'] = df_tabela['Percentual'].map('{:.2f}%'.format) # Formata

        # Adiciona linha 'Outros', se houver itens além do Top N
        if total_itens_unicos > top_n and contagem_outros > 0:
            percentual_outros = (contagem_outros / total_ocorrencias) * 100
            outros_row = pd.DataFrame({
                'Item': [f'Outros ({total_itens_unicos - top_n} itens)'],
                'Contagem': [contagem_outros],
                'Percentual': [f'{percentual_outros:.2f}%']
            })
            df_tabela = pd.concat([df_tabela, outros_row], ignore_index=True)

        # Adiciona linha 'TOTAL'
        total_row = pd.DataFrame({
            'Item': ['TOTAL'],
            'Contagem': [total_ocorrencias],
            'Percentual': ['100.00%']
        })
        df_tabela = pd.concat([df_tabela, total_row], ignore_index=True)


        # Exibe a tabela formatada
        print(df_tabela.to_string(index=False))
        print("-" * (len(coluna) + 23)) # Linha separadora

    print("=" * 90 + "\n")


# --- Função Principal de Orquestração da Análise (main_analise) ---
# (Chamadas para analisar_frequencias e exibir_resultados permanecem iguais)
def main_analise():
    """Função principal que orquestra a análise dos arquivos CSV."""
    logger.info(">>> INICIANDO SCRIPT DE ANÁLISE DOS DADOS CNJ DE SAÚDE <<<")

    # 1. Analisar o Arquivo Consolidado (Brasil)
    logger.info("--- Analisando Dados Consolidados (Brasil) ---")
    resultados_completos_brasil = analisar_frequencias(
        ARQUIVO_DADOS_CONSOLIDADO,
        COLUNAS_ANALISE
        # top_n não é mais passado aqui, é decidido na exibição
    )
    if resultados_completos_brasil:
        exibir_resultados("Resultados Consolidados - Brasil", resultados_completos_brasil, TOP_N)
    else:
        logger.error("Não foi possível gerar a análise consolidada do Brasil.")

    # 2. Analisar Arquivos Regionais Individualmente
    logger.info("\n--- Analisando Dados Regionais ---")
    arquivos_regionais = sorted(list(PASTA_DADOS_REGIONAIS.glob("*.csv"))) # Busca apenas CSV

    if not arquivos_regionais:
        logger.warning(f"Nenhum arquivo CSV encontrado em {PASTA_DADOS_REGIONAIS} para análise regional.")
    else:
        for caminho_csv_regional in arquivos_regionais:
            try:
                 nome_analise = caminho_csv_regional.stem.replace('dados_saude_', '')
                 titulo_analise = f"Resultados Regionais - {nome_analise}"
            except Exception:
                 titulo_analise = f"Resultados Regionais - {caminho_csv_regional.name}"

            resultados_completos_regional = analisar_frequencias(
                caminho_csv_regional,
                COLUNAS_ANALISE
            )
            if resultados_completos_regional:
                exibir_resultados(titulo_analise, resultados_completos_regional, TOP_N)
            else:
                 logger.error(f"Falha ao analisar o arquivo regional: {caminho_csv_regional.name}")
            gc.collect()

    logger.info(">>> ANÁLISE CONCLUÍDA <<<")

# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    main_analise()