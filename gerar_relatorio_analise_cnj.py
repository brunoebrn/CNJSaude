# -*- coding: utf-8 -*-
"""
Script para análise detalhada dos dados CNJ de Saúde filtrados.

Funcionalidades:
1. Lê arquivos CSV regionais e o consolidado (filtrados por assunto saúde).
2. Realiza duas análises de frequência para cada arquivo:
    a) Análise Geral: Contagem em todas as linhas.
    b) Análise Entes Públicos: Contagem apenas em linhas onde um ente público
       figura no Polo Passivo.
3. Analisa as colunas: Assuntos, Polo Ativo/Passivo e Naturezas Jurídicas.
4. Extrai valores individuais de células multi-valoradas.
5. Calcula Top 10, 'Outros', 'Sigiloso' e percentuais.
6. Exporta TODOS os resultados para UM arquivo CSV e UM arquivo PDF formatados
   na pasta 'Output_reports'.
"""

import pandas as pd
from pathlib import Path
import zipfile
import logging
import sys
import re
from collections import Counter
import gc
from fpdf import FPDF # <-- CORRIGIDO: Importa apenas FPDF

# --- Configuração do Logging ---
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Handler para console
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)
# Handler para arquivo (opcional)
try:
    # Garante que a pasta de logs exista (ou esteja no mesmo nível)
    Path("./").mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler("relatorio_analise_cnj.log", mode='w', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
except Exception as e:
    logger.warning(f"Não foi possível criar arquivo de log: {e}")


# --- Configuração Principal ---
PASTA_DADOS_REGIONAIS = Path("./Output_AnaliseBR_Saude") # ENTRADA: Pasta dos CSVs regionais já filtrados por assunto
ARQUIVO_DADOS_CONSOLIDADO = Path("./DADOS_CNJ_FILTRADOS_SAUDE_CONSOLIDADO.csv") # ENTRADA: Consolidado filtrado por assunto
PASTA_SAIDA_RELATORIOS = Path("./Output_reports") # <-- PASTA DE SAÍDA AJUSTADA
NOME_BASE_RELATORIO = "analise_saude_cnj" # Arquivos serão analise_saude_cnj.csv e .pdf

# Colunas a serem lidas e analisadas (Nomes EXATOS do CSV de entrada!)
COLUNA_ASSUNTO_CODIGO = 'Codigos assuntos'
COLUNA_POLO_ATIVO = 'Polo ativo'
COLUNA_NATJUR_ATIVO = 'Polo ativo - Natureza juridica'
COLUNA_POLO_PASSIVO = 'Polo passivo'
COLUNA_NATJUR_PASSIVO = 'Polo passivo - Natureza juridica' # Usada para filtro de Entes Públicos

COLUNAS_ANALISE = [COLUNA_ASSUNTO_CODIGO, COLUNA_POLO_ATIVO, COLUNA_NATJUR_ATIVO, COLUNA_POLO_PASSIVO, COLUNA_NATJUR_PASSIVO]
COLUNAS_LEITURA_MINIMA = list(set(COLUNAS_ANALISE + [COLUNA_NATJUR_PASSIVO]))
TOP_N = 10
CHUNKSIZE_ANALISE = 100000
PALAVRAS_CHAVE_ENTES_PUBLICOS = {'ORGAO PUBLICO', 'ESTADO OU DISTRITO FEDERAL', 'MUNICIPIO', 'AUTARQUIA', 'FUNDACAO PUBLICA', 'UNIAO', 'SECRETARIA', 'PROCURADORIA', 'FAZENDA', 'FEDERAL', 'ESTADUAL', 'MUNICIPAL', 'INSTITUTO NACIONAL DO SEGURO SOCIAL', 'ADVOCACIA GERAL DA UNIAO'}

# Tenta usar lxml para melhor performance do CSV, senão usa engine python
try:
    import lxml
    CSV_ENGINE = 'lxml' # Não é um parâmetro direto do read_csv, mas indica a tentativa
    HTML_PARSER = 'lxml'
    logger.info("Biblioteca lxml encontrada. Será usada onde aplicável.")
except ImportError:
    logging.warning("Biblioteca 'lxml' não encontrada.")
    CSV_ENGINE = 'python' # read_csv usa C por padrão, mas pode usar python
    HTML_PARSER = 'html.parser'


# --- Funções Auxiliares de Processamento ---
# (parse_multi_valor, eh_ente_publico - Mantidas como na versão anterior)
def parse_multi_valor(valor_celula: str | float) -> list[str]:
    if pd.isna(valor_celula): return []
    texto_str = str(valor_celula).strip()
    if not texto_str: return []
    if texto_str.startswith('{') and texto_str.endswith('}'): texto_interno = texto_str[1:-1]
    else: texto_interno = texto_str
    valores = [item.strip() for item in re.split(r'\s*,\s*', texto_interno) if item.strip()]
    return valores

def eh_ente_publico(texto_natjur: str | float, palavras_chave: set) -> bool:
    if pd.isna(texto_natjur): return False
    lista_naturezas = parse_multi_valor(texto_natjur)
    if not lista_naturezas: return False
    for natureza in lista_naturezas:
        natureza_upper = natureza.upper()
        for chave in palavras_chave:
            if chave in natureza_upper: return True
    return False

# --- Função Principal de Análise (analisar_frequencias) ---
# (Mantida como na versão anterior, com filtro opcional)
def analisar_frequencias(caminho_csv: Path, colunas_analise: list, filtrar_por_ente_publico: bool = False, coluna_filtro_ente: str | None = None, palavras_chave_entes: set | None = None) -> dict | None:
    tipo_analise = " (vs Entes Públicos)" if filtrar_por_ente_publico else " (Geral)"
    logger.info(f"Analisando frequências{tipo_analise} em: {caminho_csv.name}")
    if not caminho_csv.exists(): logger.error(f"Arquivo não encontrado: {caminho_csv}"); return None
    colunas_para_ler = list(set(colunas_analise + ([coluna_filtro_ente] if filtrar_por_ente_publico and coluna_filtro_ente else [])))
    dtypes_analise = {col: 'str' for col in colunas_para_ler}
    contadores = {col: Counter() for col in colunas_analise}
    contagem_sigiloso = {col: 0 for col in colunas_analise}
    linhas_lidas_total = 0; linhas_processadas_analise = 0
    colunas_analise_existentes = []
    try:
        try:
            df_peek = pd.read_csv(caminho_csv, sep=';', encoding='utf-8', nrows=1)
            colunas_presentes = df_peek.columns.tolist()
            colunas_analise_existentes = [col for col in colunas_analise if col in colunas_presentes]
            coluna_filtro_ente_existe = coluna_filtro_ente in colunas_presentes if coluna_filtro_ente else True
            if not colunas_analise_existentes: logger.error(f"Nenhuma coluna de análise encontrada em {caminho_csv.name}. Pulando."); return None
            if filtrar_por_ente_publico and not coluna_filtro_ente_existe: logger.error(f"Coluna filtro '{coluna_filtro_ente}' não encontrada em {caminho_csv.name}. Pulando filtro ente público."); filtrar_por_ente_publico=False
            colunas_para_ler_final = list(set(colunas_analise_existentes + ([coluna_filtro_ente] if filtrar_por_ente_publico and coluna_filtro_ente_existe else [])))
            dtypes_analise = {col: 'str' for col in colunas_para_ler_final}
            contadores = {col: Counter() for col in colunas_analise_existentes}
            contagem_sigiloso = {col: 0 for col in colunas_analise_existentes}
        except Exception as peek_err: logger.error(f"Erro cabeçalho {caminho_csv.name}: {peek_err}."); return None

        iterador_csv = pd.read_csv(caminho_csv, sep=';', encoding='utf-8', dtype=dtypes_analise, chunksize=CHUNKSIZE_ANALISE, usecols=colunas_para_ler_final, low_memory=False)
        for i, chunk in enumerate(iterador_csv):
            logger.debug(f"Processando chunk análise {i+1}{tipo_analise}..."); linhas_lidas_total += len(chunk)
            chunk_processar = chunk
            if filtrar_por_ente_publico and coluna_filtro_ente_existe:
                mascara_ente_publico = chunk[coluna_filtro_ente].apply(eh_ente_publico, args=(palavras_chave_entes,))
                chunk_processar = chunk[mascara_ente_publico]
            linhas_processadas_analise += len(chunk_processar)
            if chunk_processar.empty: del chunk, chunk_processar; gc.collect() if i%5==0 else None; continue
            for coluna in colunas_analise_existentes:
                 if coluna not in chunk_processar.columns: continue
                 col_data = chunk_processar[coluna].dropna()
                 if not col_data.empty:
                    items_parsed = col_data.apply(parse_multi_valor)
                    for lista_items in items_parsed:
                        for item in lista_items:
                            item_strip_upper = item.strip().upper()
                            if item_strip_upper == 'SIGILOSO': contagem_sigiloso[coluna] += 1
                            elif item_strip_upper: contadores[coluna][item] += 1
            del chunk, chunk_processar, col_data, items_parsed; gc.collect() if i%5==0 else None

        logger.info(f"Análise{tipo_analise} de {caminho_csv.name} concluída. Lidas: {linhas_lidas_total}, Processadas: {linhas_processadas_analise}")
        resultados_finais = {}
        for coluna in colunas_analise:
            if coluna not in colunas_analise_existentes: resultados_finais[coluna] = {'contagens': pd.Series(dtype=int),'total_ocorrencias': 0,'contagem_sigiloso': 0}; continue
            contador = contadores.get(coluna, Counter()); sigilosos = contagem_sigiloso.get(coluna, 0); total_ocorrencias_geral = sum(contador.values()) + sigilosos
            if total_ocorrencias_geral == 0: resultados_finais[coluna] = {'contagens': pd.Series(dtype=int),'total_ocorrencias': 0,'contagem_sigiloso': 0}; continue
            series_contagem = pd.Series(contador).sort_values(ascending=False)
            resultados_finais[coluna] = {'contagens': series_contagem, 'total_ocorrencias': total_ocorrencias_geral, 'contagem_sigiloso': sigilosos}
        return resultados_finais
    except Exception as e: logger.error(f"Erro inesperado ao analisar '{caminho_csv.name}': {e}", exc_info=True); return None


# --- Funções de Exportação ---

def formatar_tabela_analise(dados_contagem: dict, top_n: int = TOP_N) -> pd.DataFrame | None:
    """Formata os dados de contagem em um DataFrame para exportação."""
    # (Função mantida como na versão anterior)
    contagens = dados_contagem.get('contagens')
    total_ocorrencias = dados_contagem.get('total_ocorrencias', 0)
    contagem_sigiloso = dados_contagem.get('contagem_sigiloso', 0)
    if total_ocorrencias == 0 or contagens is None: return None
    total_validas = total_ocorrencias - contagem_sigiloso
    total_itens_unicos_validos = len(contagens)
    top_n_contagens = contagens.head(top_n)
    soma_top_n = top_n_contagens.sum()
    contagem_outros = total_validas - soma_top_n

    if not top_n_contagens.empty:
        df_tabela = pd.DataFrame({'Item': top_n_contagens.index.astype(str), 'Contagem': top_n_contagens.values})
        df_tabela['Percentual'] = (df_tabela['Contagem'] / total_ocorrencias * 100).map('{:.2f}%'.format) if total_ocorrencias > 0 else '0.00%'
    else: df_tabela = pd.DataFrame(columns=['Item', 'Contagem', 'Percentual'])
    if total_itens_unicos_validos > top_n and contagem_outros > 0:
        percentual_outros = (contagem_outros / total_ocorrencias * 100) if total_ocorrencias > 0 else 0
        outros_row = pd.DataFrame({'Item': [f'Outros ({total_itens_unicos_validos - top_n} itens)'], 'Contagem': [contagem_outros], 'Percentual': [f'{percentual_outros:.2f}%']})
        df_tabela = pd.concat([df_tabela, outros_row], ignore_index=True)
    if contagem_sigiloso > 0:
        perc_sigiloso = (contagem_sigiloso / total_ocorrencias * 100) if total_ocorrencias > 0 else 0
        sigiloso_row = pd.DataFrame({'Item': ['Sigiloso'], 'Contagem': [contagem_sigiloso], 'Percentual': [f'{perc_sigiloso:.2f}%']})
        df_tabela = pd.concat([df_tabela, sigiloso_row], ignore_index=True)
    if not df_tabela.empty or contagem_sigiloso > 0:
        total_row = pd.DataFrame({'Item': ['TOTAL GERAL'], 'Contagem': [total_ocorrencias], 'Percentual': ['100.00%']})
        df_tabela = pd.concat([df_tabela, total_row], ignore_index=True)
    return df_tabela if not df_tabela.empty else None


def exportar_analises_csv(resultados_por_contexto: dict, arquivo_saida_csv: Path, top_n: int = TOP_N):
    """Exporta TODOS os resultados formatados para um único arquivo CSV."""
    logger.info(f"Iniciando exportação da análise para CSV: {arquivo_saida_csv.name}")
    arquivo_saida_csv.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(arquivo_saida_csv, 'w', encoding='utf-8', newline='') as f_out:
            for titulo, resultados_completos in resultados_por_contexto.items():
                if not resultados_completos: logger.warning(f"Sem resultados para exportar para o contexto: {titulo}"); continue
                f_out.write(f"\n=== {titulo.upper()} ===\n\n") # Título
                for coluna, dados_contagem in resultados_completos.items():
                    f_out.write(f"--- Coluna: {coluna} ---\n") # Subtítulo
                    df_formatado = formatar_tabela_analise(dados_contagem, top_n)
                    if df_formatado is not None:
                        # *** CORRIGIDO: Removido line_terminator ***
                        df_formatado.to_csv(f_out, sep=';', index=False, header=True)
                    else: f_out.write("(Nenhum dado)\n")
                    f_out.write("\n") # Linha extra entre tabelas
                f_out.write("\n\n") # Linhas extras entre contextos
        logger.info(f"Análise exportada com sucesso para: {arquivo_saida_csv.name}")
    except Exception as e: logger.error(f"Erro ao exportar análise para CSV: {e}", exc_info=True)


# --- Classe PDF Customizada (com fallback de fonte e tratamento de erro de estilo) ---
class PDFReport(FPDF):
    def __init__(self, orientation='P', unit='mm', format='A4'):
        super().__init__(orientation, unit, format)
        self.set_auto_page_break(auto=True, margin=15)
        self.alias_nb_pages()
        self.font_name = 'helvetica' # Padrão fallback
        self.has_bold = True
        self.has_italic = True
        self.setup_fonts()

    def setup_fonts(self):
        # (Lógica de setup_fonts mantida como antes - tentar DejaVu, fallback helvetica)
        try:
            font_dir = Path(__file__).parent / "font_family/"
            regular_font = font_dir / "DejaVuSans.ttf"
            bold_font = font_dir / "DejaVuSans-Bold.ttf"
            italic_font = font_dir / "DejaVuSans-Oblique.ttf"
            bold_italic_font = font_dir / "DejaVuSans-BoldOblique.ttf"
            if regular_font.exists():
                self.add_font('DejaVu', '', regular_font, uni=True)
                self.font_name = 'DejaVu'; logger.info("Usando fonte base 'DejaVuSans' para PDF.")
                self.has_bold = False; self.has_italic = False # Reseta flags
                if bold_font.exists():
                    try: self.add_font('DejaVu', 'B', bold_font, uni=True); self.has_bold = True; logger.info("  -> Estilo Negrito (B) carregado.")
                    except Exception as fe_b: logger.warning(f"  -> Erro FPDF ao adicionar DejaVu Bold: {fe_b}")
                else: logger.warning("Arquivo DejaVuSans-Bold.ttf não encontrado.")
                if italic_font.exists():
                    try: self.add_font('DejaVu', 'I', italic_font, uni=True); self.has_italic = True; logger.info("  -> Estilo Itálico (I) carregado.")
                    except Exception as fe_i: logger.warning(f"  -> Erro FPDF ao adicionar DejaVu Italic: {fe_i}")
                else: logger.warning("Arquivo DejaVuSans-Oblique.ttf não encontrado.")
                if bold_italic_font.exists() and self.has_bold and self.has_italic:
                     try: self.add_font('DejaVu', 'BI', bold_italic_font, uni=True); logger.info("  -> Estilo Negrito-Itálico (BI) carregado.")
                     except Exception as fe_bi: logger.warning(f"  -> Erro FPDF ao adicionar DejaVu Bold-Italic: {fe_bi}")
                else: logger.debug("Arquivo DejaVuSans-BoldOblique.ttf não encontrado ou B/I falharam.")
            else: logger.warning("Arquivo 'DejaVuSans.ttf' (Regular) não encontrado. Usando 'helvetica'.")
        except Exception as e: logger.warning(f"Erro inesperado ao configurar fontes: {e}. Usando 'helvetica'.")
        self.set_font(self.font_name, size=10)


    def set_font_style(self, style='', size=None):
        # (Função mantida como antes)
        current_size = size if size is not None else self.font_size; target_font = self.font_name; target_style = style.upper()
        if self.font_name == 'DejaVu':
            if 'B' in target_style and not self.has_bold: target_style = target_style.replace('B', '')
            if 'I' in target_style and not self.has_italic: target_style = target_style.replace('I', '')
            if style.upper() == 'BI' and ('B' not in target_style or 'I' not in target_style): target_style = ''
        if target_font == 'helvetica' and target_style not in ['', 'B', 'I', 'BI']: target_style = ''
        try: self.set_font(target_font, target_style, current_size)
        except RuntimeError as e: logger.warning(f"Erro ao definir fonte '{target_font}' estilo '{style}': {e}. Usando regular."); self.set_font(target_font, '', current_size)

    def header(self):
        # (Função mantida como antes)
        self.set_font_style('B', 12); self.cell(0, 10, 'Relatório de Análise CNJ - Saúde', border=0, ln=1, align='C'); self.ln(5)

    def footer(self):
        # (Função mantida como antes)
        self.set_y(-15); self.set_font_style('I', 8); self.cell(0, 10, f'Página {self.page_no()}/{{nb}}', border=0, align='C')

    def chapter_title(self, title):
        """Adiciona um título de capítulo/seção"""
        self.set_font_style('B', 14)
        self.set_fill_color(200, 220, 255)
        # *** CORRIGIDO: Removido ln=1 de multi_cell ***
        self.multi_cell(0, 8, title, border=0, align='L', fill=True)
        self.ln(4) # Mantém o espaçamento após o título

    def sub_title(self, title):
        # (Função mantida como antes)
        self.set_font_style('B', 11); self.cell(0, 6, title, border=0, ln=1, align='L'); self.ln(2)

    def draw_table(self, df_tabela: pd.DataFrame):
        """Desenha uma tabela simples no PDF a partir de um DataFrame."""
        if df_tabela is None or df_tabela.empty:
             self.set_font_style('I', 9); self.write(5, "(Nenhum dado)\n"); self.ln(2); return

        col_widths = {'Item': 110, 'Contagem': 30, 'Percentual': 30}
        line_height = self.font_size * 1.5

        # Cabeçalho da tabela
        self.set_font_style('B', 9)
        for col_name in df_tabela.columns:
            self.cell(col_widths[col_name], line_height, col_name, border=1, align='C')
        self.ln(line_height)

        # Linhas de dados
        self.set_font_style('', 8)
        for index, row in df_tabela.iterrows():
            current_x = self.get_x(); current_y = self.get_y(); max_h = line_height
            # *** CORRIGIDO: Removido ln=3 e max_line_height de multi_cell ***
            self.multi_cell(col_widths['Item'], line_height, str(row['Item']), border='LR', align='L')
            h_item = self.get_y() - current_y
            max_h = max(max_h, h_item)
            self.set_xy(current_x + col_widths['Item'], current_y)
            self.cell(col_widths['Contagem'], max_h, str(row['Contagem']), border='LR', align='R', ln=0) # Border LR para alinhar com multicell
            self.cell(col_widths['Percentual'], max_h, str(row['Percentual']), border='LR', align='R', ln=1) # Border LR e ln=1
        # Desenha linha inferior da tabela
        self.cell(sum(col_widths.values()), 0, '', border='T', ln=1)
        self.ln(4)


# --- Função de Exportação PDF (Corrigida) ---
def exportar_analises_pdf(resultados_por_contexto: dict, arquivo_saida_pdf: Path, top_n: int = TOP_N):
    """Exporta TODOS os resultados formatados para um único arquivo PDF."""
    logger.info(f"Iniciando exportação da análise para PDF: {arquivo_saida_pdf.name}")
    arquivo_saida_pdf.parent.mkdir(parents=True, exist_ok=True)
    try:
        pdf = PDFReport('P', 'mm', 'A4') # Cria instância

        for titulo, resultados_completos in resultados_por_contexto.items():
             pdf.add_page()
             titulo_curto = titulo.replace("Resultados ", "").replace(" (vs Entes Públicos)", " vs Entes Públicos").replace(" (Geral)", " - Geral")
             pdf.chapter_title(titulo_curto)
             if not resultados_completos:
                 pdf.set_font_style('I', 9)
                 pdf.write(5, "(Nenhum resultado para este contexto)\n")
                 pdf.ln(5); continue
             for coluna, dados_contagem in resultados_completos.items():
                 # Verifica espaço ANTES de adicionar subtítulo e tabela
                 altura_estimada = 6 + 2 + (pdf.font_size*1.5)*2 # Título + espaço + Cabeçalho Tab + 1 linha dados (mínimo)
                 if pdf.get_y() + altura_estimada > pdf.page_break_trigger:
                      pdf.add_page()
                      # Opcional: repetir título do capítulo se quebrar página entre colunas
                      # pdf.chapter_title(titulo_curto)

                 pdf.sub_title(f"Coluna: '{coluna}'")
                 df_formatado = formatar_tabela_analise(dados_contagem, top_n)
                 pdf.draw_table(df_formatado) # Chama a função corrigida

        pdf.output(arquivo_saida_pdf) # Salva o PDF no final
        logger.info(f"Análise exportada com sucesso para: {arquivo_saida_pdf.name}")

    except ImportError: logger.error("Erro: Biblioteca FPDF2 não encontrada.")
    except FileNotFoundError as fnf_err:
        if 'DejaVuSans' in str(fnf_err): logger.error(f"Erro: Arquivo de fonte .ttf da família DejaVuSans não encontrado: {fnf_err}")
        else: logger.error(f"Erro de arquivo não encontrado ao gerar PDF: {fnf_err}")
    except RuntimeError as rt_err:
         logger.error(f"Erro FPDF/Runtime ao gerar PDF: {rt_err}", exc_info=True)
    except Exception as e:
        logger.error(f"Erro geral ao exportar análise para PDF: {e}", exc_info=True)

# --- Função Principal de Orquestração ---
def main():
    """Função principal que orquestra a análise e exportação."""
    logger.info(">>> INICIANDO SCRIPT DE ANÁLISE CNJ SAÚDE (GERAL E ENTES PÚBLICOS) <<<")
    PASTA_SAIDA_RELATORIOS.mkdir(parents=True, exist_ok=True)
    resultados_todas_analises = {}

    # --- ANÁLISE GERAL ---
    logger.info("\n=== REALIZANDO ANÁLISE GERAL (TODOS OS PROCESSOS DE SAÚDE) ===")
    resultados_brasil_geral = analisar_frequencias(ARQUIVO_DADOS_CONSOLIDADO, COLUNAS_ANALISE, False)
    if resultados_brasil_geral: resultados_todas_analises["Brasil Consolidado - Geral"] = resultados_brasil_geral
    else: logger.warning("Não foi possível gerar a análise geral consolidada.")
    arquivos_regionais = sorted(list(PASTA_DADOS_REGIONAIS.glob("*.csv")))
    if arquivos_regionais:
        logger.info(f"Encontrados {len(arquivos_regionais)} arquivos regionais para análise geral.")
        for caminho_csv in arquivos_regionais:
            nome_analise = caminho_csv.stem.replace('dados_saude_', '')
            titulo = f"Regional {nome_analise} - Geral"
            resultados_reg = analisar_frequencias(caminho_csv, COLUNAS_ANALISE, False)
            if resultados_reg: resultados_todas_analises[titulo] = resultados_reg
            else: logger.error(f"Falha análise geral: {caminho_csv.name}")
            gc.collect()
    else: logger.warning(f"Nenhum arquivo CSV encontrado em {PASTA_DADOS_REGIONAIS} para análise regional geral.")

    # --- ANÁLISE ENTES PÚBLICOS ---
    logger.info("\n=== REALIZANDO ANÁLISE FOCADA (PROCESSOS CONTRA ENTES PÚBLICOS) ===")
    resultados_brasil_entes = analisar_frequencias(ARQUIVO_DADOS_CONSOLIDADO, COLUNAS_ANALISE, True, COLUNA_NATJUR_PASSIVO, PALAVRAS_CHAVE_ENTES_PUBLICOS)
    if resultados_brasil_entes: resultados_todas_analises["Brasil Consolidado - vs Entes Públicos"] = resultados_brasil_entes
    else: logger.warning("Não foi possível gerar a análise consolidada vs Entes Públicos.")
    if arquivos_regionais:
        logger.info(f"Encontrados {len(arquivos_regionais)} arquivos regionais para análise vs Entes Públicos.")
        for caminho_csv in arquivos_regionais:
            nome_analise = caminho_csv.stem.replace('dados_saude_', '')
            titulo = f"Regional {nome_analise} - vs Entes Públicos"
            resultados_reg_entes = analisar_frequencias(caminho_csv, COLUNAS_ANALISE, True, COLUNA_NATJUR_PASSIVO, PALAVRAS_CHAVE_ENTES_PUBLICOS)
            if resultados_reg_entes: resultados_todas_analises[titulo] = resultados_reg_entes
            else: logger.error(f"Falha análise vs Entes Públicos: {caminho_csv.name}")
            gc.collect()
    else: logger.warning(f"Nenhum arquivo CSV encontrado em {PASTA_DADOS_REGIONAIS} para análise regional vs Entes Públicos.")

    # --- EXPORTAÇÃO FINAL ---
    if resultados_todas_analises:
        logger.info("\n--- Exportando Todos os Resultados da Análise ---")
        caminho_saida_csv = PASTA_SAIDA_RELATORIOS / f"{NOME_BASE_RELATORIO}.csv"
        caminho_saida_pdf = PASTA_SAIDA_RELATORIOS / f"{NOME_BASE_RELATORIO}.pdf"
        exportar_analises_csv(resultados_todas_analises, caminho_saida_csv, TOP_N)
        exportar_analises_pdf(resultados_todas_analises, caminho_saida_pdf, TOP_N)
    else: logger.warning("Nenhum resultado de análise foi gerado para exportar.")
    logger.info(">>> ANÁLISE E EXPORTAÇÃO CONCLUÍDAS <<<")

# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    main()