# relatorio_export.py
# -*- coding: utf-8 -*-
"""
Módulo para formatar e exportar resultados de análise de frequência
para arquivos CSV e PDF.
"""

import pandas as pd
from pathlib import Path
import logging
from fpdf import FPDF # Importa a biblioteca FPDF

logger = logging.getLogger(__name__)

# --- Constantes de Formatação ---
TOP_N_DEFAULT = 10

# --- Funções Auxiliares de Formatação ---

def formatar_tabela_analise(
    dados_contagem: dict,
    top_n: int = TOP_N_DEFAULT
) -> pd.DataFrame | None:
    """
    Formata os dados de contagem de uma coluna em um DataFrame para exibição/exportação.

    Args:
        dados_contagem: Dicionário contendo {'contagens': pd.Series,
                         'total_ocorrencias': int, 'contagem_sigiloso': int}.
        top_n: Número de itens principais a incluir.

    Returns:
        DataFrame formatado com colunas 'Item', 'Contagem', 'Percentual',
        incluindo linhas para 'Outros', 'Sigiloso' (se aplicável) e 'TOTAL GERAL',
        ou None se não houver dados.
    """
    contagens = dados_contagem.get('contagens')
    total_ocorrencias = dados_contagem.get('total_ocorrencias', 0)
    contagem_sigiloso = dados_contagem.get('contagem_sigiloso', 0)

    if total_ocorrencias == 0 or contagens is None:
        return None # Retorna None se não há dados válidos

    total_validas = total_ocorrencias - contagem_sigiloso
    total_itens_unicos_validos = len(contagens)

    # Pega o Top N (dos não sigilosos)
    top_n_contagens = contagens.head(top_n)

    # Calcula 'Outros' (dos não sigilosos)
    soma_top_n = top_n_contagens.sum()
    contagem_outros = total_validas - soma_top_n

    # Prepara DataFrame para exibição
    if not top_n_contagens.empty:
        df_tabela = pd.DataFrame({
            'Item': top_n_contagens.index.astype(str),
            'Contagem': top_n_contagens.values
        })
        # Calcula Percentual para Top N (relativo ao total GERAL)
        df_tabela['Percentual'] = (df_tabela['Contagem'] / total_ocorrencias * 100).map('{:.2f}%'.format) if total_ocorrencias > 0 else '0.00%'
    else:
        df_tabela = pd.DataFrame(columns=['Item', 'Contagem', 'Percentual'])

    # Adiciona linha 'Outros', se houver itens válidos além do Top N
    if total_itens_unicos_validos > top_n and contagem_outros > 0:
        percentual_outros = (contagem_outros / total_ocorrencias * 100) if total_ocorrencias > 0 else 0
        outros_row = pd.DataFrame({
            'Item': [f'Outros ({total_itens_unicos_validos - top_n} itens)'],
            'Contagem': [contagem_outros],
            'Percentual': [f'{percentual_outros:.2f}%']
        })
        df_tabela = pd.concat([df_tabela, outros_row], ignore_index=True)

    # Adiciona linha 'Sigiloso', se houver
    if contagem_sigiloso > 0:
        perc_sigiloso = (contagem_sigiloso / total_ocorrencias * 100) if total_ocorrencias > 0 else 0
        sigiloso_row = pd.DataFrame({
            'Item': ['Sigiloso'],
            'Contagem': [contagem_sigiloso],
            'Percentual': [f'{perc_sigiloso:.2f}%']
        })
        df_tabela = pd.concat([df_tabela, sigiloso_row], ignore_index=True)

    # Adiciona linha 'TOTAL GERAL' se houver dados na tabela
    if not df_tabela.empty:
        total_row = pd.DataFrame({
            'Item': ['TOTAL GERAL'],
            'Contagem': [total_ocorrencias],
            'Percentual': ['100.00%']
        })
        df_tabela = pd.concat([df_tabela, total_row], ignore_index=True)

    return df_tabela if not df_tabela.empty else None


# --- Funções de Exportação ---

def exportar_analises_csv(
    resultados_por_contexto: dict,
    arquivo_saida_csv: Path,
    top_n: int = TOP_N_DEFAULT
):
    """
    Exporta os resultados formatados da análise para um único arquivo CSV.

    Args:
        resultados_por_contexto: Dicionário {titulo_contexto: resultados_completos}.
        arquivo_saida_csv: Path para o arquivo CSV de saída.
        top_n: Número de itens principais a incluir nas tabelas.
    """
    logger.info(f"Iniciando exportação da análise para CSV: {arquivo_saida_csv.name}")
    arquivo_saida_csv.parent.mkdir(parents=True, exist_ok=True)
    primeira_tabela = True

    try:
        with open(arquivo_saida_csv, 'w', encoding='utf-8', newline='') as f_out:
            for titulo, resultados_completos in resultados_por_contexto.items():
                if not resultados_completos: continue

                # Escreve o título do contexto no CSV (como linha separada)
                f_out.write(f"\n=== {titulo.upper()} ===\n") # Linha de título

                for coluna, dados_contagem in resultados_completos.items():
                    f_out.write(f"\n--- Coluna: {coluna} ---\n") # Linha de subtítulo
                    df_formatado = formatar_tabela_analise(dados_contagem, top_n)

                    if df_formatado is not None:
                        # Escreve o DataFrame formatado no CSV
                        # O cabeçalho só é escrito implicitamente se mode='w' e header=True
                        # Como estamos escrevendo manualmente títulos e subtítulos,
                        # controlamos o cabeçalho do DF aqui.
                        header_df = True # Sempre escreve o cabeçalho do DF formatado
                        df_formatado.to_csv(
                            f_out,
                            sep=';',
                            index=False,
                            header=header_df,
                            line_terminator='\n' # Pode usar se Pandas >= 0.24.0
                        )
                        primeira_tabela = False # Flag não tão relevante aqui
                    else:
                        f_out.write("(Nenhum dado)\n")
                f_out.write("\n") # Linha extra entre contextos

        logger.info(f"Análise exportada com sucesso para: {arquivo_saida_csv.name}")

    except Exception as e:
        logger.error(f"Erro ao exportar análise para CSV: {e}", exc_info=True)


class PDFReport(FPDF):
    """Classe customizada para adicionar cabeçalho/rodapé se necessário (opcional)"""
    def header(self):
        # Exemplo: Adicionar um logo ou título padrão em toda página
        # self.image('logo.png', 10, 8, 33)
        self.set_font('helvetica', 'B', 12) # Usar fontes padrão inicialmente
        self.cell(0, 10, 'Relatório de Análise CNJ - Saúde vs Entes Públicos', border=0, ln=1, align='C')
        self.ln(5) # Pular linha

    def footer(self):
        self.set_y(-15) # Posição 1.5 cm da parte inferior
        self.set_font('helvetica', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}/{{nb}}', border=0, align='C')

    def chapter_title(self, title):
        """Adiciona um título de capítulo/seção"""
        self.set_font('helvetica', 'B', 14)
        self.set_fill_color(200, 220, 255) # Cor de fundo azul claro
        self.cell(0, 8, title, border=0, ln=1, align='L', fill=True)
        self.ln(4)

    def sub_title(self, title):
        """Adiciona um subtítulo de coluna"""
        self.set_font('helvetica', 'B', 11)
        self.cell(0, 6, title, border=0, ln=1, align='L')
        self.ln(2)

    def draw_table(self, df_tabela: pd.DataFrame):
        """Desenha uma tabela simples no PDF a partir de um DataFrame."""
        if df_tabela is None or df_tabela.empty:
             self.set_font('helvetica', 'I', 9)
             self.cell(0, 5, "(Nenhum dado)", ln=1)
             self.ln(2)
             return

        self.set_font('helvetica', 'B', 9) # Fonte para o cabeçalho da tabela
        col_widths = {'Item': 115, 'Contagem': 25, 'Percentual': 25} # Larguras APROXIMADAS (ajuste)
        total_width = sum(col_widths.values())
        line_height = self.font_size * 1.5

        # Desenha cabeçalho da tabela
        for col_name in df_tabela.columns:
            self.cell(col_widths[col_name], line_height, col_name, border=1, align='C')
        self.ln(line_height)

        # Desenha linhas de dados
        self.set_font('helvetica', '', 8) # Fonte para os dados
        for index, row in df_tabela.iterrows():
            # Verifica se precisa de nova página ANTES de desenhar a linha
            if self.get_y() + line_height > self.page_break_trigger:
                self.add_page(self.cur_orientation)
                # Redesenha cabeçalho na nova página (opcional, mas bom para tabelas longas)
                self.set_font('helvetica', 'B', 9)
                for col_name_hdr in df_tabela.columns:
                    self.cell(col_widths[col_name_hdr], line_height, col_name_hdr, border=1, align='C')
                self.ln(line_height)
                self.set_font('helvetica', '', 8)

            # Desenha células da linha atual
            for col_name in df_tabela.columns:
                 align = 'L' if col_name == 'Item' else 'R' # Alinha números à direita
                 self.cell(col_widths[col_name], line_height, str(row[col_name]), border=1, align=align)
            self.ln(line_height)
        self.ln(4) # Espaço após a tabela


def exportar_analises_pdf(
    resultados_por_contexto: dict,
    arquivo_saida_pdf: Path,
    top_n: int = TOP_N_DEFAULT
):
    """
    Exporta os resultados formatados da análise para um arquivo PDF.

    Args:
        resultados_por_contexto: Dicionário {titulo_contexto: resultados_completos}.
        arquivo_saida_pdf: Path para o arquivo PDF de saída.
        top_n: Número de itens principais a incluir nas tabelas.
    """
    logger.info(f"Iniciando exportação da análise para PDF: {arquivo_saida_pdf.name}")
    arquivo_saida_pdf.parent.mkdir(parents=True, exist_ok=True)

    try:
        pdf = PDFReport('P', 'mm', 'A4') # Retrato, mm, A4
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15) # Habilita quebra de pág automática
        pdf.alias_nb_pages() # Habilita contagem total de páginas no rodapé '{nb}'

        # Tentar adicionar fonte com suporte a UTF-8 (Ex: DejaVu)
        # NOTA: O arquivo .ttf precisa estar acessível (no mesmo dir ou path completo)
        #       Baixe 'DejaVuSans.ttf' se não tiver.
        try:
            # Tenta adicionar a fonte DejaVu (requer arquivo .ttf)
            # Se o arquivo não for encontrado, ele usará a fonte padrão 'helvetica'
            # que pode não renderizar todos os caracteres corretamente.
            font_path = Path("DejaVuSans.ttf") # Procura no diretório atual
            if font_path.exists():
                pdf.add_font('DejaVu', '', font_path, uni=True)
                pdf.set_font('DejaVu', size=10)
                logger.info("Usando fonte DejaVuSans para PDF (suporte UTF-8).")
            else:
                logger.warning("Arquivo de fonte 'DejaVuSans.ttf' não encontrado. Usando fonte padrão 'helvetica' para PDF (pode haver problemas com acentos/caracteres especiais).")
                pdf.set_font('helvetica', size=10)
        except Exception as font_err:
             logger.warning(f"Erro ao carregar fonte DejaVu: {font_err}. Usando fonte padrão 'helvetica'.")
             pdf.set_font('helvetica', size=10)


        for titulo, resultados_completos in resultados_por_contexto.items():
            pdf.chapter_title(titulo.replace("Resultados ", "").replace(" (vs Entes Públicos)", "")) # Título da seção

            if not resultados_completos:
                 pdf.set_font('helvetica', 'I', 9)
                 pdf.cell(0, 5, "(Nenhum resultado para este contexto)", ln=1)
                 pdf.ln(5)
                 continue

            for coluna, dados_contagem in resultados_completos.items():
                pdf.sub_title(f"Coluna: '{coluna}'") # Subtítulo
                df_formatado = formatar_tabela_analise(dados_contagem, top_n)
                pdf.draw_table(df_formatado) # Desenha a tabela

        pdf.output(arquivo_saida_pdf)
        logger.info(f"Análise exportada com sucesso para: {arquivo_saida_pdf.name}")

    except ImportError:
         logger.error("Erro: Biblioteca FPDF2 não encontrada. Instale com 'pip install fpdf2'")
    except Exception as e:
        logger.error(f"Erro ao exportar análise para PDF: {e}", exc_info=True)