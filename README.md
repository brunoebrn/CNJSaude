# CNJSaude

**Quem sou eu:**
Bruno Eduardo Bastos Rolim Nunes, alagoano, médico (CRM 10.089/AL) e cientista de dados, com atuação no âmbito da auditoria e gestão pública em saúde.

**Objetivo do trabalho**
Esse script foi criado por objetivo realizar o processamento de dados públicos extraídos do DATAJUD (Base Nacional de Dados do Judiciário), provida pelo CNJ (Conselho Nacional de Justiça) relacionadas à judicialização da saúde.

**Link de acesso à plataforma:** https://justica-em-numeros.cnj.jus.br/painel-estatisticas/

As bases de dados utilizadas foram exportadas nos dias 13 e 14 de abril de 2025, a partir da aba **Download**, **Lista de Processos por Indicador**, com os seguintes filtros ativos: Tribunal (Selecionando individualmente cada tribunal de interesse) e Indicador (casos novos)

Os Tribuinais selecionados foram todos os Tribunais de Justiça (TJ) e os 6 Tribunais Regionais Federais.
Os arquivos foram organizados em pastas, sendo os arquivos relacionados aos TJs na pasta da respectiva região do Brasil na qual o tribunal está localizada (Centro-Oeste, Nordeste, Norte, Sudeste e Sul). Os TRFs foram separadso em uma pasta a parte. As pastas utilizadas foram expostas no projeto no seguinte diretório **AnaliseBR/CO** (exemplo do Centro-Oeste, abreviado). 

No entanto, dado o tamanho dos arquivos e os limites de upload, os arquivos não estão disponíveis neste repositório do GitHub. Porém os anexo abaixo:
- CO: https://1drv.ms/f/c/250344c52e70c468/ErGjt0-flAdAmBQR42UrZSkBaEH6nPIHNsr1a8_KgCHsxA?e=O0gJNl
- NE: https://1drv.ms/f/c/250344c52e70c468/EiheRrbLqghFoo1up2SWAjwBA42hK9INJh2Uk1H2IvRfSA?e=A8PP7v
- NO: https://1drv.ms/f/c/250344c52e70c468/Er3d4I8JRHxBnvgpjmDnSZIB2I0ieAhnu1lWd9aQjKkSrQ?e=9Km7RW
- SE: https://1drv.ms/f/c/250344c52e70c468/Epcq9_Dx31hJkyr2vkjd_2wBREKaPqrSz3V8iT64YuuTZA?e=PdNiQu
- SU: https://1drv.ms/f/c/250344c52e70c468/En7Ma_KA_BpJsVI1sbUFv0oBZo_beKf3wpoXDDH4rdRgCA?e=b0E3DC
- TRF: https://1drv.ms/f/c/250344c52e70c468/EjyVIprTZN5LgOKl8XMb-pUBtYzdxQjmbGkyjVFNApd-0Q?e=SJierP

Além disso, os mesmos arquivos ainda podem ser baixados diretamente da página do DATAJUD/CNJ supracitada, utilizando os filtros citados.

O arquivo obtido através da página é um .zip (arquivo zipado) e contém um ou mais arquivos de tabulação dentro deles (no formato .CSV).

**Funcionamento dos scripts Python**

*main.py* - Script responsável por:
- leitura dos arquivos presentes na pasta *AnaliseBR* e subspastas;
- descompactação dos arquivos *.zip*;
- filtragem dos processos contendo códigos de assunto relacionados à demandas de saúde (códigos expostos abaixo);
- seleção de colunas necessárias, para o autor, para análise;
- exportação, também no formato *.CSV*, apenas dos processos com assunto relacionados à saúde referentes a cada TJ e TRF (disponíveis em *Output_AnaliseBR_Saude*), bem como exporta um compilado global (*DADOS_CNJ_FILTRADOS_SAUDE_CONSOLIDADO.csv*).

*Códigos de assuntos utilizados no filtro*: 
    12480, 12521, 12520, 12507, 12508, 12509, 12510, 12481, 12485, 12498, 12497, 12499, 12484, 12496, 12492, 12495, 12494, 12493, 12483, 12505, 12506, 12511, 12518, 12512, 12513, 12514, 12515, 12516, 12517, 14759, 12491, 12501, 12502, 12503, 12500, 12504, 12519, 12482, 12486, 12490, 12487, 12488, 12489, 14760;

*gerar_relatorio_analise_cnj.py* - Script responsável por:
- leitura dos arquivos *.CSV* regionais e consolidado;
- realização de duas análises de frequência (contagem) e cálculo percentual, relacionadas aos: assuntos mais prevalentes, polos ativos, polos ativos por natureza jurídica, polos passivos e polos passivos por natureza jurídica;
- as **duas análises realizadas** referem-se: a primeira, relacionada todos os processos; a segunda, relacionada aos **processos que tiveram algum ente do SUS como polo passivo** (disponível a partir da página 117 do *.PDF* de exportação, *Output_reports/analise_saude_cnj.pdf*)
- exportação das análises nos formatos *.CSV* e *.PDF*, para posterior avaliação. 

*outros scripts* - Scripts temporários, para suprir tarefas intermediárias, mas disponíveis no repositório para consulta e auxílio na eventual necessidade de esclarecimento do código.