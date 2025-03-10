import requests
import pandas as pd
import ast
import time
from datetime import datetime
import logging
from typing import List, Tuple, Optional
import os

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ClienteAPI:
    def __init__(self, url_base: str):
        self.url_base = url_base
        self.token_api = self._carregar_chave_api()  # Lê a chave da API do arquivo .txt
        if not self.token_api:
            raise ValueError("A chave da API não foi encontrada. Verifique o arquivo api_key.txt.")
        self.headers = {'accept': '*/*', 'chave-api-dados': self.token_api}
    
    def get(self, endpoint: str, params: Optional[dict] = None, restrito: bool = False) -> Optional[dict]:
        self._aplicar_limite_taxa(restrito)
        try:
            response = requests.get(f"{self.url_base}{endpoint}", headers=self.headers, params=params)
            response.raise_for_status()  # Lança uma exceção se o status não for 2xx
            logging.info(f"Dados recebidos com sucesso do endpoint {endpoint}!")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao acessar a API: {e}, endpoint: {endpoint}")
            return None
    
    def _aplicar_limite_taxa(self, restrito: bool):
        hora_atual = datetime.now().hour
        if restrito:
            max_requisicoes_por_minuto = 180
        elif 0 <= hora_atual < 6:
            max_requisicoes_por_minuto = 700
        else:
            max_requisicoes_por_minuto = 400
        time.sleep(60 / max_requisicoes_por_minuto)

    def _carregar_chave_api(self) -> str:
        """
        Lê a chave da API do arquivo de texto.
        """
        try:
            with open('api_key.txt', 'r') as file:
                return file.readline().strip().split('=')[1]
        except Exception as e:
            logging.error(f"Erro ao carregar a chave da API do arquivo: {e}")
            return None


class BuscadorContrato:
    def __init__(self, cliente: ClienteAPI, codigo_orgao: str, pagina_inicial: int = 1):
        self.cliente = cliente
        self.codigo_orgao = codigo_orgao
        self.pagina_inicial = pagina_inicial
        self.dataframe = pd.DataFrame()

    def buscar_contratos(self) -> pd.DataFrame:
        pagina = self.pagina_inicial
        todos_os_dados = []

        while True:
            dados = self.cliente.get('/contratos', {'codigoOrgao': self.codigo_orgao, 'pagina': pagina})
            if not dados:
                break
            todos_os_dados.extend(dados)
            pagina += 1

        self.dataframe = pd.DataFrame(todos_os_dados)
        return self.dataframe

    def processar_dataframe(self) -> pd.DataFrame:
        self._expandir_colunas(self.dataframe, 'compra', [
            ('codNumCompra', 'numero'),
            ('objeto_Compra', 'objeto'),
            ('numeroProcesso_Compra', 'numeroProcesso'),
            ('contatoResponsavel_Compra', 'contatoResponsavel')
        ])
        self._expandir_colunas(self.dataframe, 'unidadeGestora', [
            ('codUnidadeGestora', 'codigo'),
            ('nome_UnidadeGestora', 'nome'),
            ('descricaoPoder_UnidadeGestora', 'descricaoPoder'),
            ('orgaoVinculado_codigoSIAFI', 'orgaoVinculado', 'codigoSIAFI'),
            ('orgaoVinculado_cnpj', 'orgaoVinculado', 'cnpj'),
            ('orgaoVinculado_sigla', 'orgaoVinculado', 'sigla'),
            ('orgaoVinculado_nome', 'orgaoVinculado', 'nome'),
            ('orgaoMaximo_codigo', 'orgaoMaximo', 'codigo'),
            ('orgaoMaximo_sigla', 'orgaoMaximo', 'sigla'),
            ('orgaoMaximo_nome', 'orgaoMaximo', 'nome')
        ])
        self._expandir_colunas(self.dataframe, 'fornecedor', [
            ('id_fornecedor', 'id'),
            ('cpfFormatado_fornecedor', 'cpfFormatado'),
            ('cnpjFormatado_fornecedor', 'cnpjFormatado'),
            ('numeroInscricaoSocial_fornecedor', 'numeroInscricaoSocial'),
            ('nome_fornecedor', 'nome'),
            ('razaoSocialReceita_fornecedor', 'razaoSocialReceita'),
            ('nomeFantasiaReceita_fornecedor', 'nomeFantasiaReceita'),
            ('tipo_fornecedor', 'tipo')
        ])
        self._expandir_colunas(self.dataframe, 'unidadeGestoraCompras', [
            ('codigo_UnidadeGestoraCompras', 'codigo'),
            ('nome_UnidadeGestoraCompras', 'nome')
        ])
        
        # Formatando as colunas 'valorInicialCompra' e 'valorFinalCompra' no formato brasileiro
        self._formatar_valores_brasileiros(self.dataframe, ['valorInicialCompra', 'valorFinalCompra'])
        
        return self.dataframe

    def _expandir_colunas(self, df: pd.DataFrame, nome_coluna: str, mapeamentos: List[Tuple[str, str]]) -> None:
        """
        Expande uma coluna que contém dicionários ou strings representando dicionários em múltiplas colunas.
        """
        if nome_coluna in df.columns:
            df[nome_coluna + '_dict'] = df[nome_coluna].apply(self._parse_dict)
            for nova_coluna, *chaves in mapeamentos:
                if len(chaves) == 1:
                    df[nova_coluna] = df[nome_coluna + '_dict'].apply(lambda x: x.get(chaves[0], None))
                else:
                    df[nova_coluna] = df[nome_coluna + '_dict'].apply(lambda x: x.get(chaves[0], {}).get(chaves[1], None))
            df.drop(columns=[nome_coluna + '_dict', nome_coluna], inplace=True)

    @staticmethod
    def _parse_dict(valor: str) -> dict:
        """
        Converte uma string representando um dicionário em um dicionário real.
        """
        try:
            return ast.literal_eval(valor) if isinstance(valor, str) else valor
        except (ValueError, SyntaxError) as e:
            logging.warning(f"Erro ao analisar o dicionário: {e}")
            return {}

    @staticmethod
    def _formatar_valores_brasileiros(df: pd.DataFrame, colunas: List[str]) -> None:
        """
        Formata as colunas numéricas no formato brasileiro (com vírgula para separador decimal e ponto para milhar).
        """
        for coluna in colunas:
            if coluna in df.columns:
                df[coluna] = df[coluna].apply(lambda x: f'{x:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'))
                logging.info(f"Coluna '{coluna}' formatada para o padrão brasileiro.")

# Exemplo de uso
if __name__ == "__main__":
    # Agora a chave da API é carregada automaticamente a partir do arquivo api_key.txt
    cliente_api = ClienteAPI('https://api.portaldatransparencia.gov.br/api-de-dados')
    buscador_contrato = BuscadorContrato(cliente_api, codigo_orgao='20701')

    logging.info("Iniciando a busca de contratos...")
    df_contratos = buscador_contrato.buscar_contratos()
    
    if df_contratos.empty:
        logging.warning("Nenhum contrato encontrado.")
    else:
        logging.info(f"Contratos encontrados: {df_contratos.shape[0]} registros.")
        df_contratos = buscador_contrato.processar_dataframe()

        # Salvar o DataFrame de contratos em um arquivo CSV
        df_contratos.to_csv('contratos_FULL.csv', index=False)
        logging.info("Contratos salvos no arquivo 'contratos.csv'.")

        # Exibir as primeiras linhas do DataFrame de contratos
        logging.info("Exibindo as primeiras linhas dos contratos:")
        logging.info(df_contratos.head())
