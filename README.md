# Sistema de Processamento de Dados Bancários

Este sistema processa dados de movimentações bancárias e calcula saldos atualizados usando PySpark. O sistema lê arquivos de saldo inicial e movimentações diárias, processa os dados considerando estornos e novas movimentações, e gera uma tabela com saldos atualizados por cliente e data.

## Requisitos

- Python 3.7 ou superior
- Java 8 ou superior (necessário para o PySpark)
- PySpark
- Hadoop e Winutils (necessário para PySpark no Windows)

## Instalação

1. Certifique-se de ter o Python instalado em seu sistema
2. Instale o PySpark usando pip:

```bash
pip install pyspark
```

3. Configuração do Hadoop e Winutils (necessário para Windows):

   a. Crie uma pasta `C:\hadoop`
   
   b. Baixe o Hadoop binário para Windows:
      - Acesse https://github.com/steveloughran/winutils
      - Escolha a versão compatível com seu PySpark (recomendado: hadoop-3.0.0)
      - Baixe o arquivo `winutils.exe`
      - Coloque o arquivo `winutils.exe` na pasta `C:\hadoop\bin`
   
   c. Configure as variáveis de ambiente:
      - Adicione `HADOOP_HOME=C:\hadoop` às variáveis de ambiente do sistema
      - Adicione `%HADOOP_HOME%\bin` ao PATH do sistema

   d. Reinicie seu terminal/IDE para aplicar as alterações

## Estrutura de Arquivos

```
bases/
  ├── tabela_saldo_inicial.txt
  ├── movimentacao_dia_02_04_2022.txt
  └── movimentacao_dia_03_04_2022.txt
process_bank_data.py
README.md
```

## Formato dos Arquivos de Entrada

### tabela_saldo_inicial.txt
- Contém o saldo inicial dos clientes em 01/04/2022
- Formato: Nome;CPF;Saldo_Inicial_CC;data

### movimentacao_dia_02_04_2022.txt e movimentacao_dia_03_04_2022.txt
- Contém as movimentações diárias
- Formato: Nome;CPF;Movimentacao_dia;data

## Como Executar

1. Certifique-se de que todos os arquivos de dados estão na pasta `bases/`
2. Execute o script principal:

```bash
python process_bank_data.py
```

## Saída

O sistema irá exibir uma tabela com as seguintes informações para cada cliente:
- CPF
- Nome
- Saldo Inicial (01/04/2022)
- Saldo Final (02/04/2022)
- Saldo Final (03/04/2022)

## Funcionalidades

- Processamento de saldos diários
- Cálculo de movimentações por cliente
- Tratamento de estornos em datas posteriores
- Rastreabilidade de saldos entre dias
- Manutenção do histórico de saldos para novos clientes

## Observações

- O sistema considera estornos que aparecem em datas posteriores
- Mantém o saldo do dia anterior quando não há movimentação
- Inclui novos clientes que aparecem nas movimentações
- Todos os cálculos são feitos usando o PySpark para processamento eficiente

## Problema Proposto

- Tecnologia: Pyspark
 * Simular os resultados em dataframes e apresentar o resultado.
 Objetivo: Apresentar o saldo atualizado da conta corrente de todos os clientes separados por data onde seja possível reprocessar/identificar as alterações de saldos entre os dias.
- Ex:

- Dia 02/04/2022:
 * Cliente 01 - Saldo Inicial 100,00 (01/04/2022) + Movimentação do dia 02/04/2022 - 50,00 saldo final do dia 02/04/2022 50,00
          Saldo final do cliente no dia 02/04/2022 = 50,00

- Dia 03/04/2022:
 * Cliente 01 - Saldo Inicial 50,00 (Saldo calculado no final do dia 02/04/2022) + Movimentações (03/04/2022) 50,00 + (02/04/2022) + 50,00 (estorno ao cliente)
         Saldo final do dia 02/04/2022 = 100,00
         Saldo final do dia 03/04/2022 = 150,00

* Os movimentos de estorno ou adição de saldos sempre acontecerão em datas posteriores as processadas, por exemplo,
      o estorno de 50,00 do dia 02/04/2022 veio no arquivo de movimentos do dia 03/04/2022 e por esse motivo se faz necessário o
 reprocessamento do saldo do dia 02/04/2022.
 
* À partir do cálculo do saldo inicial do cliente o mesmo deve aparecer para todas as próximas datas, caso não ocorra movimentação manter o saldo do dia anterior.
* À partir do momento que apareçam novos clientes nos arquivos de movimentação, deve-se manter o saldo do cliente mesmo não havendo mais movimentação nos dias posteriores.

- Resultado final:
 * Tabela contendo o saldo atualizado de todos os clientes por data, onde seja possível ter uma rastreabilidade entre um dia e outro.
Ex:
- 02/04/2022:
      02/04/2022: Cliente 01 - 50,00 (Saldo Final)

- 03/04/2022:
      02/04/2022 Cliente 01 - 100,00 (Saldo Final)
      03/04/2022 Cliente 01 - 150,00 (Saldo Final)