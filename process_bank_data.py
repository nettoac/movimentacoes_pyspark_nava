from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, lit, coalesce, lag, round
from pyspark.sql.window import Window
import os
import shutil
import tempfile

# Configurar ambiente Hadoop
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['HADOOP_USER_NAME'] = 'root'

def create_spark_session():
    return SparkSession.builder \
        .appName("BankDataProcessing") \
        .getOrCreate()

def read_csv_file(spark, file_path):
    return spark.read.csv(file_path, header=True, sep=';')

def process_daily_balance(spark):
    # nossa base em arquivos
    saldo_inicial_df = read_csv_file(spark, 'bases/tabela_saldo_inicial.txt')
    mov_dia_02_df = read_csv_file(spark, 'bases/movimentacao_dia_02_04_2022.txt')
    mov_dia_03_df = read_csv_file(spark, 'bases/movimentacao_dia_03_04_2022.txt')

    # Converte colunas numéricas p double
    saldo_inicial_df = saldo_inicial_df.withColumn('Saldo_Inicial_CC', col('Saldo_Inicial_CC').cast('double'))
    mov_dia_02_df = mov_dia_02_df.withColumn('Movimentacao_dia', col('Movimentacao_dia').cast('double'))
    mov_dia_03_df = mov_dia_03_df.withColumn('Movimentacao_dia', col('Movimentacao_dia').cast('double'))

    # coluna de saldo acumulado para rastrear cada movimentação
    window_spec = Window.partitionBy('CPF').orderBy('data')
    mov_dia_02_df = mov_dia_02_df.withColumn('Saldo_Apos_Movimentacao', col('Movimentacao_dia'))
    mov_dia_03_df = mov_dia_03_df.withColumn('Saldo_Apos_Movimentacao', col('Movimentacao_dia'))

    # Calculando todas as movimentações do dia 02.
    mov_dia_02_agg = mov_dia_02_df.groupBy('CPF').agg(sum('Movimentacao_dia').alias('Movimentacao_Total_02'))

    # saldo inicial + movimentações do dia 02
    saldo_dia_02 = saldo_inicial_df.join(mov_dia_02_agg, 'CPF', 'left_outer')
    saldo_dia_02 = saldo_dia_02.withColumn('Saldo_02_04_2022', coalesce(col('Saldo_Inicial_CC') + col('Movimentacao_Total_02'), col('Saldo_Inicial_CC')))

    # filtro que Identifica os estornos do dia 02 que aparecem no arquivo do dia 03...
    estornos_dia_02 = mov_dia_03_df.filter(col('data') == '02/04/2022')
    estornos_dia_02_agg = estornos_dia_02.groupBy('CPF').agg(sum('Movimentacao_dia').alias('Estornos_02'))

    # criacao de um join para adicionar estornos ao saldo do dia 02...
    saldo_dia_02_final = saldo_dia_02.join(estornos_dia_02_agg, 'CPF', 'left_outer')
    saldo_dia_02_final = saldo_dia_02_final.withColumn('Saldo_Final_02_04_2022', coalesce(col('Saldo_02_04_2022') + col('Estornos_02'), col('Saldo_02_04_2022')))

    # filrto p calcular as movimentações do dia 03/04..
    mov_dia_03_real = mov_dia_03_df.filter(col('data') == '03/04/2022')
    mov_dia_03_agg = mov_dia_03_real.groupBy('CPF').agg(sum('Movimentacao_dia').alias('Movimentacao_Total_03'))

    # movimentações do dia 03 ao saldo final
    resultado_final = saldo_dia_02_final.join(mov_dia_03_agg, 'CPF', 'left_outer')
    resultado_final = resultado_final.withColumn('Saldo_Final_03_04_2022', coalesce(col('Saldo_Final_02_04_2022') + col('Movimentacao_Total_03'), col('Saldo_Final_02_04_2022')))

    # Tabela de detalhamento por data
    detalhamento = resultado_final.select(
        'CPF',
        'Nome',
        round(col('Saldo_Inicial_CC'), 2).alias('Saldo_Inicial_01_04_2022'),
        coalesce(round(col('Movimentacao_Total_02'), 2), lit(0.0)).alias('Movimentacao_02_04_2022'),
        coalesce(round(col('Estornos_02'), 2), lit(0.0)).alias('Estorno_02_04_2022'),
        round(col('Saldo_Final_02_04_2022'), 2).alias('Saldo_Final_02_04_2022'),
        coalesce(round(col('Movimentacao_Total_03'), 2), lit(0.0)).alias('Movimentacao_03_04_2022'),
        round(col('Saldo_Final_03_04_2022'), 2).alias('Saldo_Final_03_04_2022')
    ).orderBy('CPF')

    # print do detalhamento por data pode ser log tbm (print so pro teste)
    print("\nRastreabilidade detalhada das movimentações por cliente:")
    print("\nData: 01/04/2022 (Saldo Inicial)")
    detalhamento.select('CPF', 'Nome', 'Saldo_Inicial_01_04_2022').show(truncate=False)

    print("\nMovimentações do dia 02/04/2022:")
    mov_dia_02_df.orderBy('CPF', 'data').show(truncate=False)

    print("\nEstornos do dia 02/04/2022 (registrados em 03/04):")
    estornos_dia_02.orderBy('CPF', 'data').show(truncate=False)

    print("\nSaldo após movimentações e estornos do dia 02/04/2022:")
    detalhamento.select(
        'CPF', 'Nome', 'Saldo_Inicial_01_04_2022',
        'Movimentacao_02_04_2022', 'Estorno_02_04_2022',
        'Saldo_Final_02_04_2022'
    ).show(truncate=False)

    print("\nMovimentações do dia 03/04/2022:")
    mov_dia_03_real.orderBy('CPF', 'data').show(truncate=False)

    print("\nSaldo final após todas as movimentações:")
    detalhamento.select(
        'CPF', 'Nome', 'Saldo_Final_02_04_2022',
        'Movimentacao_03_04_2022', 'Saldo_Final_03_04_2022'
    ).show(truncate=False)

    return detalhamento

# Função para limpar arquivos temporários do spark
def clean_spark_temp_files():
    temp_dir = tempfile.gettempdir()
    spark_temp_pattern = 'spark-*'
    try:
        for item in os.listdir(temp_dir):
            if item.startswith('spark-'):
                item_path = os.path.join(temp_dir, item)
                try:
                    if os.path.isfile(item_path):
                        os.unlink(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                except Exception as e:
                    print(f"Erro ao remover {item_path}: {e}")
    except Exception as e:
        print(f"Erro ao limpar arquivos temporários: {e}")

def main():
    # Limpandos os temps primeiramente para evitar erros de execucao
    clean_spark_temp_files()
    
    spark = create_spark_session()
    
    try:
        resultado = process_daily_balance(spark)
        # mostra o result
        print("\nSaldos atualizados por cliente e data:")
        resultado.show(truncate=False)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()