import pandas as pd
import numpy as np
from datetime import timedelta
from sqlalchemy import create_engine
import os


def read_and_prepare_entry_data(file_path):
    df = pd.read_csv(file_path, header=None, low_memory=False, dtype=str)
    df.columns = [
        'ID do Apontamento', 'Numero da OS', 'Numero da Etapa', 'Numero do Apontamento',
        'Numero do Equipamento', 'Nome do Equipamento', 'Tipo de Apontamento', 'Numero do Funcionário',
        'Nome do Funcionário', 'Quantidade Prevista', 'Inicio', 'Fim', 'Tempo Gasto', 'Quantidade Produzida',
        'Produção Média', 'Código de Parada de Maquina', 'Custo Hora', 'Custo Total',
    ]
    regex = r'^\d{6}[A-Za-z]?$'
    df.dropna(subset=['Numero da OS', 'Numero do Equipamento'], inplace=True)
    df[['Inicio', 'Fim']] = df[['Inicio', 'Fim']].apply(pd.to_datetime, format='%d/%m/%Y-%H:%M', errors='coerce')
    df['Tempo Gasto'] = df['Tempo Gasto'].str.replace(',', '.').astype('float64')
    df['Custo Hora'] = df['Custo Hora'].str.replace(',', '.').astype('float64')
    df['Custo Total'] = df['Custo Total'].str.replace('.', '').str.replace(',', '.').astype('float64')
    df['Quantidade Produzida'] = df['Quantidade Produzida'].str.replace('.', '').astype('Int64')
    df['Quantidade Prevista'] = df['Quantidade Prevista'].str.replace('.', '').str.replace(',', '.').astype(float).astype('Int64')
    df['Produção Média'] = pd.to_numeric(df['Produção Média'].str.replace('.', ''), errors='coerce').astype('Int64')
    df['Numero da Etapa'] = df['Numero da Etapa'].str[-3:].astype('Int64', errors='ignore').fillna(0)
    df['Quantidade Prevista Max'] = df.groupby('Numero da OS')['Quantidade Prevista'].transform('max')
    df = df[df['Numero da OS'].str.match(regex)]
    return df


def read_and_prepare_budget_data(file_path):
    df = pd.read_csv(file_path, encoding="latin-1", delimiter="\t", header=None, low_memory=False, dtype=str)
    df.columns = [
        'Numero da OS', 'Numero da Etapa', 'Tipo de Etapa', 'Descrição', 'Tipo de OS', 'Código da Maquina',
        'Custo por Unidade', 'Acerto Previsto', 'Numero de Entradas', 'Produção Prevista', 'Acerto Efetivo',
        'Produção Efetiva', 'Unidade de Medida', 'Custo Total', 'Impressões', 'Horas no PCP', 'Numero da Faca',
        'Numero do Equipamento PCP', 'Status no PCP', 'Ordem', 'Porcentagem Realizada', 'Quantidade Impressa',
        'Inicio', 'Inicio Calculado', 'Fim Calculado', 'Data de Termino', 'Código de Parada', 'Custo Orçado'
    ]
    df['Custo por Unidade'] = df['Custo por Unidade'].str.replace('.', '').str.replace(',', '.').astype('float64')
    df['Acerto Previsto'] = df['Acerto Previsto'].str.replace(',', '.').astype('float64')
    df['Produção Prevista'] = df['Produção Prevista'].str.replace('.', '').str.replace(',', '.').astype('float64')
    df['Acerto Efetivo'] = df['Acerto Efetivo'].str.replace(',', '.').astype('float64')
    df['Produção Efetiva'] = df['Produção Efetiva'].str.replace(',', '.').astype('float64')
    df['Custo Total'] = df['Custo Total'].str.replace('.', '').str.replace(',', '.').astype('float64')
    df['Horas no PCP'] = df['Horas no PCP'].str.replace(',', '.').astype('float64')
    df['Quantidade Impressa'] = df['Quantidade Impressa'].str.replace('.', '').str.replace(',', '.').astype('float64')
    df['Numero da Etapa'] = df['Numero da Etapa'].astype('Int64')
    df[['Inicio', 'Inicio Calculado', 'Fim Calculado', 'Data de Termino']] = df[
        ['Inicio', 'Inicio Calculado', 'Fim Calculado', 'Data de Termino']].apply(pd.to_datetime, format='%d/%m/%Y', errors='coerce')
    df['Custo Orçado'] = df['Custo Orçado'].str.replace('.', '').str.replace(',', '.').astype('float64')
    regex = r'^\d{6}[A-Za-z]?$'
    df = df[df['Numero da OS'].str.match(regex)]
    return df


def adjust_date(data_fim):
    if pd.isnull(data_fim):
        return pd.NaT
    if data_fim.time() < pd.Timestamp('6:35').time():
        return (data_fim - timedelta(days=1)).date()
    else:
        return data_fim.date()


def calculate_shift(row):
    horario_turno_3 = pd.Timestamp('06:00:00').time()
    horario_turno_1 = pd.Timestamp('14:30:00').time()
    horario_turno_2 = pd.Timestamp('22:20:00').time()

    inicio = row['Inicio'].time()
    duracao = row['Duração']

    if inicio < pd.Timestamp('06:00').time() and duracao <= 30:
        return '3 Turno'
    elif inicio < pd.Timestamp('14:00').time() and duracao <= 30:
        return '1 Turno'
    elif inicio < pd.Timestamp('22:00').time() and duracao <= 30:
        return '2 Turno'
    else:
        return 'Sem Registro'


def compute_gold_hour_press(df_entry, df_budget):
    df_entry_filtered = df_entry[df_entry['Tipo de Apontamento'] != 'Ocioso']
    df_entry_press = df_entry_filtered[df_entry_filtered['Numero da Etapa'] == 201].drop(
        columns=['ID do Apontamento', 'Numero do Apontamento', 'Numero do Funcionário', 'Produção Média',
                 'Código de Parada de Maquina', 'Custo Hora', 'Custo Total'])
    df_budget_completed = df_budget[df_budget['Status no PCP'] == 'Concluído'].drop(
        columns=['Tipo de Etapa', 'Descrição', 'Tipo de OS', 'Custo por Unidade', 'Numero de Entradas',
                 'Acerto Efetivo', 'Produção Efetiva', 'Unidade de Medida', 'Custo Total', 'Impressões',
                 'Horas no PCP', 'Numero da Faca', 'Numero do Equipamento PCP', 'Ordem', 'Porcentagem Realizada',
                 'Quantidade Impressa', 'Inicio', 'Inicio Calculado', 'Fim Calculado', 'Data de Termino',
                 'Código de Parada', 'Custo Orçado'])
    df_budget_press = df_budget_completed[df_budget_completed['Numero da Etapa'] == 201]
    df_gold_hour_press = pd.merge(df_entry_press, df_budget_press, on='Numero da OS')
    df_gold_hour_press['Acerto Realizado'] = np.where(
        df_gold_hour_press['Tipo de Apontamento'] == 'Acerto', df_gold_hour_press['Tempo Gasto'], 0)
    df_gold_hour_press['Produção Realizada'] = np.where(
        df_gold_hour_press['Tipo de Apontamento'] == 'Produção', df_gold_hour_press['Tempo Gasto'], 0)
    df_gold_hour_press = df_gold_hour_press.drop(columns=['Tempo Gasto', 'Tipo de Apontamento'])
    df_gold_hour_press['Data para Calculo'] = df_gold_hour_press['Fim'].dt.date
    df_gold_hour_press = df_gold_hour_press.groupby(
        ['Numero da OS', 'Nome do Funcionário', 'Data para Calculo'], as_index=False).agg({
        'Nome do Equipamento': 'first',
        'Inicio': 'max',
        'Fim': 'max',
        'Acerto Previsto': 'max',
        'Produção Prevista': 'max',
        'Quantidade Prevista Max': 'max',
        'Quantidade Produzida': 'max',
        'Acerto Realizado': 'sum',
        'Produção Realizada': 'sum',
    })
    df_gold_hour_press['Quantidade Produzida Max'] = df_gold_hour_press.groupby('Numero da OS')[
        'Quantidade Produzida'].transform('sum')
    df_gold_hour_press['Quantidade Produzida 10%'] = df_gold_hour_press.apply(
        lambda x: min(x['Quantidade Produzida Max'], x['Quantidade Prevista Max'] * 1.10), axis=1)
    df_gold_hour_press['Produção Prevista Real'] = (df_gold_hour_press['Quantidade Produzida 10%'] * df_gold_hour_press['Produção Prevista']) / df_gold_hour_press['Quantidade Prevista Max']
    sum_of_setup = df_gold_hour_press.groupby('Numero da OS')['Acerto Realizado'].transform('sum')
    sum_of_press = df_gold_hour_press.groupby('Numero da OS')['Produção Realizada'].transform('sum')
    df_gold_hour_press['Contagem de OS'] = df_gold_hour_press.groupby('Numero da OS')['Numero da OS'].transform('count')
    df_gold_hour_press['Hora Ouro do Acerto'] = np.where(
        df_gold_hour_press['Contagem de OS'] < 2, df_gold_hour_press['Acerto Previsto'],
        (df_gold_hour_press['Acerto Previsto'] / sum_of_setup) * df_gold_hour_press['Acerto Realizado'])
    df_gold_hour_press['Hora Ouro da Produção'] = np.where(
        df_gold_hour_press['Contagem de OS'] < 2, df_gold_hour_press['Produção Prevista Real'],
        (df_gold_hour_press['Produção Prevista Real'] / sum_of_press) * df_gold_hour_press['Produção Realizada'])
    df_gold_hour_press.replace([float('inf'), float('-inf'), pd.NA], 0.0, inplace=True)
    df_gold_hour_press = df_gold_hour_press.round(decimals=3).drop(columns=['Contagem de OS'])
    df_gold_hour_press['Hora Ouro Total'] = df_gold_hour_press['Hora Ouro do Acerto'].fillna(0) + df_gold_hour_press[
        'Hora Ouro da Produção'].fillna(0)
    total_acerto_previsto_por_os = df_gold_hour_press.groupby('Numero da OS')['Acerto Previsto'].transform('max')
    total_producao_prevista_por_os = df_gold_hour_press.groupby('Numero da OS')['Produção Prevista'].transform('max')
    soma_acerto_realizado_por_os = df_gold_hour_press.groupby('Numero da OS')['Acerto Realizado'].transform('sum')
    proporcao_acerto = df_gold_hour_press['Acerto Realizado'] / soma_acerto_realizado_por_os
    soma_producao_realizada_por_os = df_gold_hour_press.groupby('Numero da OS')['Produção Realizada'].transform('sum')
    proporcao_producao = df_gold_hour_press['Produção Realizada'] / soma_producao_realizada_por_os
    df_gold_hour_press['Hora Ouro Meta do Acerto'] = total_acerto_previsto_por_os * proporcao_acerto
    df_gold_hour_press['Hora Ouro Meta da Produção'] = total_producao_prevista_por_os * proporcao_producao
    df_gold_hour_press['Hora Ouro Meta'] = df_gold_hour_press['Hora Ouro Meta do Acerto'] + df_gold_hour_press[
        'Hora Ouro Meta da Produção']
    df_gold_hour_press.replace([float('inf'), float('-inf'), pd.NA], 0.0, inplace=True)
    df_gold_hour_press = df_gold_hour_press.round(decimals=3)
    df_gold_hour_press['Data de Conclusão'] = df_gold_hour_press['Fim'].apply(adjust_date)
    df_gold_hour_press = df_gold_hour_press[df_gold_hour_press['Quantidade Produzida Max'] > 0]
    df_gold_hour_press['Duração'] = (df_gold_hour_press['Fim'] - df_gold_hour_press['Inicio']).dt.total_seconds() / 60
    df_gold_hour_press['Turno'] = df_gold_hour_press.apply(calculate_shift, axis=1)

    return df_gold_hour_press


def compute_final_dataframe(df_gold_hour_press):
    df_aggregate = df_gold_hour_press.groupby(['Nome do Funcionário', 'Data de Conclusão', 'Nome do Equipamento']).agg({
        'Acerto Previsto': 'sum',
        'Acerto Realizado': 'sum',
        'Produção Prevista': 'sum',
        'Produção Prevista Real': 'sum',
        'Produção Realizada': 'sum',
        'Quantidade Prevista Max': 'sum',
        'Quantidade Produzida': 'sum',
        'Quantidade Produzida 10%': 'sum',
        'Hora Ouro da Produção': 'sum',
        'Hora Ouro do Acerto': 'sum',
        'Hora Ouro Total': 'sum',
        'Hora Ouro Meta do Acerto': 'sum',
        'Hora Ouro Meta da Produção': 'sum',
        'Hora Ouro Meta': 'sum',
    }).reset_index()
    df_final = df_aggregate[[
        'Nome do Funcionário', 'Data de Conclusão', 'Nome do Equipamento',
        'Hora Ouro Total', 'Hora Ouro Meta', 'Hora Ouro da Produção', 'Hora Ouro do Acerto',
        'Hora Ouro Meta da Produção', 'Hora Ouro Meta do Acerto', 'Acerto Previsto', 'Produção Prevista Real'
    ]]
    return df_final


def compute_cumulative_hours(df_final):
    df_final['Data de Conclusão'] = pd.to_datetime(df_final['Data de Conclusão'])
    df_final['month'] = df_final['Data de Conclusão'].dt.to_period('M')
    grouped = df_final.groupby(['Nome do Equipamento', 'month', 'Data de Conclusão']).agg({
        'Hora Ouro Total': 'sum',
        'Hora Ouro Meta': 'sum',
        'Acerto Previsto': 'sum',
        'Produção Prevista Real': 'sum',
    }).reset_index()
    grouped = grouped.sort_values(['Nome do Equipamento', 'Data de Conclusão'])
    grouped['hora ouro acumulada'] = grouped.groupby(['Nome do Equipamento', 'month'])['Hora Ouro Total'].cumsum()
    grouped['hora ouro meta acumulada'] = grouped.groupby(['Nome do Equipamento', 'month'])['Hora Ouro Meta'].cumsum()
    final_df = grouped[[
        'Data de Conclusão', 'Nome do Equipamento', 'Hora Ouro Total',
        'hora ouro acumulada', 'Hora Ouro Meta', 'hora ouro meta acumulada'
    ]]
    final_df = final_df.rename(columns={'Hora Ouro Total': 'Hora Ouro'})
    return final_df


# Caminhos e conexões
path = "app/ref"
connection_string = 'postgresql://caiop:asdf@localhost:5432/astro'
engine = create_engine(connection_string)
file1 = os.path.join(path, "DataSync - Apontamento.txt")
file2 = os.path.join(path, "DataSync - Ordem de produção.txt")

# Leitura e preparação dos dados
df_entry = read_and_prepare_entry_data(file1)
df_budget = read_and_prepare_budget_data(file2)

# Cálculo das Horas Ouro
df_gold_hour_press = compute_gold_hour_press(df_entry, df_budget)
df_final = compute_final_dataframe(df_gold_hour_press)
final_df = compute_cumulative_hours(df_final)