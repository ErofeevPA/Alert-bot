import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date, datetime, timedelta
import io
import sys
import os
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database': 'simulator_20221220',
                      'user': 'student',
                      'password': 'dpo_python_2020'
                      }
# Дефолтные параметры, которые прокидываются в таски

default_args = {
    'owner': 'p-erofeev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5),
    'start_date': datetime(2023, 2, 1),
}
schedule_interval = '*/15 * * * *'
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def erofeev_dag_alert():
    
    @task()
    def extract():
        query = """
                SELECT ts, date, hm, users_message, users_feed, messages, likes, views, CTR
                FROM
                    (SELECT toStartOfFifteenMinutes(time) AS ts,
                    toDate(time) As date,
                    formatDateTime(ts, '%R') AS hm,
                    uniqExact(user_id) AS users_message,
                    COUNT(user_id) AS messages
                    FROM simulator_20221220.message_actions
                    WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts) AS t1
                FULL JOIN
                    (SELECT toStartOfFifteenMinutes(time) AS ts,
                    toDate(time) AS date,
                    formatDateTime(ts, '%R') AS hm,
                    uniqExact(user_id) AS users_feed,
                    countIf(user_id, action='view') AS views,
                    countIf(user_id, action='like') AS likes,
                    countIf(action = 'like')/countIf(action = 'view') AS CTR
                    FROM simulator_20221220.feed_actions
                    WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts) AS t2
                ON t1.ts = t2.ts AND t1.date = t2.date AND t1.hm = t2.hm
                 """
        df_cube = ph.read_clickhouse(query, connection=connection)
        return df_cube

    @task()
    def alerts(df_cube):
        def check_anomaly(df, metric, a = 3, n = 5):
            df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
            df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
            df['iqr'] = df['q75'] - df['q25']
            df['up'] = df['q75'] + a * df['iqr']
            df['down'] = df['q25'] - a * df['iqr']
            df['up'] = df['up'].rolling(n, center = True, min_periods = 1).mean()
            df['down'] = df['down'].rolling(n, center = True, min_periods = 1).mean()  

            if df[metric].iloc[-1] < df['down'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
                is_alert = 1
            else:
                is_alert = 0
            return is_alert, df

        metrics = ['users_feed','users_message','likes','views','CTR','messages']
        for metric in metrics:
            df = df_cube[['ts', 'date', 'hm', metric]].copy()
            is_alert, df_res = check_anomaly(df, metric)
            if is_alert == 1:
                text='''Метрика {metric}:\n Текущее значение = {current_value:.2f}\n Отклонение от предыдущего значения {diff:.2%}\n '''.format(metric = metric, current_value = df_res[metric].iloc[-1], diff = abs(1 - df_res[metric].iloc[-1] / df_res[metric].iloc[-2]))
                bot=telegram.Bot(token='6041309867:AAFvptymKwQi7Ad7qaCpvV_7ExYvsfNtWxU')
                chat_id = -634869587
                bot.sendMessage(chat_id=chat_id, text=text)
                sns.set(rc={'figure.figsize':(20,10)})
                plt.tight_layout()
                ax = sns.lineplot(x = df['ts'],y = df[metric], label= metric, marker = 'o')
                ax = sns.lineplot(x = df['ts'],y = df['up'], label= '{metric} up'.format(metric = metric))
                ax = sns.lineplot(x = df['ts'],y = df['down'], label='{metric} down'.format(metric = metric))

                ax.set(xlabel = 'time')
                ax.set(ylabel = metric)
                ax.set_title(metric, fontsize = 16)
                ax.set(ylim = (0, None))
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'metric_plot.png'
                plt.close()
                bot.sendPhoto(chat_id = chat_id, photo = plot_object)
                
                
    df_cube=extract()
    alerts(df_cube)


erofeev_dag_alert=erofeev_dag_alert()
