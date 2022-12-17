from datetime import datetime
from dateutil import tz
import os, sys
import jquantsapi
import numpy as np
import pandas as pd
import yaml
import time
from snowflake.snowpark import Session
from script.connect import Snowflake
from script.connect import JQuants

def finance_import(snowflake, jqapi, stage, format):
    ## Snowflake環境のインポート設定
    tmp_path = './tmp_data/fin_update.csv'
    table = 'financial_update'
    pattern = '.*fin_update.csv.*'

    ## 情報の取得
    stock_fin = jqapi.get_financial(period=4)

    ## インポート用のCSVファイルをステージに転送
    stock_fin.to_csv(tmp_path, index=False, encoding='utf8')
    snowflake.put_stage(tmp_path, stage)

    ## データインポート
    snowflake.copy_table(table, stage, format, pattern)

    ## ゴミ削除
    os.remove(tmp_path)
    snowflake.truncate_table(table)

def stock_import(snowflake, jqapi, stage, format):
    ## Snowflake環境のインポート設定
    tmp_path = './tmp_data/stocks_value_update.csv'
    table = 'stock_update'
    pattern = '.*stocks_value_update.csv.*'

    ## 情報の取得
    stocks = jqapi.get_stock(period=7)

    ## インポート用のCSVファイルをステージに転送
    stocks.to_csv(tmp_path, index=False, encoding='cp932')
    snowflake.put_stage(tmp_path, stage)

    ## データインポート
    snowflake.copy_table(table, stage, format, pattern)

    ## ゴミ削除
    os.remove(tmp_path)
    snowflake.truncate_table(table)


def topix_import(snowflake, jqapi, stage, format):
    ## Snowflake環境のインポート設定
    tmp_path = './tmp_data/topix_value_update.csv'
    table = 'topix_update'
    pattern = '.*topix_value_update.csv.*'

    ## 情報の取得
    topix = jqapi.get_topix()

    ## インポート用のCSVファイルをステージに転送
    topix.to_csv(tmp_path, index=False, encoding='cp932')
    snowflake.put_stage(tmp_path, stage)

    ## データインポート
    snowflake.copy_table(table, stage, format, pattern)

    ## ゴミ削除
    os.remove(tmp_path)
    snowflake.truncate_table(table)

if __name__ == "__main__":
    print("----------------------- Job Start -----------------------")
    
    ## Classの定義
    snowflake = Snowflake()
    jqapi = JQuants()

    ## Taskの開始
    snowflake.resume_task('stock_update_task')
    snowflake.resume_task('financial_update_task')
    snowflake.resume_task('topix_update_task')

    print("----------------------- Complete Resume Task -----------------------")

    ## 各データのインポート
    stage = '@data_csv'
    format = 'file_csv'
    finance_import(snowflake, jqapi, stage, format)
    print("----------------------- Complete Finance Import -----------------------")

    stock_import(snowflake, jqapi, stage, format)
    print("----------------------- Complete Stock Import -----------------------")
    
    topix_import(snowflake, jqapi, stage, format)
    print("----------------------- Complete Topix Import -----------------------")

    ## ステージからファイルをリムーブ
    snowflake.remove_stage(stage)

    ## Taskの停止
    time.sleep(90)
    snowflake.suspend_task('stock_update_task')
    snowflake.suspend_task('financial_update_task')
    snowflake.suspend_task('topix_update_task')
    print("----------------------- Complete Suspend Task -----------------------")
