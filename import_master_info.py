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

def stock_code_import(snowflake, jqapi, stage, format):
    ## Snowflake環境のインポート設定
    tmp_path = './tmp_data/stock_code_update.csv'
    table = 'stock_code'
    pattern = '.*stock_code_update.csv.*'

    ## 情報の取得
    stock_code = jqapi.get_stock_code()

    ## インポート用のCSVファイル取得
    stock_code.to_csv(tmp_path, index=False, encoding='utf8')

    ## インポート用のCSVファイルをステージに転送
    snowflake.put_stage(tmp_path, stage)

    ## スキーマ取得
    schema = snowflake.convert_column_type(stock_code)

    ## テーブル再作成
    snowflake.replace_table(table, schema)

    ## データインポート
    snowflake.copy_table(table, stage, format, pattern)
    os.remove(tmp_path)


def sector_33_import(snowflake, jqapi, stage, format):
    ## Snowflake環境のインポート設定
    tmp_path = './tmp_data/sector_33_update.csv'
    table = 'sector_33_code'
    pattern = '.*sector_33_update.csv.*'

    ## 情報の取得
    stock_sectors = jqapi.get_sector_33_code()

    ## インポート用のCSVファイルをステージに転送
    stock_sectors.to_csv(tmp_path, index=False, encoding='utf8')
    snowflake.put_stage(tmp_path, stage)

    ## スキーマ取得
    schema = snowflake.convert_column_type(stock_sectors)

    ## テーブル再作成
    snowflake.replace_table(table, schema)

    ## データインポート
    snowflake.copy_table(table, stage, format, pattern)
    os.remove(tmp_path)


def sector_17_import(snowflake, jqapi, stage, format):
    ## Snowflake環境のインポート設定
    tmp_path = './tmp_data/sector_17_update.csv'
    table = 'sector_17_code'
    pattern = '.*sector_17_update.csv.*'

    ## 情報の取得
    stock_sectors = jqapi.get_sector_17_code()

    ## インポート用のCSVファイルをステージに転送
    stock_sectors.to_csv(tmp_path, index=False, encoding='utf8')
    snowflake.put_stage(tmp_path, stage)

    ## スキーマ取得
    schema = snowflake.convert_column_type(stock_sectors)

    ## テーブル再作成
    snowflake.replace_table(table, schema)

    ## データインポート
    snowflake.copy_table(table, stage, format, pattern)
    os.remove(tmp_path)

def market_import(snowflake, jqapi, stage, format):
    ## Snowflake環境のインポート設定
    tmp_path = './tmp_data/market_update.csv'
    table = 'market_code'
    pattern = '.*market_update.csv.*'

    ## 情報の取得
    stock_market = jqapi.get_market_code()

    ## インポート用のCSVファイルをステージに転送
    stock_market.to_csv(tmp_path, index=False, encoding='utf8')
    snowflake.put_stage(tmp_path, stage)

    ## スキーマ取得
    schema = snowflake.convert_column_type(stock_market)

    ## テーブル再作成
    snowflake.replace_table(table, schema)

    ## データインポート
    snowflake.copy_table(table, stage, format, pattern)
    os.remove(tmp_path)


if __name__ == "__main__":
    print("----------------------- Job Start -----------------------")

    ## Classの定義
    snowflake = Snowflake()
    jqapi = JQuants()

    ## 各データのインポート
    stage = '@data_csv'
    format = 'file_csv'

    # 株価コードのインポート
    stock_code_import(snowflake, jqapi, stage, format)
    print("----------------------- Complete StockCode Import -----------------------")

    # 33セクターコードのインポート
    sector_33_import(snowflake, jqapi, stage, format)
    print("----------------------- Complete SectorCode Import -----------------------")

    # 17セクターコードのインポート
    sector_17_import(snowflake, jqapi, stage, format)
    print("----------------------- Complete SectorCode Import -----------------------")

    # 市場コードのインポート
    market_import(snowflake, jqapi, stage, format)
    print("----------------------- Complete MarketCode Import -----------------------")

    ## ステージからファイルをリムーブ
    snowflake.remove_stage(stage)