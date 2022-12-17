from datetime import datetime
from dateutil import tz
import os, sys
import jquantsapi
import numpy as np
import pandas as pd
import yaml
import time
from snowflake.snowpark import Session

class Snowflake:
    def __init__(self):
        if os.path.exists('../setting/creds.yaml'):
            with open('../setting/creds.yaml') as file:
                conf = yaml.safe_load(file)
                sf_account = conf['snowflake']['account']
                sf_user = conf['snowflake']['username']
                sf_password = conf['snowflake']['password']
                sf_database = conf['snowflake']['database']
                sf_warehouse = conf['snowflake']['warehouse']
                sf_schema = conf['snowflake']['schema']
                sf_role = conf['snowflake']['role']
        else:
            sf_account = os.environ.get('account')
            sf_user = os.environ.get('username')
            sf_password = os.environ.get('password')
            sf_database = os.environ.get('database')
            sf_warehouse = os.environ.get('warehouse')
            sf_schema = os.environ.get('schema')
            sf_role = os.environ.get('role')

        connection_parameters = {
            "account": sf_account,
            "user": sf_user,
            "password": sf_password,
            "database": sf_database,
            "schema": sf_schema,
            "warehouse": sf_warehouse,
            "role" : sf_role
        }

        # セッションの作成＆ロール、DB、スキーマの指定
        self.session = Session.builder.configs(connection_parameters).create()
        self.session.use_role(sf_role)
        self.session.use_database(sf_database)
        self.session.use_schema(sf_schema)

    def resume_task(self, task):
        query = '''alter task %s resume;''' % (task)
        self.session.sql(query).collect()


    def suspend_task(self, task):
        query = '''alter task %s suspend;''' % (task)
        self.session.sql(query).collect()


    def put_stage(self, path, stage):
        self.session.file.put(path, stage, overwrite=True)    


    def copy_table(self, table, stage, format, pattern):
        query = '''copy into %s from %s 
            file_format = (format_name = %s)
            pattern = '%s';''' % (table, stage, format, pattern)
        self.session.sql(query).collect()


    def remove_stage(self, stage):
        query = '''remove %s;''' % (stage)
        self.session.sql(query).collect()


    def convert_column_type(self, df):
        # テーブル作成用のカラム取得
        data_schema = ''
        for (c, d) in zip(df.columns, df.dtypes):
            if str(d) == 'object':
                datatype = 'STRING'
            elif str(d) == 'float64':
                datatype = 'NUMBER'
            elif str(d) == 'datetime64[ns]':
                datatype = 'DATE'
            data_schema += c + ' ' + datatype + ', '

        data_schema = data_schema[0:-2]

        return data_schema

    def replace_table(self, table, schema):
        query = '''create or replace table %s (%s);''' % (table, schema)
        self.session.sql(query).collect()


    def truncate_table(self, table):
        query = '''truncate table %s;''' % (table)
        self.session.sql(query).collect()


class JQuants:
    def __init__(self):
        if os.path.exists('../setting/creds.yaml'):
            with open('../setting/creds.yaml') as file:
                conf = yaml.safe_load(file)
                my_mail_address = conf['jquants']['my_mail_address']
                my_password = conf['jquants']['my_password']
        else:
            my_mail_address = os.environ.get('my_mail_address')
            my_password = os.environ.get('my_password')

        self.jqapi = jquantsapi.Client(mail_address=my_mail_address, password=my_password)

    def get_financial(self, period=4):
        # 3カ月前の株価データ取得
        now = pd.Timestamp.now(tz="Asia/Tokyo")
        start_dt = now - pd.offsets.MonthBegin(period)
        # start_dt = now - pd.offsets.YearBegin(period)
        end_dt = now
        stock_fin_load: pd.DataFrame = self.jqapi.get_statements_range(start_dt=start_dt, end_dt=end_dt)

        # 財務情報のいくつかがobject型になっているので数値型に変換
        numeric_cols_fin = [
            'AverageNumberOfShares', 'BookValuePerShare', 'EarningsPerShare','Equity', 'EquityToAssetRatio',
            'ForecastDividendPerShare1stQuarter', 'ForecastDividendPerShare2ndQuarter', 'ForecastDividendPerShare3rdQuarter',
            'ForecastDividendPerShareAnnual', 'ForecastDividendPerShareFiscalYearEnd', 'ForecastEarningsPerShare', 'ForecastNetSales', 'ForecastOperatingProfit',
            'ForecastOrdinaryProfit', 'ForecastProfit', 'NetSales', 'NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock',
            'OperatingProfit', 'OrdinaryProfit', 'Profit', 'ResultDividendPerShare1stQuarter','ResultDividendPerShare2ndQuarter','ResultDividendPerShare3rdQuarter',
            'ResultDividendPerShareAnnual','ResultDividendPerShareFiscalYearEnd','TotalAssets']
        stock_fin_load[numeric_cols_fin] = stock_fin_load[numeric_cols_fin].apply(pd.to_numeric, errors='coerce', axis=1)

        # 財務情報のobject型をdatetime64[ns]型に変換
        stock_fin_load["DisclosedDate"] = pd.to_datetime(stock_fin_load["DisclosedDate"]) #開示
        stock_fin_load["CurrentFiscalYearEndDate"] = pd.to_datetime(stock_fin_load["CurrentFiscalYearEndDate"])  # 当事業年度終了日
        stock_fin_load["CurrentFiscalYearStartDate"] = pd.to_datetime(stock_fin_load["CurrentFiscalYearStartDate"])  # 当事業年度開始日
        stock_fin_load["CurrentPeriodEndDate"] = pd.to_datetime(stock_fin_load["CurrentPeriodEndDate"]) # 当会計期間終了日

        # 財務情報の値を調整します
        stock_fin_load["Result_FinancialStatementFiscalYear"] = stock_fin_load["CurrentFiscalYearEndDate"].dt.strftime("%Y")

        # 財務情報の同一日に複数レコードが存在することに対応します。
        # ある銘柄について同一日に複数の開示が行われた場合レコードが重複します。
        # ここでは簡易的に処理するために特定のTypeOfDocumentを削除した後に、開示時間順に並べて一番最後に発表された開示情報を採用しています。
        stock_fin_load = stock_fin_load.loc[~stock_fin_load["TypeOfDocument"].isin(["ForecastRevision", "NumericalCorrection", "ForecastRevision_REIT"])]
        stock_fin_load = stock_fin_load.sort_values("DisclosedUnixTime").drop_duplicates(subset=["LocalCode", "DisclosedDate"], keep="last")

        # 普通株 (5桁で末尾が0) の銘柄コードを4桁にします
        stock_fin_load.loc[(stock_fin_load["LocalCode"].str.len() == 5) & (stock_fin_load["LocalCode"].str[-1] == "0"), "LocalCode"] = stock_fin_load.loc[(stock_fin_load["LocalCode"].str.len() == 5) & (stock_fin_load["LocalCode"].str[-1] == "0"), "LocalCode"].str[:-1]

        # コード・日付でソート & インデックス最適化
        stock_fin_load.drop_duplicates(subset=['LocalCode', 'DisclosedDate'], inplace=True)
        stock_fin_load.reset_index(inplace=True, drop=True)

        return stock_fin_load

    def get_stock(self, period=7):
        # 7日間の株価データ取得
        now = pd.Timestamp.now(tz="Asia/Tokyo")
        start_dt = now - pd.offsets.Day(period)
        end_dt = now
        if end_dt.hour < 19:
            end_dt -= pd.Timedelta(1, unit="D")
        stocks = self.jqapi.get_price_range(
            start_dt=start_dt, end_dt=end_dt
        )

        # 結合と重複データ削除
        stocks.drop_duplicates(subset=['Code', 'Date'], inplace=True)
        stocks.reset_index(inplace=True, drop=True)

        # 普通株 (5桁で末尾が0) の銘柄コードを4桁にします
        stocks.loc[(stocks["Code"].str.len() == 5) & (stocks["Code"].str[-1] == "0"), "Code"] = stocks.loc[(stocks["Code"].str.len() == 5) & (stocks["Code"].str[-1] == "0"), "Code"].str[:-1]

        # 株価情報のいくつかがobject型になっているので数値型に変換
        stocks["AdjustmentOpen"] = stocks["AdjustmentOpen"].astype(np.float64)
        stocks["AdjustmentHigh"] = stocks["AdjustmentHigh"].astype(np.float64)
        stocks["AdjustmentLow"] = stocks["AdjustmentLow"].astype(np.float64)
        stocks["AdjustmentClose"] = stocks["AdjustmentClose"].astype(np.float64)
        for item in ['Open','High', 'Low', 'Close', 'Volume', 'Volume', 'TurnoverValue', 'AdjustmentFactor']:
            stocks[item] = stocks[item].astype('float')
        
        return stocks

    def get_stock_code(self):
        # 普通株 (5桁で末尾が0) の銘柄コードを4桁にします
        stock_list = self.jqapi.get_listed_info()
        stock_list.loc[(stock_list["Code"].str.len() == 5) & (stock_list["Code"].str[-1] == "0"), "Code"] = stock_list.loc[(stock_list["Code"].str.len() == 5) & (stock_list["Code"].str[-1] == "0"), "Code"].str[:-1]
        return stock_list

    def get_sector_code(self):
        stock_sectors = self.jqapi.get_listed_sections()
        return stock_sectors

    def get_sector_17_code(self):
        stock_sectors = self.jqapi.get_17_sectors()
        return stock_sectors

    def get_sector_33_code(self):
        stock_sectors = self.jqapi.get_33_sectors()
        return stock_sectors

    def get_market_code(self):
        stock_market = self.jqapi.get_market_segments()
        return stock_market
    
    def get_topix(self):
        topix = self.jqapi.get_indices_topix()
        return topix