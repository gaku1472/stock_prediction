# -*- coding: utf-8 -*-
import io
import os
import pickle

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.ensemble import RandomForestRegressor
from tqdm.auto import tqdm
from pyti.ichimoku_cloud import tenkansen, kijunsen, chiku_span, senkou_a, senkou_b
from pyti.bollinger_bands import upper_bollinger_band, middle_bollinger_band, lower_bollinger_band, bandwidth, percent_bandwidth, range
from pyti.relative_strength_index import relative_strength_index
from pyti.exponential_moving_average import exponential_moving_average
from pyti.weighted_moving_average import weighted_moving_average
from pyti.volume_adjusted_moving_average import volume_adjusted_moving_average
from pyti.moving_average_convergence_divergence import moving_average_convergence_divergence
from pyti.stochastic import percent_d, percent_k

class ScoringService(object):
    TRAIN_END = "2018-12-31" # 訓練期間終了日
    VAL_START = "2019-02-01" # 評価期間開始日
    VAL_END = "2019-12-01" # 評価期間終了日
    TEST_START = "2020-01-01" # テスト期間開始日
    TARGET_LABELS = ["label_high_20", "label_low_20"] # 目的変数

    # データをこの変数に読み込む
    dfs = None
    # モデルをこの変数に読み込む
    models = None
    # 対象の銘柄コードをこの変数に読み込む
    codes = None

    @classmethod
    def get_inputs(cls, dataset_dir):
        """
        Args:
            dataset_dir (str)  : path to dataset directory
        Returns:
            dict[str]: path to dataset files
        """
        inputs = {
            "stock_list": f"{dataset_dir}/stock_list.csv.gz",
            "stock_price": f"{dataset_dir}/stock_price.csv.gz",
            "stock_fin": f"{dataset_dir}/stock_fin.csv.gz",
            # "stock_fin_price": f"{dataset_dir}/stock_fin_price.csv.gz",
            "stock_labels": f"{dataset_dir}/stock_labels.csv.gz",
        }
        return inputs

    @classmethod
    def get_dataset(cls, inputs):
        """
        Args:
            inputs (list[str]): path to dataset files
        Returns:
            dict[pd.DataFrame]: loaded data
        """
        if cls.dfs is None:
            cls.dfs = {}
        for k, v in inputs.items():
            cls.dfs[k] = pd.read_csv(v)
        return cls.dfs

    @classmethod
    def get_codes(cls, dfs):
        stock_list = dfs["stock_list"].copy()

        # 予測対象の銘柄コードを取得
        cls.codes = stock_list[stock_list["prediction_target"] == True]["Local Code"].values

        # 本番はこのコメントアウトを除外
        return cls.codes

        rondom_list = []
        for k in range(150):
            # x = random.randint(1,len(codes)-1)
            rondom_list.append(k)
    
        limit_codes = cls.codes[rondom_list]

        return limit_codes

    @classmethod
    def get_features_and_label(cls, dfs, codes, feature, label):
        """
        Args:
            dfs (dict[pd.DataFrame]): loaded data
            codes  (array) : target codes
            feature (pd.DataFrame): features
            label (str) : label column name
        Returns:
            train_X (pd.DataFrame): training data
            train_y (pd.DataFrame): label for train_X
            val_X (pd.DataFrame): validation data
            val_y (pd.DataFrame): label for val_X
            test_X (pd.DataFrame): test data
            test_y (pd.DataFrame): label for test_X
        """
        # 分割データ用の変数を定義
        trains_X, vals_X, tests_X = [], [], []
        trains_y, vals_y, tests_y = [], [], []

        # 銘柄コード毎に特徴量を作成
        for code in tqdm(codes):
            # 特徴量取得
            feats = feature[feature["code"] == code]

            # stock_labelデータを読み込み
            stock_labels = dfs["stock_labels"].copy()
            # 特定の銘柄コードのデータに絞る
            stock_labels = stock_labels[stock_labels["Local Code"] == code]
            # 日付列をpd.Timestamp型に変換してindexに設定
            stock_labels["datetime"] = pd.to_datetime(stock_labels["base_date"])
            stock_labels.set_index("datetime", inplace=True)

            # 特定の目的変数に絞る
            labels = stock_labels[label]
            # nanを削除
            labels.dropna(inplace=True)

            if feats.shape[0] > 0 and labels.shape[0] > 0:
                # 特徴量と目的変数のインデックスを合わせる
                labels = labels.loc[labels.index.isin(feats.index)]
                feats = feats.loc[feats.index.isin(labels.index)]
                labels.index = feats.index

                # データを分割
                _train_X = feats[: cls.TRAIN_END].copy()
                _val_X = feats[cls.VAL_START : cls.VAL_END].copy()
                _test_X = feats[cls.TEST_START :].copy()

                _train_y = labels[: cls.TRAIN_END].copy()
                _val_y = labels[cls.VAL_START : cls.VAL_END].copy()
                _test_y = labels[cls.TEST_START :].copy()

                # データを配列に格納 (後ほど結合するため)
                trains_X.append(_train_X)
                vals_X.append(_val_X)
                tests_X.append(_test_X)

                trains_y.append(_train_y)
                vals_y.append(_val_y)
                tests_y.append(_test_y)
        # 銘柄毎に作成した説明変数データを結合します。
        train_X = pd.concat(trains_X)
        val_X = pd.concat(vals_X)
        test_X = pd.concat(tests_X)
        # 銘柄毎に作成した目的変数データを結合します。
        train_y = pd.concat(trains_y)
        val_y = pd.concat(vals_y)
        test_y = pd.concat(tests_y)

        return train_X, train_y, val_X, val_y, test_X, test_y

    @classmethod
    def get_features_for_predict(cls, dfs, code, start_dt="2016-01-01"):
        ################## stock_fin_feat ##################
        # stock_finデータを読み込み
        stock_fin = dfs["stock_fin"].copy()

        # 特定の銘柄コードのデータに絞る
        fin_data = stock_fin[stock_fin["Local Code"] == code].copy()

        # 業界データと結合
        stock_list = dfs["stock_list"].copy()
        fin_list = stock_list[["Local Code", "33 Sector(name)", "17 Sector(name)", "IssuedShareEquityQuote IssuedShare"]].copy()
        fin_data = pd.merge(fin_data, fin_list, how="left", on="Local Code")

        # 日付列をpd.Timestamp型に変換してindexに設定
        fin_data["datetime"] = pd.to_datetime(fin_data["base_date"])
        fin_data.set_index("datetime", inplace=True)

        # fin_dataのnp.float64のデータのみを取得
        fin_data = fin_data.select_dtypes(include=["float64"])

        # 企業
        ## 営業利益率
        fin_data["Rate_Operating_Margin"] = fin_data["Result_FinancialStatement OperatingIncome"] / fin_data["Result_FinancialStatement NetSales"] * 100
        ## 経常利益率
        fin_data["Rate_Ordinary_Margin"] = fin_data["Result_FinancialStatement OrdinaryIncome"] / fin_data["Result_FinancialStatement NetSales"] * 100
        ## 純利益率
        fin_data["Rate_Ordinary_Margin"] = fin_data["Result_FinancialStatement NetIncome"] / fin_data["Result_FinancialStatement NetSales"] * 100
        ## 売上成長率
        fin_data["Rate_Grow_NetSales"] = fin_data["Result_FinancialStatement NetSales"].pct_change(4)
        ## 営業利益成長率
        fin_data["Rate_Grow_Operating"] = fin_data["Result_FinancialStatement OperatingIncome"].pct_change(4)
        ## 経常利益成長率
        fin_data["Rate_Grow_Ordinary"] = fin_data["Result_FinancialStatement OrdinaryIncome"].pct_change(4)
        ## 純利益成長率
        fin_data["Rate_Grow_NetIncome"] = fin_data["Result_FinancialStatement NetIncome"].pct_change(4)   
        ## ROE
        fin_data["ROE"] = fin_data["Result_FinancialStatement NetIncome"] / fin_data["Result_FinancialStatement NetAssets"] * 100
        ## ROA
        fin_data["ROA"] = fin_data["Result_FinancialStatement NetIncome"] / fin_data["Result_FinancialStatement TotalAssets"] * 100
        ## EPS
        fin_data["EPS"] = fin_data["Result_FinancialStatement NetIncome"] / fin_data["IssuedShareEquityQuote IssuedShare"] * 100
        ## BPS
        fin_data["BPS"] = fin_data["Result_FinancialStatement TotalAssets"] / fin_data["IssuedShareEquityQuote IssuedShare"]
        ## ROE成長率
        fin_data["Rate_Grow_ROE"] = fin_data["ROE"].pct_change(4)
        ## ROA成長率
        fin_data["Rate_Grow_ROA"] = fin_data["ROA"].pct_change(4)
        ## 負債比率
        fin_data["Result_FinancialStatement Liability"] = fin_data["Result_FinancialStatement TotalAssets"] - fin_data["Result_FinancialStatement NetAssets"]
        fin_data["Rate Liability"] = fin_data["Result_FinancialStatement Liability"] / fin_data["Result_FinancialStatement NetAssets"] * 100
        ## 来季結果と今期予測の差異
        fin_forecast = fin_data[["Forecast_FinancialStatement NetSales", "Forecast_FinancialStatement OperatingIncome", "Forecast_FinancialStatement OrdinaryIncome", "Forecast_FinancialStatement NetIncome"]].diff(4)
        fin_data["Diff Forecast Result NetSales"] = fin_forecast["Forecast_FinancialStatement NetSales"] / fin_data["Result_FinancialStatement NetSales"] * 100
        fin_data["Diff Forecast Result OperatingIncome"] = fin_forecast["Forecast_FinancialStatement OperatingIncome"] / fin_data["Result_FinancialStatement OperatingIncome"] * 100
        fin_data["Diff Forecast Result OrdinaryIncome"] = fin_forecast["Forecast_FinancialStatement OrdinaryIncome"] / fin_data["Result_FinancialStatement OrdinaryIncome"] * 100
        fin_data["Diff Forecast Result NetIncome"] = fin_forecast["Forecast_FinancialStatement NetIncome"] / fin_data["Result_FinancialStatement NetIncome"] * 100

        # 欠損値処理
        fin_feats = fin_data.fillna(0)

        ################## stock_price_feat ##################
        # stock_priceデータを読み込む
        price = dfs["stock_price"].copy()

        # 特定の銘柄コードのデータに絞る
        price_data = price[price["Local Code"] == code].copy()

        # 日付列をpd.Timestamp型に変換してindexに設定
        price_data["datetime"] = pd.to_datetime(price_data["EndOfDayQuote Date"])
        price_data.set_index("datetime", inplace=True)

        # 終値, ボリューム
        feats = price_data[["EndOfDayQuote ExchangeOfficialClose", "EndOfDayQuote Volume"]].copy()

        # 終値の20営業日リターン
        feats["return_1month"] = feats["EndOfDayQuote ExchangeOfficialClose"].pct_change(20)
        # 終値と20営業日の単純移動平均線の乖離
        feats["MA_gap_1month"] = feats["EndOfDayQuote ExchangeOfficialClose"] / (feats["EndOfDayQuote ExchangeOfficialClose"].rolling(20).mean())
        # 終値と20営業日の単純移動平均線の乖離
        feats["EMA_gap_1month"] = feats["EndOfDayQuote ExchangeOfficialClose"] / (feats["EndOfDayQuote ExchangeOfficialClose"].ewm(span=20).mean())    
        # 過去20営業日の平均売買金額
        feats["Volume_mean_1month"] = feats["EndOfDayQuote Volume"].rolling(window=20).mean()

        # RSI
        feats["rsi"] = relative_strength_index(feats["EndOfDayQuote ExchangeOfficialClose"], period=20)

        # テクニカル指標
          # ラグファクター
          # トレンドライン、サポートライン、レジスタンスライン

        # おおまかな手順の3つ目
        # 欠損値処理
        feats = feats.fillna(0)

        # 財務データの特徴量とマーケットデータの特徴量のインデックスを合わせる
        feats = feats.loc[feats.index.isin(fin_feats.index)]
        fin_feats = fin_feats.loc[fin_feats.index.isin(feats.index)]

        # データを結合
        feats = pd.concat([feats, fin_feats], axis=1).dropna()    

        ################## stock_fin&price_feat ##################
        ## 時価総額
        feats["Market Capitalization"] = feats["IssuedShareEquityQuote IssuedShare"] * feats["EndOfDayQuote ExchangeOfficialClose"]
        ## PER
        feats["PER"] = feats["EndOfDayQuote ExchangeOfficialClose"] / feats["EPS"]
        ## PBR
        feats["PBR"] = feats["EndOfDayQuote ExchangeOfficialClose"] / feats["BPS"]

        # 元データのカラムを削除
        feats = feats.drop(["EndOfDayQuote ExchangeOfficialClose"], axis=1)

        # 不要な特徴を削除
        drop_col = ["Result_FinancialStatement FiscalYear",
                "Result_Dividend FiscalYear",
                "Result_FinancialStatement TotalAssets",
                "Result_FinancialStatement NetAssets",
                "Result_FinancialStatement Liability",
                "Forecast_Dividend FiscalYear",
                "Result_FinancialStatement CashFlowsFromFinancingActivities",
                "Result_FinancialStatement CashFlowsFromInvestingActivities",
                "Result_FinancialStatement CashFlowsFromOperatingActivities",
                "Forecast_FinancialStatement FiscalYear"]
        feats = feats.drop(drop_col, axis=1)

        # 欠損値処理を行います。
        feats = feats.replace([np.inf, -np.inf], 0)

        # 銘柄コードを設定
        feats["code"] = code

        return feats

    @classmethod
    def create_model(cls, dfs, codes, label):
        """
        Args:
            dfs (dict)  : dict of pd.DataFrame include stock_fin, stock_price
            codes (list[int]): A local code for a listed company
            label (str): prediction target label
        Returns:
            RandomForestRegressor
        """
        # 特徴量を取得
        buff = []
        for code in codes:
            buff.append(cls.get_features_for_predict(cls.dfs, code))
        feature = pd.concat(buff)
        # 特徴量と目的変数を一致させて、データを分割
        train_X, train_y, _, _, _, _ = cls.get_features_and_label(
            dfs, codes, feature, label
        )

        # モデル作成
        model = lgb.LGBMRegressor(random_state=0, n_estimators=300)
        model.fit(train_X, train_y)

        return model

    @classmethod
    def save_model(cls, model, label, model_path="../model"):
        """
        Args:
            model (RandomForestRegressor): trained model
            label (str): prediction target label
            model_path (str): path to save model
        Returns:
            -
        """
        # tag::save_model_partial[]
        # モデル保存先ディレクトリを作成
        os.makedirs(model_path, exist_ok=True)
        with open(os.path.join(model_path, f"my_model_{label}.pkl"), "wb") as f:
            # モデルをpickle形式で保存
            pickle.dump(model, f)
        # end::save_model_partial[]

    @classmethod
    def get_model(cls, model_path="../model", labels=None):
        """Get model method

        Args:
            model_path (str): Path to the trained model directory.
            labels (arrayt): list of prediction target labels

        Returns:
            bool: The return value. True for success, False otherwise.

        """
        if cls.models is None:
            cls.models = {}
        if labels is None:
            labels = cls.TARGET_LABELS
        try:
            for label in labels:
                m = os.path.join(model_path, f"my_model_{label}.pkl")
                with open(m, "rb") as f:
                    # pickle形式で保存されているモデルを読み込み
                    cls.models[label] = pickle.load(f)
            return True
        except Exception as e:
            print(e)
            return False

    @classmethod
    def train_and_save_model(
        cls, inputs, labels=None, codes=None, model_path="../model"
    ):
        """Predict method

        Args:
            inputs (str)   : paths to the dataset files
            labels (array) : labels which is used in prediction model
            codes  (array) : target codes
            model_path (str): Path to the trained model directory.
        Returns:
            Dict[pd.DataFrame]: Inference for the given input.
        """
        if cls.dfs is None:
            cls.get_dataset(inputs)
            cls.get_codes(cls.dfs)
        if codes is None:
            codes = cls.codes
        if labels is None:
            labels = cls.TARGET_LABELS
        for label in labels:
            print(label)
            model = cls.create_model(cls.dfs, codes=codes, label=label)
            cls.save_model(model, label, model_path=model_path)

    @classmethod
    def predict(cls, inputs, labels=None, codes=None, start_dt=TEST_START):
        """Predict method

        Args:
            inputs (dict[str]): paths to the dataset files
            labels (list[str]): target label names
            codes (list[int]): traget codes
            start_dt (str): specify date range
        Returns:
            str: Inference for the given input.
        """

        # データ読み込み
        if cls.dfs is None:
            cls.get_dataset(inputs)
            cls.get_codes(cls.dfs)

        # 予測対象の銘柄コードと目的変数を設定
        if codes is None:
            codes = cls.codes
        if labels is None:
            labels = cls.TARGET_LABELS

        # 特徴量を作成
        buff = []
        for code in codes:
            buff.append(cls.get_features_for_predict(cls.dfs, code, start_dt))
        feats = pd.concat(buff)

        # 結果を以下のcsv形式で出力する
        # １列目:datetimeとcodeをつなげたもの(Ex 2016-05-09-1301)
        # ２列目:label_high_20　終値→最高値への変化率
        # ３列目:label_low_20　終値→最安値への変化率
        # headerはなし、B列C列はfloat64

        # 日付と銘柄コードに絞り込み
        df = feats.loc[:, ["code"]].copy()
        # codeを出力形式の１列目と一致させる
        df.loc[:, "code"] = df.index.strftime("%Y-%m-%d-") + df.loc[:, "code"].astype(
            str
        )

        # 出力対象列を定義
        output_columns = ["code"]

        # 目的変数毎に予測
        for label in labels:
            # 予測実施
            df[label] = cls.models[label].predict(feats)
            # 出力対象列に追加
            output_columns.append(label)

        out = io.StringIO()
        df.to_csv(out, header=False, index=False, columns=output_columns)

        return out.getvalue()