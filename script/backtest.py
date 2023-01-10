import pandas as pd
import numpy as np
import datetime
import copy


data_path = "/Users/smurakawa/src/portfolio/data/data/stock_df_dev.pkl"

class BacktestBase(object):
    def __init__(self, start,end,amount,
                    ftc=0.0,ptc=0.0,verbose=True):
        self.start = start
        self.end = end
        self.initial_amount = amount
        self.amount = amount
        self.ftc = ftc
        self.ptc = ptc
        self.units = 0
        self.position = 0
        self.trades = 0
        self.win_trades = 0
        self.win_amount = 0
        self.lose_trades = 0
        self.lose_amount = 0
        self.buy_price = 0
        self.sell_price = 0
        self.total_ret = 0
        self.daily_ret = []
        self.verbose = verbose
    
    def get_date_price(self, bar, df):
        date = str(df.index[bar])[:10]
        price = df.CLOSE.iloc[bar]

        return date, price

    def print_balance(self, date):
        print(f'{date} | current balance {self.amount:.2f}')


    def print_net_wealth(self, date, price):
        net_wealth = self.units * price + self.amount
        print(f'{date} | current net wealth {net_wealth:.2f}')


    def place_buy_order(self, date, price, units=None, amount=None):
        self.buy_price = price
        if units is None:
            units = int(amount / price)

        self.amount -= (units * price) * (1 + self.ptc) + self.ftc
        self.units += units
        self.trades += 1
        if self.verbose:
            print(f'{date} | selling {units} units at {price:.2f}')
            self.print_balance(date)
            self.print_net_wealth(date, price)


    def place_sell_order(self, date, price, units=None, amount=None):
        self.sell_price = price
        if units is None:
            units = int(amount / price)

        self.amount += (units * price) * (1 - self.ptc) - self.ftc
        self.units -= units
        self.trades += 1
        if self.verbose:
            print(f'{date} | selling {units} units at {price:.2f}')
            self.print_balance(date)
            self.print_net_wealth(date, price)

        ret = (self.sell_price - self.buy_price) * units
        if ret > 0:
            self.win_trades += 1
            self.win_amount += ret
        elif ret < 0:
            self.lose_trades += 1
            self.lose_amount += ret
        
        self.buy_price = 0
        self.sell_price = 0
        self.total_ret += ret


    def close_out(self, bar, df):
        date, price = self.get_date_price(bar, df)
        self.amount += self.units * price
        self.units = 0
        self.trades += 1

        print('Final balance [¥] {:.2f}'.format(self.amount))
        print('Total Return [¥] {:.2f}'.format(self.total_ret))

        pref = ((self.amount - self.initial_amount) / self.initial_amount)
        print(f'Net Performance [%] {pref:.2f}')
        print('Total Trades {:.2f}'.format(self.trades))
        print('Win Trades {:.2f}'.format(self.win_trades))

        win_rate = self.win_trades / (self.win_trades + self.lose_trades)
        print('Win Rate [%] {:.2f}'.format(win_rate))

        avg_ret = (((self.amount - self.initial_amount) / (self.win_trades + self.lose_trades)))
        print('Avg Return {:.2f}'.format(avg_ret))

        payoff_ratio = abs(self.win_amount / self.win_trades) / abs(self.lose_amount / self.lose_trades)
        print('Payoff Ratio {:.2f}'.format(payoff_ratio))


class BacktestCanslimTrade(BacktestBase):
    def get_data_from_master_stock(self):
        df = pd.read_pickle(data_path)
        df = df[["DATE","CODE","COMPANYNAME","SECTOR33CODENAME","SECTOR17CODENAME","CLOSE","VOLUME"]]
        df.DATE = pd.to_datetime(df.DATE)
        df['DATE_YEAR'] = df.DATE.dt.year
        df = df.astype({'DATE_YEAR' : str})

        return df


    def feature_engineering(self, df):
        # CANSLIM基礎特徴料作成
        df["MA_50"] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE'] \
                        .rolling(51, min_periods=1).mean().reset_index(drop=True, level=0)

        df["MA_150"] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE']\
                        .rolling(151, min_periods=1).mean().reset_index(drop=True, level=0)

        df["MA_200"] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE']\
                        .rolling(201, min_periods=1).mean().reset_index(drop=True, level=0)

        df["LOW_52"] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE']\
                        .rolling(53, min_periods=1).min().reset_index(drop=True, level=0)

        df["HIGH_52"] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE']\
                        .rolling(53, min_periods=1).max().reset_index(drop=True, level=0)

        df['LAG_63'] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE'].pct_change(63)
        df['LAG_126'] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE'].pct_change(126)
        df['LAG_189'] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE'].pct_change(189)
        df['LAG_252'] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['CLOSE'].pct_change(252)
        df["RSI"] = ((df['LAG_63'] * 0.4) +  (df['LAG_126'] * 0.2) + (df['LAG_189'] * 0.2) + (df['LAG_252'] * 0.2)) * 100

        # UD_RATIO計算メソッド
        df["LAG_1_VOLUME"] = df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['VOLUME'].pct_change(1)

        ud_ratio_df = copy.deepcopy(df[["DATE", "CODE", "LAG_1_VOLUME"]])
        ud_ratio_df["VOL_PLUS"] = np.where(df["LAG_1_VOLUME"] >= 0, df["VOLUME"], 0)
        ud_ratio_df["VU"] = ud_ratio_df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['VOL_PLUS']\
                                .rolling(53, min_periods=1).sum().reset_index(drop=True, level=0)

        ud_ratio_df["VOL_MINUS"] = np.where(df["LAG_1_VOLUME"] <= 0, df["VOLUME"], 0)
        ud_ratio_df["VD"] = ud_ratio_df.sort_values(by=['DATE'],ascending=True).groupby(['CODE'])['VOL_MINUS']\
                                .rolling(53, min_periods=1).sum().reset_index(drop=True, level=0)

        df["UD_RATIO"] = ud_ratio_df["VU"] / ud_ratio_df["VD"]
        df.UD_RATIO = df.UD_RATIO.replace([np.inf, -np.inf], 0)
        df.UD_RATIO = df.UD_RATIO.fillna(0)
        del ud_ratio_df

        # CANSLIM上昇トレンド判定メソッド
        df["UP_COND1"] = np.where((df.CLOSE > df.MA_150) & (df.CLOSE > df.MA_200), 1, 0 )
        df["UP_COND2"] = np.where(df.MA_150 > df.MA_200, 1, 0 )
        df["UP_COND4"] = np.where((df.MA_50 > df.MA_150) & (df.MA_50 > df.MA_200), 1, 0 )
        df["UP_COND5"] = np.where((df.CLOSE > df.MA_50), 1, 0 )
        df["UP_COND6"] = np.where(df.CLOSE > df.LOW_52 * 1.3, 1, 0 )
        df["UP_COND7"] = np.where(df.CLOSE > df.HIGH_52 * 0.75, 1, 0 )
        df["UP_COND8"] = np.where(df.RSI > 1.7 , 1, 0 )
        df["UP_MATCH_COND_NUM"] = df.UP_COND1 + df.UP_COND2 + df.UP_COND4 \
                                        + df.UP_COND5 + df.UP_COND6 + df.UP_COND7 + df.UP_COND8
        df["UP_MATCH_COND_TREND"] = np.where((df.UP_COND1 == 1) & (df.UP_COND4 == 1) & \
                                                (df.UP_COND5 == 1) & (df.UP_COND6 == 1), 1, 0 )

        # 不要カラムの削除
        df = df[['DATE','CODE','SECTOR33CODENAME','CLOSE','UP_MATCH_COND_NUM','UP_MATCH_COND_TREND','UD_RATIO']]
        df = df.astype({
            "CLOSE" : "float16"
            ,"UP_MATCH_COND_NUM" : "int8"
            ,"UP_MATCH_COND_TREND" : "int8"
        })

        return df

    def cumsum_ret(self, code, date):
        return self.daily_ret.append([code, date, self.total_ret])


    def run_trend_1_and_match_num_over_5(self, data):
        print('----------------------- Start Run Strategy run_trend_1_and_match_num_over_6 ----------------------------')

        codes = data.CODE.unique()
        for code in codes:
            df = data[data["CODE"]==code]
            df = df.set_index("DATE")

            for bar in range(0, len(df)):
                date, price = self.get_date_price(bar, df)
                if self.position == 0:
                    if df["UP_MATCH_COND_TREND"].iloc[bar] == 1 and df["UP_MATCH_COND_NUM"].iloc[bar] > 4:
                        self.place_buy_order(date, price, amount=self.amount)
                        self.position = 1

                elif self.position == 1:
                    if df["UP_MATCH_COND_TREND"].iloc[bar] == 0 or df["UP_MATCH_COND_NUM"].iloc[bar] <= 4:
                        self.place_sell_order(date, price, units=self.units)
                        self.position = 0

                self.cumsum_ret(code, date)

        self.close_out(bar, df)
        print('----------------------- End Run Strategy run_trend_1_and_match_num_over_6 ----------------------------')

        result_df = pd.DataFrame(self.daily_ret, columns=["code", "date", "return"])
        result_df.to_csv("./backtest_result/run_trend_1_and_match_num_over_5.csv", index=False)


    def run_match_num_over_2(self, data):
        print('----------------------- Start Run Strategy run_match_num_over_2 ----------------------------')

        codes = data.CODE.unique()
        for code in codes:
            df = data[data["CODE"]==code]
            df = df.set_index("DATE")

            for bar in range(0, len(df)):
                date, price = self.get_date_price(bar, df)
                if self.position == 0:
                    if df["UP_MATCH_COND_NUM"].iloc[bar] > 2:
                        self.place_buy_order(date, price, amount=self.amount)
                        self.position = 1

                elif self.position == 1:
                    if df["UP_MATCH_COND_NUM"].iloc[bar] <= 2:
                        self.place_sell_order(date, price, units=self.units)
                        self.position = 0

                self.cumsum_ret(code, date)

        self.close_out(bar, df)
        print('----------------------- End Run Strategy run_match_num_over_2 ----------------------------')

        result_df = pd.DataFrame(self.daily_ret, columns=["code", "date", "return"])
        result_df.to_csv("./backtest_result/run_match_num_over_2.csv", index=False)


    def run_udratio_over_1(self, data):
        print('----------------------- Start Run Strategy run_udratio_over_1 ----------------------------')

        codes = data.CODE.unique()
        for code in codes:
            df = data[data["CODE"]==code]
            df = df.set_index("DATE")

            for bar in range(0, len(df)):
                date, price = self.get_date_price(bar, df)
                if self.position == 0:
                    if df["UD_RATIO"].iloc[bar] >= 1:
                        self.place_buy_order(date, price, amount=self.amount)
                        self.position = 1

                elif self.position == 1:
                    if df["UD_RATIO"].iloc[bar] < 1 or df["UP_MATCH_COND_NUM"].iloc[bar] <= 4:
                        self.place_sell_order(date, price, units=self.units)
                        self.position = 0

                self.cumsum_ret(code, date)

        self.close_out(bar, df)
        print('----------------------- End Run Strategy run_udratio_over_1 ----------------------------')

        result_df = pd.DataFrame(self.daily_ret, columns=["code", "date", "return"])
        result_df.to_csv("./backtest_result/run_udratio_over_1.csv", index=False)


if __name__ == '__main__':
    dt_now = datetime.datetime.now()
    print('------------- Script Start at %s -------------' % dt_now)
    bct = BacktestCanslimTrade('2021-01-01','2022-12-31',1000,ftc=0.0,ptc=0.0,verbose=False)
    df = bct.get_data_from_master_stock()
    df = bct.feature_engineering(df)

    bct = BacktestCanslimTrade('2021-01-01','2022-12-31',1000,ftc=0.0,ptc=0.0,verbose=False)
    bct.run_trend_1_and_match_num_over_5(df)

    bct = BacktestCanslimTrade('2021-01-01','2022-12-31',1000,ftc=0.0,ptc=0.0,verbose=False)
    bct.run_udratio_over_1(df)

    bct = BacktestCanslimTrade('2021-01-01','2022-12-31',1000,ftc=0.0,ptc=0.0,verbose=False)
    bct.run_match_num_over_2(df)

    dt_now = datetime.datetime.now()
    print('------------- Script End at %s -------------' % dt_now)

