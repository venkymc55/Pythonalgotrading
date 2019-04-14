import pandas as pd
import numpy as np
from stocktrends import Renko
from kiteconnect import KiteTicker


def data_frame(data, size, period1, period2):
    df = pd.DataFrame(data)
    df['date'], df['time'] = df['date'].dt.date, df['date'].dt.strftime('%H:%M:%S')
    df = Renko(df)
    df.brick_size = size
    data = df.get_bricks()
    data['MA4'] = data['close'].rolling(window=period1).mean()
    data['MA10'] = data['close'].rolling(window=period2).mean()
    return data


def stochastic_osc(df):
    array_close = np.array(df['close'])
    array_open = np.array(df['open'])
    array_high = np.array(df['high'])
    array_low = np.array(df['low'])
    y = 0
    z = 0
    # kperiods are 14 array start from 0 index
    kperiods = 13
    array_highest = []
    for x in range(0, array_high.size - kperiods):
        z = array_high[y]
        for j in range(0, kperiods):
            if z < array_high[y + 1]:
                z = array_high[y + 1]
            y = y + 1
        # creating list highest of k periods
        array_highest.append(z)
        y = y - (kperiods - 1)
    y = 0
    z = 0
    array_lowest = []
    for x in range(0, array_low.size - kperiods):
        z = array_low[y]
        for j in range(0, kperiods):
            if (z > array_low[y + 1]):
                z = array_low[y + 1]
            y = y + 1
        # creating list lowest of k periods
        array_lowest.append(z)
        y = y - (kperiods - 1)

    # KDJ (K line, D line, J line)
    Kvalue = []
    for x in range(kperiods, array_close.size):
        k = ((array_close[x] - array_lowest[x - kperiods]) * 100 / (array_highest[x - kperiods] - array_lowest[x - kperiods]))
        Kvalue.append(k)
    y = 0
    # dperiods for calculate d values
    dperiods = 3
    Dvalue = [None, None]
    mean = 0
    for x in range(0, len(Kvalue) - dperiods + 1):
        sum = 0
        for j in range(0, dperiods):
            sum = Kvalue[y] + sum
            y = y + 1
        mean = sum / dperiods
        # d values for %d line
        Dvalue.append(mean)
        y = y - (dperiods - 1)
    D_df = pd.DataFrame(Dvalue, columns=['Dvalue'])
    Jvalue = D_df.rolling(window=3).mean()

    return D_df, Jvalue


def sma(data, period):
    result = data.rolling(window=period).mean()
    return result


def ATR(df, n):  # df is the DataFrame, n is the period 7,14 ,etc
    df['H-L'] = abs(df['high'] - df['low'])
    df['H-PC'] = abs(df['high'] - df['close'].shift(1))
    df['L-PC'] = abs(df['low'] - df['close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    df['ATR'] = np.nan
    df.ix[n - 1, 'ATR'] = df['TR'][:n - 1].mean()  # .ix is deprecated from pandas version- 0.19
    for i in range(n, len(df)):
        df['ATR'][i] = (df['ATR'][i - 1] * (n - 1) + df['TR'][i]) / n
    return df
