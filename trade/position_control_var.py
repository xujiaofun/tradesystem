
def fun_calStockWeight_by_risk(context, confidencelevel, stocklist):
    
    def __fun_calstock_risk_ES(stock, lag, confidencelevel):
        # print("__fun_calstock_risk_ES " + stock)
        hStocks = history(lag, '1d', 'close', stock, df=True)
        dailyReturns = hStocks.resample('D',how='last').pct_change().fillna(value=0, method=None, axis=0).values
        if confidencelevel   == 1.96:
            a = (1 - 0.95)
        elif confidencelevel == 2.06:
            a = (1 - 0.96)
        elif confidencelevel == 2.18:
            a = (1 - 0.97)
        elif confidencelevel == 2.34:
            a = (1 - 0.98)
        elif confidencelevel == 2.58:
            a = (1 - 0.99)
        elif confidencelevel == 5:
            a = (1 - 0.99999)
        else:
            a = (1 - 0.95)
        
        dailyReturns_sort =  sorted(dailyReturns)

        count = 0
        sum_value = 0
        for i in range(len(dailyReturns_sort)):
            if i < (lag * a):
                sum_value += dailyReturns_sort[i]
                count += 1
        if count == 0:
            ES = 0
        else:
            ES = max(0.05, -(sum_value / (lag * a)))
    
        if isnan(ES):
            ES = 0

        return ES

    def __fun_calstock_risk_VaR(stock, lag, confidencelevel):
        hStocks = history(lag, '1d', 'close', stock, df=True)
        dailyReturns = hStocks.resample('D',how='last').pct_change().fillna(value=0, method=None, axis=0).values
        VaR = 1 * confidencelevel * np.std(dailyReturns)

        return VaR
        
    __risk = {}

    stock_list = []
    for stock in stocklist:
        curRisk = __fun_calstock_risk_ES(stock, 180, confidencelevel)

        if curRisk <> 0.0:
            __risk[stock] = curRisk

    __position = {}
    for stock in __risk.keys():
        __position[stock] = 1.0 / __risk[stock]

    total_position = 0
    for stock in __position.keys():
        total_position += __position[stock]

    __ratio = {}
    for stock in __position.keys():
        tmpRatio = __position[stock] / total_position
        if isnan(tmpRatio):
            tmpRatio = 0
        __ratio[stock] = round(tmpRatio, 4)

    return __ratio

def assetAllocationSystem(context, buylist):
    def __fun_getEquity_ratio(context, __stocklist):
        __ratio = {}
        # 按风险平价配仓
        if __stocklist:
            __ratio = fun_calStockWeight_by_risk(context, 2.58, __stocklist)

        return __ratio

    equity_ratio = __fun_getEquity_ratio(context, buylist)
    bonds_ratio  = __fun_getEquity_ratio(context, context.lowPEG_moneyfund)
    
    return equity_ratio, bonds_ratio

def fun_getEquity_value(equity_ratio, risk_money, maxrisk_money, confidence_ratio, lag):
    def __fun_getdailyreturn(stock, freq, lag):
        hStocks = history(lag, freq, 'close', stock, df=True)
        dailyReturns = hStocks.resample('D',how='last').pct_change().fillna(value=0, method=None, axis=0).values
        #dailyReturns = hStocks.pct_change().fillna(value=0, method=None, axis=0).values

        return dailyReturns

    def __fun_get_portfolio_dailyreturn(ratio, freq, lag):
        __portfolio_dailyreturn = []
        for stock in ratio.keys():
            if ratio[stock] != 0:
                __dailyReturns = __fun_getdailyreturn(stock, freq, lag)
                __tmplist = []
                for i in range(len(__dailyReturns)):
                    __tmplist.append(__dailyReturns[i] * ratio[stock])
                if __portfolio_dailyreturn:
                    __tmplistB = []
                    for i in range(len(__portfolio_dailyreturn)):
                        __tmplistB.append(__portfolio_dailyreturn[i]+__tmplist[i])
                    __portfolio_dailyreturn = __tmplistB
                else:
                    __portfolio_dailyreturn = __tmplist

        return __portfolio_dailyreturn

    def __fun_get_portfolio_ES(ratio, freq, lag, confidencelevel):
        if confidencelevel == 1.96:
            a = (1 - 0.95)
        elif confidencelevel == 2.06:
            a = (1 - 0.96)
        elif confidencelevel == 2.18:
            a = (1 - 0.97)
        elif confidencelevel == 2.34:
            a = (1 - 0.98)
        elif confidencelevel == 2.58:
            a = (1 - 0.99)
        else:
            a = (1 - 0.95)
        dailyReturns = __fun_get_portfolio_dailyreturn(ratio, freq, lag)
        dailyReturns_sort =  sorted(dailyReturns)

        count = 0
        sum_value = 0
        for i in range(len(dailyReturns_sort)):
            if i < (lag * a):
                sum_value += dailyReturns_sort[i]
                count += 1
        if count == 0:
            ES = 0
        else:
            ES = max(0.05, -(sum_value / (lag * a)))

        return ES

    def __fun_get_portfolio_VaR(ratio, freq, lag, confidencelevel):
        __dailyReturns = __fun_get_portfolio_dailyreturn(ratio, freq, lag)
        __portfolio_VaR = 1.0 * confidencelevel * np.std(__dailyReturns)

        return __portfolio_VaR

    # 每元组合资产的 VaR
    __portfolio_VaR = __fun_get_portfolio_VaR(equity_ratio, '1d', lag, confidence_ratio)

    __equity_value_VaR = 0
    if __portfolio_VaR:
        __equity_value_VaR = risk_money / __portfolio_VaR

    __portfolio_ES = __fun_get_portfolio_ES(equity_ratio, '1d', lag, confidence_ratio)

    __equity_value_ES = 0
    if __portfolio_ES:
        __equity_value_ES = maxrisk_money / __portfolio_ES

    if __equity_value_VaR == 0:
        equity_value = __equity_value_ES
    elif __equity_value_ES == 0:
        equity_value = __equity_value_VaR
    else:
        equity_value = min(__equity_value_VaR, __equity_value_ES)

    return equity_value

def fun_calPosition(context, equity_ratio, bonds_ratio, lowPEG_ratio, lowPEG_risk_ratio, lowPEG_confidencelevel):

        #risk_ratio = len(equity_ratio.keys())
        risk_ratio = 0
        for stock in equity_ratio.keys():
            if equity_ratio[stock] <> 0 and stock not in context.lowPEG_moneyfund:
                risk_ratio += 1
        risk_money = context.portfolio.portfolio_value * risk_ratio * lowPEG_ratio * lowPEG_risk_ratio
        maxrisk_money = risk_money * 1.7

        equity_value = 0
        if equity_ratio:
            equity_value = fun_getEquity_value(equity_ratio, risk_money, maxrisk_money, lowPEG_confidencelevel,180)

        value_ratio = 0
        total_value = context.portfolio.portfolio_value * lowPEG_ratio
        if equity_value > total_value:
            bonds_value = 0
            value_ratio = 1.0 * lowPEG_ratio
        else:
            value_ratio = (equity_value / total_value) * lowPEG_ratio
            bonds_value = total_value - equity_value
        
        trade_ratio = {}
        equity_list = equity_ratio.keys()
        for stock in equity_list:
            if stock in trade_ratio:
                trade_ratio[stock] += round((equity_ratio[stock] * value_ratio), 3)
            else:
                trade_ratio[stock] = round((equity_ratio[stock] * value_ratio), 3)
    
        for stock in bonds_ratio.keys():
            if stock in trade_ratio:
                trade_ratio[stock] += round((bonds_ratio[stock] * bonds_value / total_value) * lowPEG_ratio, 3)
            else:
                trade_ratio[stock] = round((bonds_ratio[stock] * bonds_value / total_value) * lowPEG_ratio, 3)
    
        return trade_ratio