# 克隆自聚宽文章：https://www.joinquant.com/post/3347
# 标题：彼得林奇修正 PEG
# 作者：小兵哥

import numpy as np
import talib
import pandas
import scipy as sp
import scipy.optimize
import datetime as dt
from scipy import linalg as sla
from scipy import spatial
from jqdata import jy
from jqlib.technical_analysis import *

def initialize(context):
    #用沪深 300 做回报基准
    set_benchmark('000300.XSHG')
    set_slippage(FixedSlippage(0.02))
    set_option('use_real_price', True)

    # 关闭部分log
    log.set_level('order', 'error')
    # 定义策略占用仓位比例
    context.lowPEG_ratio = 1.0

    # for lowPEG algorithms
    # 正态分布概率表，标准差倍数以及置信率
    # 1.96, 95%; 2.06, 96%; 2.18, 97%; 2.34, 98%; 2.58, 99%; 5, 99.9999%
    context.lowPEG_confidencelevel = 1.96
    context.lowPEG_hold_periods, context.lowPEG_hold_cycle = 0, 30
    context.lowPEG_stock_list = []
    context.lowPEG_position_price = {}
    context.window = 180

    g.quantlib = quantlib()

    run_daily(fun_main, '10:30')

def fun_main(context):

    lowPEG_trade_ratio = lowPEG_algo(context, context.lowPEG_ratio, context.portfolio.portfolio_value)
    # 调仓，执行交易
    g.quantlib.fun_do_trade(context, lowPEG_trade_ratio, context.lowPEG_moneyfund)


def lowPEG_algo(context, lowPEG_ratio, portfolio_value):
    '''
    low PEG algorithms
    输入参数：lowPEG_ratio, protfolio_value
    输出参数：lowPEG_trade_ratio
    自有类  : lowPEG_lib
    调用类  : quantlib
    '''

    # 引用 lib
    g.lowPEG = lowPEG_lib()
    # 引用 quantlib
    g.quantlib = quantlib()

    g.lowPEG.fun_initialize(context)

    recal_flag = False
    if g.lowPEG.fun_needRebalance(context):
        recal_flag = True

    # 配仓，分配持股比例
    equity_ratio = {}
    if recal_flag:
        context.lowPEG_stock_list = g.lowPEG.fun_get_stock_list(context)
        equity_ratio, bonds_ratio = g.lowPEG.fun_assetAllocationSystem(context, context.lowPEG_stock_list)
    else:
        equity_ratio = context.lowPEG_equity_ratio
        bonds_ratio = context.lowPEG_bonds_ratio

    context.lowPEG_equity_ratio = equity_ratio
    context.lowPEG_bonds_ratio = bonds_ratio

    # 分配头寸，配置市值
    trade_ratio = {}
    if recal_flag:
        trade_ratio = g.lowPEG.fun_calPosition(context, equity_ratio, bonds_ratio, lowPEG_ratio, portfolio_value)

        stock_list = list(get_all_securities(['stock']).index)
        for stock in context.portfolio.positions.keys():
            if stock not in trade_ratio and stock in stock_list:
                trade_ratio[stock] = 0
    else:
        trade_ratio = context.lowPEG_trade_ratio

    context.lowPEG_trade_ratio = trade_ratio

    return trade_ratio

class lowPEG_lib():
    
    def __init__(self, _period = '1d'):
        pass
    
    def fun_initialize(self, context):
        # 定义股票池
        lowPEG_equity = context.lowPEG_stock_list

        lowPEG_moneyfund = ['511880.XSHG']

        # 上市不足 60 天的剔除掉
        context.lowPEG_equity    = g.quantlib.fun_delNewShare(context, lowPEG_equity, 60)
        context.lowPEG_moneyfund = g.quantlib.fun_delNewShare(context, lowPEG_moneyfund, 60)

        context.lowPEG_hold_num = 5
        context.lowPEG_risk_ratio = 0.03 / context.lowPEG_hold_num

    def fun_needRebalance(self, context):
        if len(context.lowPEG_stock_list) == 0:
            context.lowPEG_hold_periods = context.lowPEG_hold_cycle
            return True
        
        if context.lowPEG_hold_periods == 0:
            context.lowPEG_hold_periods = context.lowPEG_hold_cycle
            return True
        else:
            context.lowPEG_hold_periods -= 1
            return False

    # 取得净利润增长率参数
    def fun_get_inc(self, context, stock_list):
        # 取最近的四个季度财报的日期
        def __get_quarter(stock_list):
            '''
            输入 stock_list
            返回最近 n 个财报的日期
            返回每个股票最近一个财报的日期
            '''
            # 取最新一季度的统计日期
            
            # print("stock_list=", stock_list)
            q = query(indicator.code, indicator.statDate
                     ).filter(indicator.code.in_(stock_list))
            df = get_fundamentals(q)

            stock_last_statDate = {}
            tmpDict = df.to_dict()
            for i in range(len(tmpDict['statDate'].keys())):
                # 取得每个股票的代码，以及最新的财报发布日
                stock_last_statDate[tmpDict['code'][i]] = tmpDict['statDate'][i]

            df = df.sort(columns='statDate', ascending=False)
            # 取得最新的财报日期
            
            if df.empty:
                return 0, 0, 0, 0, 0, {}

            last_statDate = df.iloc[0,1]

            this_year = int(str(last_statDate)[0:4])
            this_month = str(last_statDate)[5:7]

            if this_month == '12':
                last_quarter       = str(this_year)     + 'q4'
                last_two_quarter   = str(this_year)     + 'q3'
                last_three_quarter = str(this_year)     + 'q2'
                last_four_quarter  = str(this_year)     + 'q1'
                last_five_quarter  = str(this_year - 1) + 'q4'

            elif this_month == '09':
                last_quarter       = str(this_year)     + 'q3'
                last_two_quarter   = str(this_year)     + 'q2'
                last_three_quarter = str(this_year)     + 'q1'
                last_four_quarter  = str(this_year - 1) + 'q4'
                last_five_quarter  = str(this_year - 1) + 'q3'

            elif this_month == '06':
                last_quarter       = str(this_year)     + 'q2'
                last_two_quarter   = str(this_year)     + 'q1'
                last_three_quarter = str(this_year - 1) + 'q4'
                last_four_quarter  = str(this_year - 1) + 'q3'
                last_five_quarter  = str(this_year - 1) + 'q2'

            else:  #this_month == '03':
                last_quarter       = str(this_year)     + 'q1'
                last_two_quarter   = str(this_year - 1) + 'q4'
                last_three_quarter = str(this_year - 1) + 'q3'
                last_four_quarter  = str(this_year - 1) + 'q2'
                last_five_quarter  = str(this_year - 1) + 'q1'
        
            return last_quarter, last_two_quarter, last_three_quarter, last_four_quarter, last_five_quarter, stock_last_statDate

        # 查财报，返回指定值
        def __get_fundamentals_value(stock_list, myDate):
            '''
            输入 stock_list, 查询日期
            返回指定的财务数据，格式 dict
            '''
            q = query(indicator.code, indicator.inc_net_profit_year_on_year, indicator.statDate
                     ).filter(indicator.code.in_(stock_list))

            df = get_fundamentals(q, statDate = myDate).fillna(value=0)

            tmpDict = df.to_dict()
            stock_dict = {}
            for i in range(len(tmpDict['statDate'].keys())):
                tmpList = []
                tmpList.append(tmpDict['statDate'][i])
                tmpList.append(tmpDict['inc_net_profit_year_on_year'][i])
                stock_dict[tmpDict['code'][i]] = tmpList

            return stock_dict

        # 对净利润增长率进行处理
        def __cal_net_profit_inc(inc_list):

            inc = inc_list

            for i in range(len(inc)):   # 约束在 +- 100 之内，避免失真
                if inc[i] > 100:
                    inc[i] = 100
                if inc[i] < -100:
                    inc[i] = -100

            avg_inc = np.mean(inc[:4])
            last_inc = inc[0]
            inc_std = np.std(inc)
                
            return avg_inc, last_inc, inc_std

        # 得到最近 n 个季度的统计时间
        last_quarter, last_two_quarter, last_three_quarter, last_four_quarter, last_five_quarter, stock_last_statDate = __get_quarter(stock_list)
    
        stock_dict = {}
        if last_quarter == 0 :
            for stock in stock_list:
                stock_dict[stock] = {}
                stock_dict[stock]['avg_inc'] = 1
                stock_dict[stock]['last_inc'] = 1
                stock_dict[stock]['inc_std'] = 1
            return stock_dict

        last_quarter_dict       = __get_fundamentals_value(stock_list, last_quarter)
        last_two_quarter_dict   = __get_fundamentals_value(stock_list, last_two_quarter)
        last_three_quarter_dict = __get_fundamentals_value(stock_list, last_three_quarter)
        last_four_quarter_dict  = __get_fundamentals_value(stock_list, last_four_quarter)
        last_five_quarter_dict  = __get_fundamentals_value(stock_list, last_five_quarter)
    
        for stock in stock_list:
            inc_list = []

            if stock in stock_last_statDate:
                if stock in last_quarter_dict:
                    if stock_last_statDate[stock] == last_quarter_dict[stock][0]:
                        inc_list.append(last_quarter_dict[stock][1])

                if stock in last_two_quarter_dict:
                    inc_list.append(last_two_quarter_dict[stock][1])
                else:
                    inc_list.append(0)

                if stock in last_three_quarter_dict:
                    inc_list.append(last_three_quarter_dict[stock][1])
                else:
                    inc_list.append(0)

                if stock in last_four_quarter_dict:
                    inc_list.append(last_four_quarter_dict[stock][1])
                else:
                    inc_list.append(0)

                if stock in last_five_quarter_dict:
                    inc_list.append(last_five_quarter_dict[stock][1])
                else:
                    inc_list.append(0)
            else:
                inc_list = [0, 0, 0, 0]

            # 取得过去4个季度的平均增长，最后1个季度的增长，增长标准差
            avg_inc, last_inc, inc_std = __cal_net_profit_inc(inc_list)

            stock_dict[stock] = {}
            stock_dict[stock]['avg_inc'] = avg_inc
            stock_dict[stock]['last_inc'] = last_inc
            stock_dict[stock]['inc_std'] = inc_std

        return stock_dict

    def fun_cal_stock_PEG(self, context, stock_list, stock_dict):
        if not stock_list:
            PEG = {}
            return PEG

        q = query(valuation.code, valuation.pe_ratio
                ).filter(valuation.code.in_(stock_list))
        
        df = get_fundamentals(q).fillna(value=0)

        tmpDict = df.to_dict()
        pe_dict = {}
        tmp_dict = {}
        for i in range(len(tmpDict['code'].keys())):
            pe_dict[tmpDict['code'][i]] = tmpDict['pe_ratio'][i]

        # 国泰民安版本
        #df = g.quantlib.fun_get_Divid_by_year(context, stock_list)
        # 聚源版本
        statsDate = context.current_dt.date() - dt.timedelta(1)
        df = g.quantlib.fun_get_Dividend_yield(stock_list, statsDate)
        tmpDict = df.to_dict()

        stock_interest = {}
        if df.empty == False:
	        for stock in tmpDict['divpercent']:
	            stock_interest[stock] = tmpDict['divpercent'][stock]

        h = history(1, '1d', 'close', stock_list, df=False)
        PEG = {}
        for stock in stock_list:
            avg_inc  = stock_dict[stock]['avg_inc']
            last_inc = stock_dict[stock]['last_inc']
            inc_std  = stock_dict[stock]['inc_std']

            pe = -1            
            if stock in pe_dict:
                pe = pe_dict[stock]

            interest = 0
            if stock in stock_interest:
                interest = stock_interest[stock]

            PEG[stock] = -1
            '''
            原话大概是：
            1、增长率 > 50 的公司要小心，高增长不可持续，一旦转差就要卖掉；实现的时候，直接卖掉增长率 > 50 个股票
            2、增长平稳，不知道该怎么表达，用了 inc_std < last_inc。有思路的同学请告诉我
            '''
            if pe > 0 and last_inc <= 50 and last_inc > 0 and (avg_inc - 2*inc_std) < last_inc:
                PEG[stock] = (pe / (last_inc + interest*100))

        return PEG

    def fun_get_stock_list(self, context):
        
        def fun_get_stock_market_cap(stock_list):
            q = query(valuation.code, valuation.market_cap
                    ).filter(valuation.code.in_(stock_list))
            
            df = get_fundamentals(q).fillna(value=0)
            tmpDict = df.to_dict()
            stock_dict = {}
            for i in range(len(tmpDict['code'].keys())):
                # 取得每个股票的 market_cap
                stock_dict[tmpDict['code'][i]] = tmpDict['market_cap'][i]
                
            return stock_dict
        
        # 自由现金流
        def fun_filter_by_fcff(stock_list):
            q = query(
                cash_flow.code, 
                cash_flow.net_operate_cash_flow,
                income.net_profit,
                cash_flow.goods_sale_and_service_render_cash,
                income.operating_revenue
            ).filter(
                cash_flow.code.in_(stock_list)
            ).filter(
                cash_flow.net_operate_cash_flow / income.net_profit >= 1
            ).filter(
                income.net_profit > 0
            ).filter(
                cash_flow.goods_sale_and_service_render_cash / income.operating_revenue >= 1
            )
            year = context.current_dt.year - 1
            df = get_fundamentals(q, statDate=str(year))
            arr = list(df['code'])

            # q = query(
            #     cash_flow.code, 
            #     cash_flow.net_operate_cash_flow,
            #     income.net_profit,
            #     cash_flow.goods_sale_and_service_render_cash,
            #     income.operating_revenue
            # ).filter(
            #     cash_flow.code.in_(arr)
            # )

            # sum_dict = {}
            # for stock in arr:
            #     sum_dict[stock] = [0, 0, 0, 0]

            # for x in xrange(0,5):
            #     df = get_fundamentals(q, statDate=str(year-x)).fillna(0)
            #     tmpDict = df.to_dict()
            #     for i in range(len(tmpDict['code'].keys())):
            #         stock = tmpDict['code'][i]
            #         sum_dict[stock][0] = sum_dict[stock][0] + tmpDict['net_operate_cash_flow'][i]
            #         sum_dict[stock][1] = sum_dict[stock][1] + tmpDict['net_profit'][i]
            #         sum_dict[stock][2] = sum_dict[stock][2] + tmpDict['goods_sale_and_service_render_cash'][i]
            #         sum_dict[stock][3] = sum_dict[stock][3] + tmpDict['operating_revenue'][i]

            # arr = []
            # for stock in sum_dict:
            #     tmp_arr = sum_dict[stock]
            #     if tmp_arr[0] / tmp_arr[1] >= 1 and tmp_arr[1] > 0 and tmp_arr[2] / tmp_arr[3] >= 1 :
            #         arr.append(stock)

            return arr

        # 相对强度
        def fun_filter_by_rsi(stock_list, date):
            new_list = []
            rsi_m = RSI(stock_list, check_date=date, N1 =24)
            rsi_300 = RSI('000300.XSHG', check_date=date, N1 =24)
            # rsi_y = RSI(stock_list, check_date=date - datetime.timedelta(days = 250), N1 =24)

            # for stock in stock_list:
            #     if stock in rsi_y and stock in rsi_m :
            #         if rsi_y[stock] > 0 and rsi_m[stock] > 0 and rsi_y[stock] > rsi_m[stock]:
            #             new_list.append(stock)
            
            for stock in stock_list:
                if stock in rsi_m and rsi_m[stock] > 50:
                    new_list.append(stock)

            return new_list

        def fun_filter_by_pe(stock_list):
            # q = query(
            #         valuation.code, valuation.pe_ratio
            #     ).filter(
            #         valuation.code.in_(stock_list)
            #     ).filter(
            #         valuation.pe_ratio >=5
            #     ).filter(
            #         valuation.pe_ratio <= 25
            #     )
            
            # df = get_fundamentals(q).fillna(value=0)
            # return list(df['code'])
            return stock_list

        def fun_shenqigongsi(df):
            # 息税前利润(EBIT) = 净利润 + 财务费用 + 所得税费用
            NP = df['net_profit']
            FE = df['financial_expense']
            TE = df['income_tax_expense']
            EBIT = NP + FE + TE

            # 固定资产净额(Net Fixed Assets) = 固定资产 - 工程物资 - 在建工程 - 固定资产清理
            FA = df['fixed_assets']
            CM = df['construction_materials']
            CP = df['constru_in_process']
            FAL = df['fixed_assets_liquidation']
            NFA = FA - CM - CP - FAL

            # 净营运资本(Net Working Capital)= 流动资产合计－流动负债合计
            TCA = df['total_current_assets']
            TCL = df['total_current_liability']
            NWC = TCA - TCL

            # 企业价值(Enterprise Value) = 总市值 + 负债合计 – 期末现金及现金等价物余额
            MC = df['market_cap']*100000000
            TL = df['total_liability']
            TC = df['cash_and_equivalents_at_end']
            EV = MC + TL - TC

            # Net Working Capital + Net Fixed Assets
            NCA = NWC + NFA

            # 剔除 NCA 和 EV 非正的股票
            tmp = set(df.index.values)-set(EBIT[EBIT<=0].index.values)-set(EV[EV<=0].index.values)-set(NCA[NCA<=0].index.values)
            EBIT = EBIT[tmp]
            NCA = NCA[tmp]
            EV = EV[tmp]

            # 计算魔法公式
            ROC = EBIT / NCA
            EY = EBIT / EV

            # 按ROC 和 EY 构建表格
            ROC_EY = pd.DataFrame({'ROC': ROC,'EY': EY})

            # 对 ROC进行降序排序, 记录序号
            ROC_EY = ROC_EY.sort('ROC',ascending=False)
            idx = pd.Series(np.arange(1,len(ROC)+1), index=ROC_EY['ROC'].index.values)
            ROC_I = pd.DataFrame({'roc_rank': idx})
            ROC_EY = pd.concat([ROC_EY, ROC_I], axis=1)

            # 对 EY进行降序排序, 记录序号
            ROC_EY = ROC_EY.sort('EY',ascending=False)
            idx = pd.Series(np.arange(1,len(EY)+1), index=ROC_EY['EY'].index.values)
            EY_I = pd.DataFrame({'ey_rank': idx})
            ROC_EY = pd.concat([ROC_EY, EY_I], axis=1)

            # 对序号求和，并记录之
            roci = ROC_EY['roc_rank']
            eyi = ROC_EY['ey_rank']
            idx = roci + eyi
            SUM_I = pd.DataFrame({'shenqi_rank': idx})
            ROC_EY = pd.concat([df, SUM_I], axis=1)
            return ROC_EY

        def fun_sort_stock(stock_list, stock_PEG, stock_inc):
            q = query(
                    valuation.code,                         # 股票代码
                    # valuation.market_cap,                   # 总市值(亿元)
                    valuation.pe_ratio, 
                    valuation.pb_ratio,
                    indicator.roe,
                    indicator.roa,
                    indicator.gross_profit_margin,
                    
    
                    income.net_profit,                      # 净利润(元)
                    income.financial_expense,               # 财务费用(元)
                    income.income_tax_expense,              # 所得税费用(元)
                    
                    balance.fixed_assets,                   # 固定资产(元)
                    balance.construction_materials,         # 工程物资(元)
                    balance.constru_in_process,             # 在建工程(元)
                    balance.fixed_assets_liquidation,       # 固定资产清理(元)
                    
                    balance.total_current_assets,           # 流动资产合计(元)
                    balance.total_current_liability,        # 流动负债合计(元)
                    
                    valuation.market_cap,                   # 总市值(亿元)
                    balance.total_liability,                # 负债合计(元)
                    cash_flow.cash_and_equivalents_at_end   # 期末现金及现金等价物余额(元)

                ).filter(
                    valuation.code.in_(stock_list)
                )
            df = get_fundamentals(q).fillna(value=0).set_index('code')
            # df = df.sort(columns = ['market_cap'], axis = 0,ascending = True)
            df = fun_shenqigongsi(df)

            index = []
            values = []
            values2 = []
            count = 1
            for stock in df.index:
                index.append(count)
                count = count + 1
                values.append(stock_PEG[stock])
                values2.append(stock_inc[stock]['inc_std'])

            obj1 = pd.Series(values, index=df.index)
            df['peg'] = obj1

            obj2 = pd.Series(values2, index=df.index)
            df['peg_inc'] = obj1
            
            df = df.sort('market_cap',ascending=True)
            idx = pd.Series(np.arange(1,len(df)+1), index=df['market_cap'].index.values)
            market_cap_rank = pd.DataFrame({'market_cap_rank': idx})
            df = pd.concat([df, market_cap_rank], axis=1)

            df = df.sort('peg',ascending=True)
            idx = pd.Series(np.arange(1,len(df)+1), index=df['peg'].index.values)
            peg_rank = pd.DataFrame({'peg_rank': idx})
            df = pd.concat([df, peg_rank], axis=1)

            df = df.sort('peg_inc',ascending=True)
            idx = pd.Series(np.arange(1,len(df)+1), index=df['peg_inc'].index.values)
            peg_inc_rank = pd.DataFrame({'peg_inc_rank': idx})
            df = pd.concat([df, peg_inc_rank], axis=1)

            df = df.sort('pe_ratio',ascending=True)
            idx = pd.Series(np.arange(1,len(df)+1), index=df['pe_ratio'].index.values)
            pe_ratio_rank = pd.DataFrame({'pe_ratio_rank': idx})
            df = pd.concat([df, pe_ratio_rank], axis=1)

            df = df.sort('roe',ascending=False)
            idx = pd.Series(np.arange(1,len(df)+1), index=df['roe'].index.values)
            roe_rank = pd.DataFrame({'roe_rank': idx})
            df = pd.concat([df, roe_rank], axis=1)

            df = df.sort('roa',ascending=False)
            idx = pd.Series(np.arange(1,len(df)+1), index=df['roa'].index.values)
            roa_rank = pd.DataFrame({'roa_rank': idx})
            df = pd.concat([df, roa_rank], axis=1)

            df = df.sort('pb_ratio',ascending=True)
            idx = pd.Series(np.arange(1,len(df)+1), index=df['pb_ratio'].index.values)
            pb_rank = pd.DataFrame({'pb_rank': idx})
            df = pd.concat([df, pb_rank], axis=1)

            df = df.sort('gross_profit_margin',ascending=False)
            idx = pd.Series(np.arange(1,len(df)+1), index=df['gross_profit_margin'].index.values)
            gross_profit_margin_rank = pd.DataFrame({'gross_profit_margin_rank': idx})
            df = pd.concat([df, gross_profit_margin_rank], axis=1)

            # 2007-4-1 ~ 2018-8-17
            # 策略收益729% 年化收益21% 最大回撤48% 最大策略收益950%
            # df['total_rank'] = df['market_cap_rank'] + df['pe_ratio_rank'] + df['roe_rank']
            # 
            # 策略收益530% 年化收益18% 最大回撤42% 最大策略收益730%
            # df['total_rank'] = df['pe_ratio_rank'] + df['roe_rank']
            # 
            # 策略收益841% 年化收益22% 最大回撤40% 最大策略收益1087%
            # df['total_rank'] = df['roe_rank']
            # 
            # 策略收益1071% 年化收益24% 最大回撤40% 最大策略收益1250%
            # df['total_rank'] = df['roa_rank']
            # 
            # 策略收益1113% 年化收益25% 最大回撤38% 最大策略收益1350%
            # df['total_rank'] = df['roa_rank'] + df['roe_rank']
            # 
            # 策略收益529% 年化收益18% 最大回撤40% 最大策略收益735%
            # df['total_rank'] = df['shenqi_rank']
            # 
            # 策略收益749% 年化收益21% 最大回撤40% 最大策略收益950%
            # df['total_rank'] = df['roa_rank'] + df['roe_rank'] + df['shenqi_rank']
            # 
            # 策略收益830% 年化收益22% 最大回撤41% 最大策略收益1050%
            # df['total_rank'] = df['roa_rank'] + df['roe_rank'] + df['pe_ratio_rank']
            # 
            # 策略收益812% 年化收益22% 最大回撤39% 最大策略收益1050%
            # df['total_rank'] = df['roa_rank'] + df['roe_rank'] + df['gross_profit_margin_rank']
            # 
            df['total_rank'] = df['roa_rank'] + df['roe_rank']

            df = df.sort('total_rank',ascending=True)

            return list(df.index)

        today = context.current_dt
        stock_list = list(get_all_securities(['stock'], today).index)
        
        stock_list = g.quantlib.unpaused(stock_list)
        stock_list = g.quantlib.fun_remove_cycle_industry(context, stock_list)
        stock_list = fun_filter_by_fcff(stock_list)
        # stock_list = fun_filter_by_pe(stock_list)
        # stock_list = fun_filter_by_rsi(stock_list, today)

        if len(stock_list) == 0:
            return []

        stock_dict = self.fun_get_inc(context, stock_list)
        old_stocks_list = []
        for stock in context.portfolio.positions.keys():
            if stock in stock_list:
                old_stocks_list.append(stock)

        stock_PEG = self.fun_cal_stock_PEG(context, stock_list, stock_dict)
        
        stock_list = []
        # buydict = {}
    
        for stock in stock_PEG.keys():
            if stock_PEG[stock] < 0.75 and stock_PEG[stock] > 0:
                stock_list.append(stock)
                # buydict[stock] = stock_PEG[stock]
        # cap_dict = fun_get_stock_market_cap(stock_list)
        # buydict = sorted(cap_dict.items(), key=lambda d:d[1], reverse=False)
        # stock_list = fun_filter_by_fcff(stock_list)
        stock_list = fun_sort_stock(stock_list, stock_PEG, stock_dict)

        buylist = []
        i = 0
        for idx in range(len(stock_list)):
            if i < context.lowPEG_hold_num:
                stock = stock_list[idx]
                buylist.append(stock) # 候选 stocks
                # print stock + ", PEG = "+ str(stock_PEG[stock])
                i += 1
        
        if len(buylist) < context.lowPEG_hold_num:
            old_stocks_PEG = stock_PEG #self.fun_cal_stock_PEG(context, old_stocks_list, stock_dict)
            tmpDict = {}
            tmpList = []
            for stock in old_stocks_list:
                if old_stocks_PEG[stock] < 1 and old_stocks_PEG[stock] > 0:
                    tmpDict[stock] = old_stocks_PEG[stock]
            tmpDict = sorted(tmpDict.items(), key=lambda d:d[1], reverse=False)
            i = len(buylist)
            for idx in tmpDict:
                if i < context.lowPEG_hold_num and idx[0] not in buylist:
                    buylist.append(idx[0])
                    i += 1

        # print str(len(stock_list)) + " / " + str(len(buylist))
        # print buylist

        return buylist

    def fun_assetAllocationSystem(self, context, buylist):
        def __fun_getEquity_ratio(context, __stocklist):
            __ratio = {}
            # 按风险平价配仓
            if __stocklist:
                __ratio = g.quantlib.fun_calStockWeight_by_risk(context, 2.58, __stocklist)

            return __ratio

        equity_ratio = __fun_getEquity_ratio(context, buylist)
        bonds_ratio  = __fun_getEquity_ratio(context, context.lowPEG_moneyfund)
        
        return equity_ratio, bonds_ratio

    def fun_calPosition(self, context, equity_ratio, bonds_ratio, lowPEG_ratio, portfolio_value):

        #risk_ratio = len(equity_ratio.keys())
        risk_ratio = 0
        for stock in equity_ratio.keys():
            if equity_ratio[stock] <> 0 and stock not in context.lowPEG_moneyfund:
                risk_ratio += 1
        risk_money = context.portfolio.portfolio_value * risk_ratio * context.lowPEG_ratio * context.lowPEG_risk_ratio
        maxrisk_money = risk_money * 1.7

        equity_value = 0
        if equity_ratio:
            equity_value = g.quantlib.fun_getEquity_value(equity_ratio, risk_money, maxrisk_money, context.lowPEG_confidencelevel,context.window)

        value_ratio = 0
        total_value = portfolio_value * lowPEG_ratio
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

class quantlib():
    
    def __init__(self, _period = '1d'):
        pass

    # 剔除周期性行业
    def fun_remove_cycle_industry(self, context, stock_list):
        cycle_industry = [#'A01', #	农业 	1993-09-17
                          #'A02', # 林业 	1996-12-06
                          #'A03', #	畜牧业 	1997-06-11
                          #'A04', #	渔业 	1993-05-07
                          #'A05', #	农、林、牧、渔服务业 	1997-05-30
                          'B06', # 煤炭开采和洗选业 	1994-01-06
                          'B07', # 石油和天然气开采业 	1996-06-28
                          'B08', # 黑色金属矿采选业 	1997-07-08
                          'B09', # 有色金属矿采选业 	1996-03-20
                          'B11', # 开采辅助活动 	2002-02-05
                          #'C13', #	农副食品加工业 	1993-12-15
                          #C14 	食品制造业 	1994-08-18
                          #C15 	酒、饮料和精制茶制造业 	1992-10-12
                          #C17 	纺织业 	1992-06-16
                          #C18 	纺织服装、服饰业 	1993-12-31
                          #C19 	皮革、毛皮、羽毛及其制品和制鞋业 	1994-04-04
                          #C20 	木材加工及木、竹、藤、棕、草制品业 	2005-05-10
                          #C21 	家具制造业 	1996-04-25
                          #C22 	造纸及纸制品业 	1993-03-12
                          #C23 	印刷和记录媒介复制业 	1994-02-24
                          #C24 	文教、工美、体育和娱乐用品制造业 	2007-01-10
                          'C25', # 石油加工、炼焦及核燃料加工业 	1993-10-25
                          'C26', # 化学原料及化学制品制造业 	1990-12-19
                          #C27 	医药制造业 	1993-06-29
                          'C28', # 化学纤维制造业 	1993-07-28
                          'C29', # 橡胶和塑料制品业 	1992-08-28
                          'C30', # 非金属矿物制品业 	1992-02-28
                          'C31', # 黑色金属冶炼及压延加工业 	1994-01-06
                          'C32', # 有色金属冶炼和压延加工业 	1996-02-15
                          'C33', # 金属制品业 	1993-11-30
                          'C34', # 通用设备制造业 	1992-03-27
                          'C35', # 专用设备制造业 	1992-07-01
                          'C36', # 汽车制造业 	1992-07-24
                          'C37', # 铁路、船舶、航空航天和其它运输设备制造业 	1992-03-31
                          'C38', # 电气机械及器材制造业 	1990-12-19
                          #C39 	计算机、通信和其他电子设备制造业 	1990-12-19
                          #C40 	仪器仪表制造业 	1993-09-17
                          'C41', # 其他制造业 	1992-08-14
                          #C42 	废弃资源综合利用业 	2012-10-26
                          'D44', # 电力、热力生产和供应业 	1993-04-16
                          #D45 	燃气生产和供应业 	2000-12-11
                          #D46 	水的生产和供应业 	1994-02-24
                          'E47', # 房屋建筑业 	1993-04-29
                          'E48', # 土木工程建筑业 	1994-01-28
                          'E50', # 建筑装饰和其他建筑业 	1997-05-22
                          #F51 	批发业 	1992-05-06
                          #F52 	零售业 	1992-09-02
                          'G53', # 铁路运输业 	1998-05-11
                          'G54', # 道路运输业 	1991-01-14
                          'G55', # 水上运输业 	1993-11-19
                          'G56', # 航空运输业 	1997-11-05
                          'G58', # 装卸搬运和运输代理业 	1993-05-05
                          #G59 	仓储业 	1996-06-14
                          #H61 	住宿业 	1993-11-18
                          #H62 	餐饮业 	1997-04-30
                          #I63 	电信、广播电视和卫星传输服务 	1992-12-02
                          #I64 	互联网和相关服务 	1992-05-07
                          #I65 	软件和信息技术服务业 	1992-08-20
                          'J66', # 货币金融服务 	1991-04-03
                          'J67', # 资本市场服务 	1994-01-10
                          'J68', # 保险业 	2007-01-09
                          'J69', # 其他金融业 	2012-10-26
                          'K70', # 房地产业 	1992-01-13
                          #L71 	租赁业 	1997-01-30
                          #L72 	商务服务业 	1996-08-29
                          #M73 	研究和试验发展 	2012-10-26
                          'M74', # 专业技术服务业 	2007-02-15
                          #N77 	生态保护和环境治理业 	2012-10-26
                          #N78 	公共设施管理业 	1992-08-07
                          #P82 	教育 	2012-10-26
                          #Q83 	卫生 	2007-02-05
                          #R85 	新闻和出版业 	1992-12-08
                          #R86 	广播、电视、电影和影视录音制作业 	1994-02-24
                          #R87 	文化艺术业 	2012-10-26
                          #S90 	综合 	1990-12-10
                          ]
        today = context.current_dt
        for industry in cycle_industry:
            stocks = get_industry_stocks(industry, today)
            stock_list = list(set(stock_list).difference(set(stocks)))
            
        return stock_list

    def fun_do_trade(self, context, trade_ratio, moneyfund):
    
        def __fun_tradeStock(context, stock, ratio):
            total_value = context.portfolio.portfolio_value
            if stock in moneyfund:
                self.fun_tradeBond(context, stock, total_value * ratio)
            else:
                curPrice = history(1,'1d', 'close', stock, df=False)[stock][-1]
                curValue = 0
                if stock in context.portfolio.positions.keys():
                  curValue = context.portfolio.positions[stock].total_amount * curPrice
                Quota = total_value * ratio
                if Quota:
                    if abs(Quota - curValue) / Quota >= 0.25:
                        if Quota > curValue:
                            cash = context.portfolio.cash
                            if cash >= Quota * 0.25:
                                self.fun_trade(context, stock, Quota)
                        else:
                            self.fun_trade(context, stock, Quota)
                else:
                    self.fun_trade(context, stock, Quota)
    
        trade_list = trade_ratio.keys()
    
        myholdstock = context.portfolio.positions.keys()
        total_value = context.portfolio.portfolio_value
    
        # 已有仓位
        holdDict = {}
        h = history(1, '1d', 'close', myholdstock, df=False)
        for stock in myholdstock:
            tmpW = round((context.portfolio.positions[stock].total_amount * h[stock])/total_value, 2)
            holdDict[stock] = float(tmpW)
    
        # 对已有仓位做排序
        tmpDict = {}
        for stock in holdDict:
            if stock in trade_ratio:
                tmpDict[stock] = round((trade_ratio[stock] - holdDict[stock]), 2)
        tradeOrder = sorted(tmpDict.items(), key=lambda d:d[1], reverse=False)
    
        _tmplist = []
        for idx in tradeOrder:
            stock = idx[0]
            __fun_tradeStock(context, stock, trade_ratio[stock])
            _tmplist.append(stock)
    
        # 交易其他股票
        for i in range(len(trade_list)):
            stock = trade_list[i]
            if len(_tmplist) != 0 :
                if stock not in _tmplist:
                    __fun_tradeStock(context, stock, trade_ratio[stock])
            else:
                __fun_tradeStock(context, stock, trade_ratio[stock])

    def fun_getEquity_value(self, equity_ratio, risk_money, maxrisk_money, confidence_ratio, lag):
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

    def fun_get_Dividend_yield(self, stocks, statsDate):
        # 按照派息日计算，计算过去 12个月的派息率(TTM)
        start_date = statsDate - dt.timedelta(365)
        end_date = statsDate
        year = statsDate.year
        #将当前股票池转换为聚源的6位股票池
        stocks_symbol=[]
        for s in stocks:
            stocks_symbol.append(s[0:6])
        
        #查找聚源的内部编码
        df_code = jy.run_query(query(
                jy.SecuMain.InnerCode,       #证券内部编码
                jy.SecuMain.SecuCode         #证券代码
            ))
        df_code = df_code[df_code.SecuCode.isin(stocks_symbol)]
        list_InnerCode = list(set(list(df_code['InnerCode'])))

        # 按派息日，查找过去12个月的分红记录
        #查找有分红的（受派息日约束）；
        df = jy.run_query(query(
                jy.LC_Dividend.InnerCode,   #证券内部编码
                jy.LC_Dividend.CashDiviRMB, #派现(含税/人民币元)（***说明文档没说明分红单位是每股，还是每十股）
                jy.LC_Dividend.DiviBaseBeforeChange, #变更前分红股本基数(股)（***又是不提单位）
            ).filter(
                jy.LC_Dividend.ToAccountDate <= end_date,
                jy.LC_Dividend.ToAccountDate >= start_date,
                jy.LC_Dividend.IfDividend == 1      #不考虑特殊分红和其他分红
            )).fillna(value=0, method=None, axis=0)

        df = df[df.InnerCode.isin(list_InnerCode)]
        df = df.reset_index(drop = True)

        df.index = df['InnerCode']
        df_code = df_code[df_code.InnerCode.isin(list(set(list(df['InnerCode']))))]
        df = df.drop(['InnerCode'],axis=1)
        df_code.index = df_code['InnerCode']
        df = df.join([df_code])

        df['SecuCode']=map(normalize_code,list(df['SecuCode']))

        # 获取最新股本
        q = query(valuation.code, valuation.capitalization)
        df2 = get_fundamentals(q).fillna(value=0)

        df2 = df2[df2.code.isin(df.SecuCode)]
        df2['SecuCode'] = df2['code']
        df2 = df2.drop(['code'], axis=1)

        # 合并成一个 dataframe
        df = df.merge(df2,on='SecuCode')
        df.index = list(df['SecuCode'])
        df = df.drop(['SecuCode'], axis=1)

        # 转换成 float
        df['DiviBaseBeforeChange'] = map(float, df['DiviBaseBeforeChange'])
        # 计算股份比值
        # *** 因为聚源的 DiviBaseBeforeChange 数据全数为 0，也就是没有记录分红时的股份基数，所以暂时默认都是 1.0
        # 实际市场里，因为送股/配股转股的缘故，分红时的股份基数，往往会与当下的股份不一致
        #df['CAP_RATIO'] = df['DiviBaseBeforeChange'] / (df['capitalization'] * 10000)
        df['CAP_RATIO'] = 1.0

        df['CashDiviRMB'] = map(float, df['CashDiviRMB'])
        # 计算相对于目前股份而言的分红额度
        df['CashDiviRMB'] = df['CashDiviRMB'] * df['CAP_RATIO']

        df = df.drop(['DiviBaseBeforeChange','capitalization','CAP_RATIO'], axis=1)
        
        #接下来这一步是考虑多次分红的股票，因此需要累加股票的多次分红
        df = df.groupby(df.index).sum()
        
        #得到当前股价
        Price=history(1, unit='1d', field='close', security_list=list(df.index), df=True, skip_paused=False, fq='pre')
        Price=Price.T

        if df.empty :
        	return df
        
        df['pre_close']=Price
        #计算股息率 = 股息/股票价格，* 10 是因为取到的是每 10 股分红
        df['divpercent']=df['CashDiviRMB']/(df['pre_close'] * 10)
        df = df.drop(['pre_close', 'CashDiviRMB'], axis=1)

        df = df[df.divpercent > 0]
        #print df
        #

        return df

    def fun_get_Divid_by_year(self, context, stocks):
        year = context.current_dt.year - 1
        #将当前股票池转换为国泰安的6位股票池
        stocks_symbol=[]
        for s in stocks:
            stocks_symbol.append(s[0:6])

        df = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,                # 股票代码
                gta.STK_DIVIDEND.DECLAREDATE,           # 分红消息的时间
            ).filter(
                gta.STK_DIVIDEND.ISDIVIDEND == 'Y',     #有分红的股票
                gta.STK_DIVIDEND.DIVDENDYEAR == year,
                gta.STK_DIVIDEND.TERMCODE == 'P2702',   # 年度分红
                gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol)
            )).fillna(value=0, method=None, axis=0)
        # 转换时间格式
        df['pubtime'] = map(lambda x: int(x.split('-')[0]+x.split('-')[1]+x.split('-')[2]),df['DECLAREDATE'])
        # 取得当前时间
        currenttime  = int(str(context.current_dt)[0:4]+str(context.current_dt)[5:7]+str(context.current_dt)[8:10])
        # 选择在当前时间能看到的记录
        df = df[(df.pubtime < currenttime)]
        # 得到目前看起来，有上一年度年度分红的股票
        stocks_symbol_this_year = list(df['SYMBOL'])
        # 得到目前看起来，上一年度没有年度分红的股票
        stocks_symbol_past_year = list(set(stocks_symbol) - set(stocks_symbol_this_year))
        
        # 查有上一年度年度分红的
        df1 = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,                # 股票代码
                gta.STK_DIVIDEND.DIVIDENTBT,            # 股票分红
                gta.STK_DIVIDEND.DECLAREDATE,           # 分红消息的时间
                gta.STK_DIVIDEND.DISTRIBUTIONBASESHARES # 分红时的股本基数
            ).filter(
                gta.STK_DIVIDEND.ISDIVIDEND == 'Y',     #有分红的股票
                gta.STK_DIVIDEND.DIVDENDYEAR == year,
                gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol_this_year)
            )).fillna(value=0, method=None, axis=0)

        df1['pubtime'] = map(lambda x: int(x.split('-')[0]+x.split('-')[1]+x.split('-')[2]),df1['DECLAREDATE'])
        currenttime  = int(str(context.current_dt)[0:4]+str(context.current_dt)[5:7]+str(context.current_dt)[8:10])
        df1 = df1[(df1.pubtime < currenttime)]

        # 求上上年的年度分红
        df2 = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,                # 股票代码
                gta.STK_DIVIDEND.DIVIDENTBT,            # 股票分红
                gta.STK_DIVIDEND.DECLAREDATE,           # 分红消息的时间
                gta.STK_DIVIDEND.DISTRIBUTIONBASESHARES # 分红时的股本基数
            ).filter(
                gta.STK_DIVIDEND.ISDIVIDEND == 'Y',     #有分红的股票
                gta.STK_DIVIDEND.DIVDENDYEAR == (year - 1),
                gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol_past_year)
            )).fillna(value=0, method=None, axis=0)
        
        df2['pubtime'] = map(lambda x: int(x.split('-')[0]+x.split('-')[1]+x.split('-')[2]),df2['DECLAREDATE'])
        currenttime  = int(str(context.current_dt)[0:4]+str(context.current_dt)[5:7]+str(context.current_dt)[8:10])
        df2 = df2[(df2.pubtime < currenttime)]
        
        df= pd.concat((df2,df1))

        df['SYMBOL']=map(normalize_code,list(df['SYMBOL']))
        df.index=list(df['SYMBOL'])
        
        # 获取最新股本
        q = query(valuation.code, valuation.capitalization
                ).filter(valuation.code.in_(list(df.index)))
        
        df3 = get_fundamentals(q).fillna(value=0)
        df3['SYMBOL'] = df3['code']
        df3 = df3.drop(['code'], axis=1)

        # 合并成一个 dataframe
        df = df.merge(df3,on='SYMBOL')
        df.index = list(df['SYMBOL'])

        # 转换成 float
        df['DISTRIBUTIONBASESHARES'] = map(float, df['DISTRIBUTIONBASESHARES'])
        # 计算股份比值
        df['CAP_RATIO'] = df['DISTRIBUTIONBASESHARES'] / (df['capitalization'] * 10000)
        
        df['DIVIDENTBT'] = map(float, df['DIVIDENTBT'])
        # 计算相对于目前股份而言的分红额度
        df['DIVIDENTBT'] = df['DIVIDENTBT'] * df['CAP_RATIO']
        df = df.drop(['SYMBOL','DECLAREDATE','DISTRIBUTIONBASESHARES','capitalization','CAP_RATIO'], axis=1)
        
        #接下来这一步是考虑多次分红的股票，因此需要累加股票的多次分红
        df = df.groupby(df.index).sum()
        
        #得到当前股价
        Price=history(1, unit='1d', field='close', security_list=list(df.index), df=True, skip_paused=False, fq='pre')
        Price=Price.T
        
        df['pre_close']=Price
    
        #计算股息率 = 股息/股票价格，* 10 是因为取到的是每 10 股分红
        df['divpercent']=df['DIVIDENTBT']/(df['pre_close'] * 10)
        
        df['code'] = np.array(df.index)
        
        return df

    def fun_calStockWeight_by_risk(self, context, confidencelevel, stocklist):
        
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
            curRisk = __fun_calstock_risk_ES(stock, context.window, confidencelevel)

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

    def fun_tradeBond(self, context, stock, Value):
        hStocks = history(1, '1d', 'close', stock, df=False)
        curPrice = hStocks[stock]
        curValue = 0
        if stock in context.portfolio.positions.keys():
          curValue = context.portfolio.positions[stock].total_amount * curPrice
        deltaValue = abs(Value - curValue)
        if deltaValue > (curPrice*100):
            if Value > curValue:
                cash = context.portfolio.cash
                if cash > (curPrice*100):
                    self.fun_trade(context, stock, Value)
            else:
                # 如果是银华日利，多卖 100 股，避免个股买少了
                if stock == '511880.XSHG':
                    Value -= curPrice*100
                self.fun_trade(context, stock, Value)

    # 剔除上市时间较短的产品
    def fun_delNewShare(self, context, equity, deltaday):
        deltaDate = context.current_dt.date() - dt.timedelta(deltaday)
    
        tmpList = []
        for stock in equity:
            if get_security_info(stock).start_date < deltaDate:
                tmpList.append(stock)
    
        return tmpList

    def unpaused(self, _stocklist):
        current_data = get_current_data()
        return [s for s in _stocklist if not current_data[s].paused]

    def fun_trade(self, context, stock, value):
        self.fun_setCommission(context, stock)
        order_target_value(stock, value)

    def fun_setCommission(self, context, stock):
        if stock in context.lowPEG_moneyfund:
            set_order_cost(OrderCost(open_tax=0, close_tax=0, open_commission=0, close_commission=0, close_today_commission=0, min_commission=0), type='stock')
        else:
            # set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
            dt = context.current_dt

            if dt > datetime.datetime(2013, 1, 1):
                set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0003, min_cost=5))

            elif dt > datetime.datetime(2011, 1, 1):
                set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))

            elif dt > datetime.datetime(2009, 1, 1):
                set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))

            else:
                set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))