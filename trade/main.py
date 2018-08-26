from kuanke.user_space_api import *

import pandas as pd
import numpy as np
import datetime as dt
from math import isnan

import talib

import scipy as sp
import scipy.optimize

from scipy import linalg as sla
from scipy import spatial
from jqdata import jy
from jqlib.technical_analysis import *
from hmmlearn.hmm import GaussianHMM

# from tradelib import *
# from adjust_position_rules import *
# from adjust_condition_rules import *
# from filter_stock_rules import *
# from filter_query_rules import *


'''选择规则组合成一个量化策略'''
def select_strategy():
    '''
    策略选择设置说明。
    策略由以下步骤规则组合，组合而成:
    1.持仓股票的处理规则
    2.调仓条件判断规则
    3.Query选股规则 (可选，有些规则可能不需要这个)
    4.股票池过滤规则 (可选，有些规则可能不需要这个)
    5.调仓规则
    6.其它规则(如统计)

    每个步骤的规则组合为一个二维数组
    一维指定由什么规则组成，注意顺序，程序将会按顺序创建，按顺序执行。
    不同的规则组合可能存在一定的顺序关系。
    二维指定具体规则配置，由 [0.是否启用，1.描述，2.规则实现类名，3.规则传递参数(dict)]] 组成。
    注：所有规则类都必需继承自Rule类或Rule类的子类
    '''

    period = 30  # 调仓周期
    buy_count = 5  # 购股数量
    
    # 配置 1.持仓股票的处理规则 (这里主要配置是否进行个股止损止盈)
    g.position_stock_config = [
        [False, '个股止损', Stop_loss_stocks, {
            'period': period  # 调仓频率，日
        }],
        [True, 'HMM止损', Stop_stocks_hmm, {
            
        }],
        [False, '个股止盈', Stop_profit_stocks, {
            'period': period,  # 调仓频率，日
        }]
    ]

    # 配置 2.调仓条件判断规则
    # 所有规则全部满足才会执行调仓操作
    g.adjust_condition_config = [
        [False, '指数最高低价比值止损', Stop_loss_by_price, {  # 该调仓条件可能会产生清仓行为
            'index': '000001.XSHG',  # 使用的指数,默认 '000001.XSHG'
            'day_count': 160,  # 可选 取day_count天内的最高价，最低价。默认160
            'multiple': 2.2  # 可选 最高价为最低价的multiple倍时，触 发清仓
        }],
        [False, '指数三乌鸦止损', Stop_loss_by_3_black_crows, {  #  该调仓条件可能会产生清仓行为
            'index': '000001.XSHG',  # 使用的指数,默认 '000001.XSHG'
            'dst_drop_minute_count': 90,  # 可选，在三乌鸦触发情况下，一天之内有多少分钟涨幅<0,则触发止损，默认60分钟
        }],
        [False, '28实时止损', Stop_loss_by_28_index, {  # 该调仓条件可能会产生清仓行为
            'index2': '000016.XSHG',  # 大盘指数
            'index8': '399333.XSHE',  # 小盘指数
            'index_growth_rate': 0.01,  # 判定调仓的二八指数20日增幅
            'dst_minute_count_28index_drop': 120  # 符合条件连续多少分钟则清仓
        }],
        # 这个一定要开启
        [False, '调仓时间', Time_condition, {
            'sell_hour': 14,  # 调仓时间,小时
            'sell_minute': 50,  # 调仓时间，分钟
            'buy_hour': 14,  # 调仓时间,小时
            'buy_minute': 50 # 调仓时间，分钟
        }],
        [False, '28调仓择时', Index28_condition, {  # 该调仓条件可能会产生清仓行为
            'index2': '000016.XSHG',  # 大盘指数
            'index8': '399333.XSHE',  # 小盘指数
            'index_growth_rate': 0.01,  # 判定调仓的二八指数20日增幅
        }],
        [True, '调仓日计数器', Period_condition, {  # 多少天调仓一次
            'period': period,  # 调仓周期,日
        }],
    ]

    # 配置 3.Query选股规则
    g.pick_stock_by_query_config = [
        [False, '选取小市值', Pick_small_cap, {}],
        [True, '区间选取小市值', Pick_by_market_cap, {
            'mcap_min': 0,
            'mcap_max': 3000000
        }],
        [False, '过滤PE', Filter_pe, {  # 市盈率=股价/每股盈利
            'pe_min': 0,  # 最小PE
            'pe_max': 40  # 最大PE
        }],
        [False, '过滤EPS', Filter_eps, {  # 每股盈余=盈余/流通在外股数
            'eps_min': 0.025  # 最小EPS
        }],
        [False, '初选股票数量', Filter_limite, {
            'pick_stock_count': 200  # 备选股票数目
        }]
    ]

    # 配置 4.股票池过滤规则
    g.filter_stock_list_config = [
        [False, '过滤创业板', Filter_gem, {}],
        [False, '过滤上证', Filter_sh, {}],
        [False, '过滤深证', Filter_sz, {}],
        [False, '过滤ST', Filter_st, {}],
        [True, '过滤停牌', Filter_paused_stock, {}],
        [False, '过滤涨停', Filter_limitup, {}],
        [False, '过滤跌停', Filter_limitdown, {}],
        [False, '过滤n日增长率为负的股票', Filter_growth_is_down, {
            'day_count': 20  # 判断多少日内涨幅
        }],
        [True, '过滤周期性行业', Filter_cycle_industry, {}],
        [False, '过滤黑名单', Filter_blacklist, {}],
        [False, '股票评分', Filter_rank, {
            'rank_stock_count': 20  # 评分股数
        }],
        [False, '庄股评分', Filter_cash_flow_rank, {'rank_stock_count': 600}],
        # [False,'筹码分布评分',Filter_chip_density,{'rank_stock_count':200}],
        
        [True, '剔除上市时间较短的产品', Filter_new_share, {'deltaday':60}],
        [True, '选取现金流强劲股票', Filter_fcff, {}],
        [True, '过滤PEG', Filter_low_peg, {  # peg = 市盈率/净利润增长率
            'peg_max': 0.75,  # 最大peg
            'peg_min': 0.0  # 最小peg
        }],
        [True, '获取最终选股数', Filter_buy_count, {
            'buy_count': buy_count  # 最终入选股票数
        }],
    ]

    # 配置 5.调仓规则
    g.adjust_position_config = [
        [False, '卖出股票', Sell_stocks, {
        }],
        [False, '买入股票', Buy_stocks, {
            'buy_count': buy_count,  # 最终买入股票数
        }],
        [False, '按比重买入股票', Buy_stocks_portion, {'buy_count': buy_count}],
        [True, 'VaR方式买入股票', Buy_stocks_var, {
            'buy_count': buy_count,
            'lowPEG_risk_ratio': 0.03 / buy_count,
            'lowPEG_ratio':1.0,
            'confidencelevel':1.96
        }],
    ]

    # 配置 6.其它规则
    g.other_config = [
        [False, '统计', Stat, {}]
    ]


# 1 设置参数
def set_params():
    # 设置基准收益
    set_benchmark('000300.XSHG')


# 2 设置中间变量
def set_variables(context):
    # 设置 VaR 仓位控制参数。风险敞口: 0.05,
    # 正态分布概率表，标准差倍数以及置信率: 0.96, 95%; 2.06, 96%; 2.18, 97%; 2.34, 98%; 2.58, 99%; 5, 99.9999%
    # 赋闲资金可以买卖银华日利做现金管理: ['511880.XSHG']

    context.lowPEG_moneyfund = ['511880.XSHG']

    set_slip_fee(context)


# 3 设置回测条件
def set_backtest():
    set_option('use_real_price', True)  # 用真实价格交易
    log.set_level('order', 'error')
    log.set_level('strategy', 'info')


def initialize(context):
    log.info("==> initialize @ %s" % (str(context.current_dt)))

    g.hmm = GaussianHMM(n_components= 3, covariance_type="diag", n_iter=2000)
    context.lastMonth = -1

    set_params()  # 1设置策略参数
    set_variables(context)  # 2设置中间变量
    set_backtest()  # 3设置回测条件

    select_strategy()

    g.init_version = 1

    # -----1.持仓股票的处理规则:-----
    g.position_stock_rules = create_rules(g.position_stock_config)

    # -----2.调仓条件判断规则:-----
    g.adjust_condition_rules = create_rules(g.adjust_condition_config)

    # -----3.Query选股规则:-----
    g.pick_stock_by_query_rules = create_rules(g.pick_stock_by_query_config)

    # -----4.股票池过滤规则:-----
    g.filter_stock_list_rules = create_rules(g.filter_stock_list_config)

    # -----5.调仓规则器:-----
    g.adjust_position_rules = create_rules(g.adjust_position_config)

    # -----6.其它规则:-------
    g.other_rules = create_rules(g.other_config)

    # 把所有规则合并排重生成一个总的规则收录器。以方便各处共同调用的
    g.all_rules = list(set(g.position_stock_rules
                           + g.adjust_condition_rules
                           + g.pick_stock_by_query_rules
                           + g.filter_stock_list_rules
                           + g.adjust_position_rules
                           + g.other_rules
                           ))

    for rule in g.all_rules:
        rule.initialize(context)


    # 打印规则参数
    log_param()

    context.trade_ratio = {}
    run_daily(trade_main, '10:30')


# 开盘
def before_trading_start(context):
    # log.info("==========================================================================")

    for rule in g.all_rules:
        rule.before_trading_start(context)


# 收盘
def after_trading_end(context):
    for rule in g.all_rules:
        rule.after_trading_end(context)

    # 得到当前未完成订单
    orders = get_open_orders()
    for _order in orders.values():
        log.info("canceled uncompleted order: %s" % (_order.order_id))


# 进程启动(一天一次)
def process_initialize(context):
    
    # 针对模拟盘，每天开始前重新初始化雪球
    if context.run_params.type == 'sim_trade':
        xq_loading(context)
    
    for rule in g.all_rules:
        rule.process_initialize(context)


# 交易
def trade_main(context):
    # log.info("handle_data.", g, context, data)
    context.flags_can_sell = True
    context.flags_can_buy = True
    data = {}

    # 持仓股票动作的执行,目前为个股止损止盈
    for rule in g.position_stock_rules:
        rule.handle_data(context, data)

    # 执行其它辅助规则
    for rule in g.other_rules:
        rule.handle_data(context, data)

    # ----------这部分当前主要做扩展用，未实现则空跑--------------
    # 这里执行选股器调仓器的handle_data主要是为了扩展某些选股方式可能需要提前处理数据。
    # 举例：动态获取黑名单，可以调仓前一段时间先执行。28小市值规则这里都是空动作。

    for rule in g.pick_stock_by_query_rules:
        rule.handle_data(context, data)

    for rule in g.filter_stock_list_rules:
        rule.handle_data(context, data)

    # 调仓器的分钟处理
    for rule in g.adjust_position_rules:
        rule.handle_data(context, data)

    # 判断是否满足调仓条件，所有规则全部满足才会执行下面的调仓操作
    can_adjust = True
    for rule in g.adjust_condition_rules:
        rule.handle_data(context, data)
        if not rule.can_adjust:
            can_adjust = False
            break
            # return

    if can_adjust:
        # ---------------------调仓--------------------------
        log.info("handle_data: ==> 满足条件进行调仓")

        # 调仓前预处理
        for rule in g.all_rules:
            rule.before_adjust_start(context, data)

        # Query 选股
        q = None
        for rule in g.pick_stock_by_query_rules:
            q = rule.filter(context, data, q)

        # 过滤股票列表
        if q != None :
            stock_list = list(get_fundamentals(q)['code'])
        else:
            stock_list = list(get_all_securities(['stock'], context.current_dt).index)
        
        # stock_list = list(get_fundamentals(q)['code']) if q != None else []
        for rule in g.filter_stock_list_rules:
            stock_list = rule.filter(context, data, stock_list)

        log.info("handle_data: 选股后可买股票: %s" % (stock_list))

        # 执行调仓
        for rule in g.adjust_position_rules:
            rule.adjust(context, data, stock_list)

        # 调仓后处理
        for rule in g.all_rules:
            rule.after_adjust_end(context, data)

    # ----------------------------------------------------
    
    fun_do_trade(context, context.trade_ratio, context.lowPEG_moneyfund)
    

# 这里示例进行模拟更改回测时，如何调整策略,基本通用代码。
def after_code_changed(context):
    # # 因为是示例，在不是模拟里更新回测代码的时候，是不需要的，所以直接退出
    # return
    print '更新代码：'
    # 调整策略通用实例代码
    # 获取新策略
    select_strategy()

    # 按新策略顺序重整规则列表，如果对象之前存在，则移到新列表，不存在则新建。
    # 不管之前旧的规则列表是什么顺序，一率按新列表重新整理。
    def check_chang(rules, config):
        nl = []
        for c in config:
            # 按顺序循环处理新规则
            if not c[g.cs_enabled]:  # 不使用则跳过
                continue
            # 查找旧规则是否存在
            find_old = None
            for old_r in rules:
                if old_r.__class__ == c[g.cs_class_name]:
                    find_old = old_r
                    break
            if find_old != None:
                # 旧规则存在则添加到新列表中,并调用规则的更新函数，更新参数。
                nl.append(find_old)
                find_old.update_params(context, c[g.cs_param])
            else:
                # 旧规则不存在，则创建并添加
                new_r = create_rule(c[g.cs_class_name], c[g.cs_param], c[g.cs_memo])
                nl.append(new_r)
                # 调用初始化时该执行的函数
                new_r.initialize(context)
        return nl

    # 重整所有规则
    g.position_stock_rules = check_chang(g.position_stock_rules, g.position_stock_config)
    g.adjust_condition_rules = check_chang(g.adjust_condition_rules, g.adjust_condition_config)
    g.pick_stock_by_query_rules = check_chang(g.pick_stock_by_query_rules, g.pick_stock_by_query_config)
    g.filter_stock_list_rules = check_chang(g.filter_stock_list_rules, g.filter_stock_list_config)
    g.adjust_position_rules = check_chang(g.adjust_position_rules, g.adjust_position_config)
    g.other_rules = check_chang(g.other_rules, g.other_config)

    # 重新生成所有规则的list
    g.all_rules = list(set(
        g.position_stock_rules
        + g.adjust_condition_rules
        + g.pick_stock_by_query_rules
        + g.filter_stock_list_rules
        + g.adjust_position_rules
        + g.other_rules
    ))
    log_param()


# 显示策略组成
def log_param():
    def get_rules_str(rules):
        return '\n'.join(['   %d.%s ' % (i + 1, str(r)) for i, r in enumerate(rules)]) + '\n'

    s = '\n---------------------策略一览：规则组合与参数----------------------------\n'
    s += '一、持仓股票的处理规则:\n' + get_rules_str(g.position_stock_rules)
    s += '二、调仓条件判断规则:\n' + get_rules_str(g.adjust_condition_rules)
    s += '三、Query选股规则:\n' + get_rules_str(g.pick_stock_by_query_rules)
    s += '四、股票池过滤规则:\n' + get_rules_str(g.filter_stock_list_rules)
    s += '五、调仓规则:\n' + get_rules_str(g.adjust_position_rules)
    s += '六、其它规则:\n' + get_rules_str(g.other_rules)
    s += '--------------------------------------------------------------------------'
    log.info(s)

