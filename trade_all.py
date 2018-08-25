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

def handle_data(context, data):
    # 持仓股票动作的执行,目前为个股止损止盈
    for rule in g.position_stock_rules:
        rule.handle_data(context, data)

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



class Rule(object):
    """策略规则基类"""

    # 持仓操作的事件
    on_open_position = None  # 买股调用外部函数
    on_close_position = None  # 卖股调用外部函数
    on_clear_position = None  # 清仓调用外部函数
    on_get_obj_by_class_type = None  # 通过类的类型查找已创建的类的实例
    memo = ''  # 对象简要说明

    def __init__(self, params):
        pass

    def initialize(self, context):
        pass

    def handle_data(self, context, data):
        pass

    def before_trading_start(self, context):
        pass

    def after_trading_end(self, context):
        pass

    def process_initialize(self, context):
        pass

    def after_code_changed(self, context):
        pass

    # 卖出股票时调用的函数
    # price为当前价，amount为发生的股票数,is_normail正常规则卖出为True，止损卖出为False
    def when_sell_stock(self, position, order, is_normal):
        pass

    # 买入股票时调用的函数
    # price为当前价，amount为发生的股票数
    def when_buy_stock(self, stock, order):
        pass

    # 清仓时调用的函数
    def when_clear_position(self, context):
        pass

    # 调仓前调用
    def before_adjust_start(self, context, data):
        pass

    # 调仓后调用用
    def after_adjust_end(slef, context, data):
        pass

    # 更改参数
    def update_params(self, context, params):
        pass

    # 持仓操作事件的简单判断处理，方便使用。
    def open_position(self, security, value):
        if self.on_open_position != None:
            return self.on_open_position(self, security, value)

    def close_position(self, position, is_normal=True):
        if self.on_close_position != None:
            return self.on_close_position(self, position, is_normal=True)

    def clear_position(self, context):
        if self.on_clear_position != None:
            self.on_clear_position(self, context)

    # 通过类的类型获取已创建的类的实例对象
    # 示例 obj = get_obj_by_class_type(Index28_condition)
    def get_obj_by_class_type(self, class_type):
        if self.on_get_obj_by_class_type != None:
            return self.on_get_obj_by_class_type(class_type)
        else:
            return None

    # 为日志显示带上是哪个规则器输出的
    def log_info(self, msg):
        log.info('%s: %s' % (self.memo, msg))

    def log_warn(self, msg):
        log.warn('%s: %s' % (self.memo, msg))

    def log_debug(self, msg):
        log.debug('%s: %s' % (self.memo, msg))

    def log_weixin(self, context, msg):
        self.log_warn(msg)
        # 只在模拟时发微信,以免浪费发信次数限额
        if context.run_params.type == 'sim_trade':
            send_message(self.__str__() + ': ' + msg, channel='weixin')


class Adjust_condition(Rule):
    """调仓条件判断器基类"""

    # 返回能否进行调仓
    @property
    def can_adjust(self):
        return True


class Filter_query(Rule):
    """选股query过滤器基类"""

    def filter(self, context, data, q):
        return None


class Filter_stock_list(Rule):
    """选股stock_list过滤器基类"""

    def filter(self, context, data, stock_list):
        return None


class Adjust_position(Rule):
    """调仓操作基类"""

    def adjust(self, context, data, buy_stocks):
        pass


class Stat(Rule):
    """统计类"""

    def __init__(self, params):
        # 加载统计模块
        self.trade_total_count = 0
        self.trade_success_count = 0
        self.statis = {'win': [], 'loss': []}

    def after_trading_end(self, context):
        self.report(context)

    def when_sell_stock(self, position, order, is_normal):
        if order.filled > 0:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            self.watch(position.security, order.filled, position.avg_cost, position.price)

    def reset(self):
        self.trade_total_count = 0
        self.trade_success_count = 0
        self.statis = {'win': [], 'loss': []}

    # 记录交易次数便于统计胜率
    # 卖出成功后针对卖出的量进行盈亏统计
    def watch(self, stock, sold_amount, avg_cost, cur_price):
        self.trade_total_count += 1
        current_value = sold_amount * cur_price
        cost = sold_amount * avg_cost

        percent = round((current_value - cost) / cost * 100, 2)
        if current_value > cost:
            self.trade_success_count += 1
            win = [stock, percent]
            self.statis['win'].append(win)
        else:
            loss = [stock, percent]
            self.statis['loss'].append(loss)

    def report(self, context):
        cash = context.portfolio.cash
        totol_value = context.portfolio.portfolio_value
        position = 1 - cash / totol_value
        self.log_info("收盘后持仓概况:%s" % str(list(context.portfolio.positions)))
        self.log_info("仓位概况:%.2f" % position)
        self.print_win_rate(context.current_dt.strftime("%Y-%m-%d"), context.current_dt.strftime("%Y-%m-%d"), context)

    # 打印胜率
    def print_win_rate(self, current_date, print_date, context):
        if str(current_date) == str(print_date):
            win_rate = 0
            if 0 < self.trade_total_count and 0 < self.trade_success_count:
                win_rate = round(self.trade_success_count / float(self.trade_total_count), 3)

            most_win = self.statis_most_win_percent()
            most_loss = self.statis_most_loss_percent()
            starting_cash = context.portfolio.starting_cash
            total_profit = self.statis_total_profit(context)
            if len(most_win) == 0 or len(most_loss) == 0:
                return

            s = '\n------------绩效报表------------'
            s += '\n交易次数: {0}, 盈利次数: {1}, 胜率: {2}'.format(self.trade_total_count, self.trade_success_count,
                                                          str(win_rate * 100) + str('%'))
            s += '\n单次盈利最高: {0}, 盈利比例: {1}%'.format(most_win['stock'], most_win['value'])
            s += '\n单次亏损最高: {0}, 亏损比例: {1}%'.format(most_loss['stock'], most_loss['value'])
            s += '\n总资产: {0}, 本金: {1}, 盈利: {2}, 盈亏比率：{3}%'.format(starting_cash + total_profit, starting_cash,
                                                                  total_profit, total_profit / starting_cash * 100)
            s += '\n--------------------------------'
            self.log_info(s)

    # 统计单次盈利最高的股票
    def statis_most_win_percent(self):
        result = {}
        for statis in self.statis['win']:
            if {} == result:
                result['stock'] = statis[0]
                result['value'] = statis[1]
            else:
                if statis[1] > result['value']:
                    result['stock'] = statis[0]
                    result['value'] = statis[1]

        return result

    # 统计单次亏损最高的股票
    def statis_most_loss_percent(self):
        result = {}
        for statis in self.statis['loss']:
            if {} == result:
                result['stock'] = statis[0]
                result['value'] = statis[1]
            else:
                if statis[1] < result['value']:
                    result['stock'] = statis[0]
                    result['value'] = statis[1]

        return result

    # 统计总盈利金额
    def statis_total_profit(self, context):
        return context.portfolio.portfolio_value - context.portfolio.starting_cash

    def __str__(self):
        return '策略绩效统计'

# ==================== 调仓条件判断器实现 ==============================================


class Time_condition(Adjust_condition):
    """调仓时间控制器"""

    def __init__(self, params):
        # 配置调仓时间（24小时分钟制）
        self.sell_hour = params.get('sell_hour', 10)
        self.sell_minute = params.get('sell_minute', 15)
        self.buy_hour = params.get('buy_hour', 14)
        self.buy_minute = params.get('buy_minute', 50)

    def update_params(self, context, params):
        self.sell_hour = params.get('sell_hour', self.sell_hour)
        self.sell_minute = params.get('sell_minute', self.sell_minute)
        self.buy_hour = params.get('buy_hour', self.buy_hour)
        self.buy_minute = params.get('buy_minute', self.buy_minute)

    @property
    def can_adjust(self):
        return self.t_can_adjust

    def handle_data(self, context, data):
        hour = context.current_dt.hour
        minute = context.current_dt.minute
        self.t_can_adjust = False
        if (hour == self.sell_hour and minute == self.sell_minute):
            self.t_can_adjust = True
            context.flags_can_sell = True
        else:
            context.flags_can_sell = False
        if (hour == self.buy_hour and minute == self.buy_minute):
            self.t_can_adjust = True
            context.flags_can_buy = True
        else:
            context.flags_can_buy = False

    def __str__(self):
        return '调仓时间控制器: [卖出时间: %d:%d] [买入时间: %d:%d]' % (
            self.sell_hour, self.sell_minute, self.buy_hour, self.buy_minute)


class Period_condition(Adjust_condition):
    """调仓日计数器，单位：日"""

    def __init__(self, params):
        self.period = params.get('period', 3)
        self.day_count = 0
        self.t_can_adjust = False

    def update_params(self, context, params):
        self.period = params.get('period', self.period)

    @property
    def can_adjust(self):
        return self.t_can_adjust

    def handle_data(self, context, data):
        self.log_info("调仓日计数 [%d]" % (self.day_count))
        stock_count = len(context.portfolio.positions)
        self.t_can_adjust = stock_count == 0 or self.day_count % self.period == 0
        if stock_count > 0: 
            self.day_count += 1    
        pass

    def before_trading_start(self, context):
        self.t_can_adjust = False

        pass

    def when_sell_stock(self, position, order, is_normal):
        if not is_normal:
            # 个股止损止盈时，即非正常卖股时，重置计数，原策略是这么写的
            self.day_count = 0
        pass

    # 清仓时调用的函数
    def when_clear_position(self, context):
        self.day_count = 0
        pass

    def __str__(self):
        return '调仓日计数器: [调仓频率: %d日] [调仓日计数 %d]' % (
            self.period, self.day_count)


class Index28_condition(Adjust_condition):
    """28指数涨幅调仓判断器"""
    # TODO 可以做性能优化,每天只需计算一次,不需要每分钟调用

    def __init__(self, params):
        self.index2 = params.get('index2', '')
        self.index8 = params.get('index8', '')
        self.index_growth_rate = params.get('index_growth_rate', 0.01)
        self.t_can_adjust = False

    def update_params(self, context, params):
        self.index2 = params.get('index2', self.index2)
        self.index8 = params.get('index8', self.index8)
        self.index_growth_rate = params.get('index_growth_rate', self.index_growth_rate)

    @property
    def can_adjust(self):
        return self.t_can_adjust

    def handle_data(self, context, data):
        # 回看指数前20天的涨幅
        gr_index2 = get_growth_rate(self.index2)
        gr_index8 = get_growth_rate(self.index8)
        self.log_info("当前%s指数的20日涨幅 [%.2f%%]" % (get_security_info(self.index2).display_name, gr_index2 * 100))
        self.log_info("当前%s指数的20日涨幅 [%.2f%%]" % (get_security_info(self.index8).display_name, gr_index8 * 100))
        if gr_index2 <= self.index_growth_rate and gr_index8 <= self.index_growth_rate:
            msg = "==> 当日%s指数和%s指数的20日增幅低于[%.2f%%]，执行28指数止损" \
                  % (
                      get_security_info(self.index2).display_name,
                      get_security_info(self.index8).display_name,
                      self.index_growth_rate * 100)
            self.log_weixin(context, msg)
            self.clear_position(context)
            self.t_can_adjust = False
        else:
            self.t_can_adjust = True
        pass

    def before_trading_start(self, context):
        pass

    def __str__(self):
        return '28指数择时: [大盘指数:%s %s] [小盘指数:%s %s] [判定调仓的二八指数20日增幅 %.2f%%]' % (
            self.index2, get_security_info(self.index2).display_name,
            self.index8, get_security_info(self.index8).display_name,
            self.index_growth_rate * 100)


# ===================== 各种止损实现 ================================================


class Stop_loss_stocks(Rule):
    """个股止损器"""

    def __init__(self, params):
        self.last_high = {}
        self.period = params.get('period', 3)  # 单位为天
        self.pct_change = {}

    def update_params(self, context, params):
        self.period = params.get('period', self.period)

    # 个股止损
    def handle_data(self, context, data):
        for stock in context.portfolio.positions.keys():
            cur_price = data[stock].close
            xi = attribute_history(stock, 2, '1d', 'high', skip_paused=True)
            ma = xi.max()
            if not self.last_high.has_key(stock) or self.last_high[stock] < cur_price:
                self.last_high[stock] = cur_price

            threshold = self.__get_stop_loss_threshold(stock, self.period)
            # log.debug("个股止损阈值, stock: %s, threshold: %f" %(stock, threshold))
            if cur_price < self.last_high[stock] * (1 - threshold):
                msg = "==> 个股止损, stock: %s, cur_price: %f, last_high: %f, threshold: %f" \
                              % (stock, cur_price, self.last_high[stock], threshold)
                self.log_weixin(context, msg)
                position = context.portfolio.positions[stock]
                self.close_position(position, False)

    # 获取个股前n天的m日增幅值序列
    # 增加缓存避免当日多次获取数据
    def __get_pct_change(self, security, n, m):
        pct_change = None
        if security in self.pct_change.keys():
            pct_change = self.pct_change[security]
        else:
            h = attribute_history(security, n, unit='1d', fields=('close'), skip_paused=True)
            pct_change = h['close'].pct_change(m)  # 3日的百分比变比（即3日涨跌幅）
            self.pct_change[security] = pct_change
        return pct_change

    # 计算个股回撤止损阈值
    # 即个股在持仓n天内能承受的最大跌幅
    # 算法：(个股250天内最大的n日跌幅 + 个股250天内平均的n日跌幅)/2
    # 返回正值
    def __get_stop_loss_threshold(self, security, n=3):
        pct_change = self.__get_pct_change(security, 250, n)
        # log.debug("pct of security [%s]: %s", pct)
        maxd = pct_change.min()
        # maxd = pct[pct<0].min()
        avgd = pct_change.mean()
        # avgd = pct[pct<0].mean()
        # maxd和avgd可能为正，表示这段时间内一直在增长，比如新股
        bstd = (maxd + avgd) / 2

        # 数据不足时，计算的bstd为nan
        if not isnan(bstd):
            if bstd != 0:
                return abs(bstd)
            else:
                # bstd = 0，则 maxd <= 0
                if maxd < 0:
                    # 此时取最大跌幅
                    return abs(maxd)

        return 0.099  # 默认配置回测止损阈值最大跌幅为-9.9%，阈值高貌似回撤降低

    def when_sell_stock(self, position, order, is_normal):
        if position.security in self.last_high:
            self.last_high.pop(position.security)
        pass

    def when_buy_stock(self, stock, order):
        if order.status == OrderStatus.held and order.filled == order.amount:
            # 全部成交则删除相关证券的最高价缓存
            self.last_high[stock] = get_close_price(stock, 1, '1m')
        pass

    def after_trading_end(self, context):
        self.pct_change = {}
        pass

    def __str__(self):
        return '个股止损器:[当前缓存价格数: %d ]' % (len(self.last_high))


class Stop_loss_stocks_inner_day(Rule):
    """日内个股止损器,日内价格低于当日最高值达到阈值则平仓止损"""

    def __init__(self, params):
        self.threshold = params.get('threshold', 0.05)
        self.last_high = {}
        self.on_close_position = close_position  # 卖股回调函数

    def update_params(self, context, params):
        self.threshold = params.get('threshold', self.threshold)

    def before_trading_start(self, context):
        for stock in context.portfolio.positions.keys():
            self.last_high[stock] = 0.0

    # 个股止损
    def handle_data(self, context, data):
        for stock in context.portfolio.positions.keys():
            cur_price = data[stock].close
            if not self.last_high.has_key(stock) or self.last_high[stock] < cur_price:
                self.last_high[stock] = cur_price

            if cur_price < self.last_high[stock] * (1 - self.threshold):
                msg = "==> stock: %s, cur_price: %f, last_high: %f, value: %f" \
                              % (stock, cur_price, self.last_high[stock], context.portfolio.positions[stock].value)
                self.log_weixin(context, msg)
                close_position(self, context.portfolio.positions[stock])

    def __str__(self):
        return '日内个股止损: [阈值: %f]' % (self.threshold)


class Stop_profit_stocks(Rule):
    """个股止盈器"""

    def __init__(self, params):
        self.last_high = {}
        self.period = params.get('period', 3)
        self.pct_change = {}

    def update_params(self, context, params):
        self.period = params.get('period', self.period)

    def handle_data(self, context, data):
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            cur_price = data[stock].close
            threshold = self.__get_stop_profit_threshold(stock, self.period)
            # log.debug("个股止盈阈值, stock: %s, threshold: %f" %(stock, threshold))
            if cur_price > position.avg_cost * (1 + threshold):
                msg = "==> 个股止盈, stock: %s, cur_price: %f, avg_cost: %f, threshold: %f" \
                              % (stock, cur_price, self.last_high[stock], threshold)
                self.log_weixin(context, msg)
                position = context.portfolio.positions[stock]
                self.close_position(position, False)

    # 获取个股前n天的m日增幅值序列
    # 增加缓存避免当日多次获取数据
    def __get_pct_change(self, security, n, m):
        pct_change = None
        if security in self.pct_change.keys():
            pct_change = self.pct_change[security]
        else:
            h = attribute_history(security, n, unit='1d', fields=('close'), skip_paused=True)
            pct_change = h['close'].pct_change(m)  # 3日的百分比变比（即3日涨跌幅）
            self.pct_change[security] = pct_change
        return pct_change

    # 计算个股止盈阈值
    # 算法：个股250天内最大的n日涨幅
    # 返回正值
    def __get_stop_profit_threshold(self, security, n=3):
        pct_change = self.__get_pct_change(security, 250, n)
        maxr = pct_change.max()

        # 数据不足时，计算的maxr为nan
        # 理论上maxr可能为负
        if (not isnan(maxr)) and maxr != 0:
            return abs(maxr)
        return 0.30  # 默认配置止盈阈值最大涨幅为30%

    def when_sell_stock(self, position, order, is_normal):
        if order.status == OrderStatus.held and order.filled == order.amount:
            # 全部成交则删除相关证券的最高价缓存
            if position.security in self.last_high:
                self.last_high.pop(position.security)
        pass

    def when_buy_stock(self, stock, order):
        self.last_high[stock] = get_close_price(stock, 1, '1m')
        pass

    def after_trading_end(self, context):
        self.pct_change = {}
        pass

    def __str__(self):
        return '个股止盈器:[当前缓存价格数: %d ]' % (len(self.last_high))


class Stop_loss_by_price(Adjust_condition):
    """最高价最低价比例止损"""

    def __init__(self, params):
        self.index = params.get('index', '000001.XSHG')
        self.day_count = params.get('day_count', 160)
        self.multiple = params.get('multiple', 2.2)
        self.is_day_stop_loss_by_price = False

    def update_params(self, context, params):
        self.index = params.get('index', self.index)
        self.day_count = params.get('day_count', self.day_count)
        self.multiple = params.get('multiple', self.multiple)

    def handle_data(self, context, data):
        # 大盘指数前130日内最高价超过最低价2倍，则清仓止损
        # 基于历史数据判定，因此若状态满足，则当天都不会变化
        # 增加此止损，回撤降低，收益降低

        if not self.is_day_stop_loss_by_price:
            h = attribute_history(self.index, self.day_count, unit='1d', fields=('close', 'high', 'low'),
                                  skip_paused=True)
            low_price_130 = h.low.min()
            high_price_130 = h.high.max()
            if high_price_130 > self.multiple * low_price_130 and h['close'][-1] < h['close'][-4] * 1 and h['close'][
                -1] > h['close'][-100]:
                # 当日第一次输出日志
                msg = "==> 大盘止损，%s指数前130日内最高价超过最低价2倍, 最高价: %f, 最低价: %f" % (
                    get_security_info(self.index).display_name, high_price_130, low_price_130)
                self.log_weixin(context, msg)
                self.is_day_stop_loss_by_price = True

        if self.is_day_stop_loss_by_price:
            self.clear_position(context)

    def before_trading_start(self, context):
        self.is_day_stop_loss_by_price = False
        pass

    def __str__(self):
        return '大盘高低价比例止损器:[指数: %s] [参数: %s日内最高最低价: %s倍] [当前状态: %s]' % (
            self.index, self.day_count, self.multiple, self.is_day_stop_loss_by_price)

    @property
    def can_adjust(self):
        return not self.is_day_stop_loss_by_price


class Stop_loss_by_3_black_crows(Adjust_condition):
    """三乌鸦止损"""

    def __init__(self, params):
        self.index = params.get('index', '000001.XSHG')
        self.dst_drop_minute_count = params.get('dst_drop_minute_count', 60)
        # 临时参数
        self.is_last_day_3_black_crows = False
        self.t_can_adjust = True
        self.cur_drop_minute_count = 0

    def update_params(self, context, params):
        self.index = params.get('index', self.index)
        self.dst_drop_minute_count = params.get('dst_drop_minute_count', self.dst_drop_minute_count)

    def initialize(self, context):
        pass

    def handle_data(self, context, data):
        # 前日三黑鸦，累计当日每分钟涨幅<0的分钟计数
        # 如果分钟计数超过一定值，则开始进行三黑鸦止损
        # 避免无效三黑鸦乱止损
        if self.is_last_day_3_black_crows:
            if get_growth_rate(self.index, 1) < 0:
                self.cur_drop_minute_count += 1

            if self.cur_drop_minute_count >= self.dst_drop_minute_count:
                if self.cur_drop_minute_count == self.dst_drop_minute_count:
                    msg = "==> 超过三黑鸦止损开始"
                    self.log_weixin(context, msg)

                self.clear_position(context)
                self.t_can_adjust = False
        else:
            self.t_can_adjust = True
        pass

    def before_trading_start(self, context):

        def is_3_black_crows(stock):
            # talib.CDL3BLACKCROWS

            # 三只乌鸦说明来自百度百科
            # 1. 连续出现三根阴线，每天的收盘价均低于上一日的收盘
            # 2. 三根阴线前一天的市场趋势应该为上涨
            # 3. 三根阴线必须为长的黑色实体，且长度应该大致相等
            # 4. 收盘价接近每日的最低价位
            # 5. 每日的开盘价都在上根K线的实体部分之内；
            # 6. 第一根阴线的实体部分，最好低于上日的最高价位
            #
            # 算法
            # 有效三只乌鸦描述众说纷纭，这里放宽条件，只考虑1和2
            # 根据前4日数据判断
            # 3根阴线跌幅超过4.5%（此条件忽略）

            h = attribute_history(stock, 4, '1d', ('close', 'open'), skip_paused=True, df=False)
            h_close = list(h['close'])
            h_open = list(h['open'])

            if len(h_close) < 4 or len(h_open) < 4:
                return False

            # 一阳三阴
            if h_close[-4] > h_open[-4] \
                    and (h_close[-1] < h_open[-1] and h_close[-2] < h_open[-2] and h_close[-3] < h_open[-3]):
                # and (h_close[-1] < h_close[-2] and h_close[-2] < h_close[-3]) \
                # and h_close[-1] / h_close[-4] - 1 < -0.045:
                return True
            return False

        self.is_last_day_3_black_crows = is_3_black_crows(self.index)
        if self.is_last_day_3_black_crows:
            self.log_info("==> 前4日已经构成三黑鸦形态")
        pass

    def after_trading_end(self, context):
        self.is_last_day_3_black_crows = False
        self.cur_drop_minute_count = 0
        pass

    def __str__(self):
        return '大盘三乌鸦止损器:[指数: %s] [跌计数分钟: %d] [当前状态: %s]' % (
            self.index, self.dst_drop_minute_count, self.is_last_day_3_black_crows)

    @property
    def can_adjust(self):
        return self.t_can_adjust


class Stop_loss_by_28_index(Adjust_condition):
    """28指数值实时止损"""

    def __init__(self, params):
        self.index2 = params.get('index2', '')
        self.index8 = params.get('index8', '')
        self.index_growth_rate = params.get('index_growth_rate', 0.01)
        self.dst_minute_count_28index_drop = params.get('dst_minute_count_28index_drop', 120)
        # 临时参数
        self.t_can_adjust = True
        self.minute_count_28index_drop = 0

    def update_params(self, context, params):
        self.index2 = params.get('index2', self.index2)
        self.index8 = params.get('index8', self.index8)
        self.index_growth_rate = params.get('index_growth_rate', self.index_growth_rate)
        self.dst_minute_count_28index_drop = params.get('dst_minute_count_28index_drop',
                                                        self.dst_minute_count_28index_drop)

    def initialize(self, context):
        pass

    def handle_data(self, context, data):
        # 回看指数前20天的涨幅
        gr_index2 = get_growth_rate(self.index2)
        gr_index8 = get_growth_rate(self.index8)

        if gr_index2 <= self.index_growth_rate and gr_index8 <= self.index_growth_rate:
            if (self.minute_count_28index_drop == 0):
                self.log_info("当前二八指数的20日涨幅同时低于[%.2f%%], %s指数: [%.2f%%], %s指数: [%.2f%%]" \
                              % (self.index_growth_rate * 100,
                                 get_security_info(self.index2).display_name,
                                 gr_index2 * 100,
                                 get_security_info(self.index8).display_name,
                                 gr_index8 * 100))

            self.minute_count_28index_drop += 1
        else:
            # 不连续状态归零
            if self.minute_count_28index_drop < self.dst_minute_count_28index_drop:
                self.minute_count_28index_drop = 0

        if self.minute_count_28index_drop >= self.dst_minute_count_28index_drop:
            if self.minute_count_28index_drop == self.dst_minute_count_28index_drop:
                msg = "==> 当日%s指数和%s指数的20日增幅低于[%.2f%%]已超过%d分钟，执行28指数止损" \
                              % (
                                  get_security_info(self.index2).display_name,
                                  get_security_info(self.index8).display_name,
                                  self.index_growth_rate * 100, self.dst_minute_count_28index_drop)
                self.log_weixin(context, msg)

            self.clear_position(context)
            self.t_can_adjust = False
        else:
            self.t_can_adjust = True
        pass

    def after_trading_end(self, context):
        self.t_can_adjust = False
        self.minute_count_28index_drop = 0
        pass

    def __str__(self):
        return '28指数值实时进行止损:[大盘指数: %s %s] [小盘指数: %s %s] [判定调仓的二八指数20日增幅 %.2f%%] [连续 %d 分钟则清仓] ' % (
            self.index2, get_security_info(self.index2).display_name,
            self.index8, get_security_info(self.index8).display_name,
            self.index_growth_rate * 100,
            self.dst_minute_count_28index_drop)

    @property
    def can_adjust(self):
        return self.t_can_adjust
        
# ===================== 调仓操作实现 ================================================


class Sell_stocks(Adjust_position):
    """股票调仓卖出"""

    def __init__(self, params):
        pass

    def update_params(self, context, params):
        pass
    
    def adjust(self, context, data, buy_stocks):
        if not context.flags_can_sell:
            self.log_warn('无法执行卖出!! context.flags_can_sell 未开启')
            return
        # 卖出不在待买股票列表中的股票
        # 对于因停牌等原因没有卖出的股票则继续持有
        for stock in context.portfolio.positions.keys():
            if stock not in buy_stocks:
                self.log_info("stock [%s] going to close position" % (stock))
                position = context.portfolio.positions[stock]
                self.close_position(position)
            else:
                self.log_debug("stock [%s] is already in position" % (stock))

    def __str__(self):
        return '股票调仓卖出规则：卖出不在buy_stocks的股票'


class Buy_stocks(Adjust_position):
    """股票调仓买入"""

    def __init__(self, params):
        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def adjust(self, context, data, buy_stocks):
        if not context.flags_can_buy:
            self.log_warn('无法执行买入!! context.flags_can_buy 未开启')
            return
        # 买入股票
        # 始终保持持仓数目为g.buy_stock_count
        # 根据股票数量分仓
        # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
        position_count = len(context.portfolio.positions)
        if self.buy_count > position_count:
            quota = 1.0 / (self.buy_count - position_count)
            value = context.portfolio.cash * quota
            for stock in buy_stocks:
                if context.portfolio.positions[stock].total_amount == 0:
                    if self.open_position(stock, value):
                        if len(context.portfolio.positions) == self.buy_count:
                            break
        pass

    def __str__(self):
        return '股票调仓买入规则：现金平分式买入股票达目标股票数'


class Buy_stocks_portion(Adjust_position):
    """按比重股票调仓买入"""

    def __init__(self, params):
        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def adjust(self, context, data, buy_stocks):
        if not context.flags_can_buy:
            self.log_warn('无法执行买入!! context.flags_can_buy 未开启')
            return
        # 买入股票
        # 始终保持持仓数目为g.buy_stock_count
        # 根据股票数量分仓
        # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
        position_count = len(context.portfolio.positions)
        if self.buy_count > position_count:
            buy_num = self.buy_count - position_count
            portion_gen = generate_portion(buy_num)
            available_cash = context.portfolio.cash
            for stock in buy_stocks:
                if context.portfolio.positions[stock].total_amount == 0:
                    try:
                        buy_portion = portion_gen.next()
                        value = available_cash * buy_portion
                        if self.open_position(stock, value):
                            if len(context.portfolio.positions) == self.buy_count:
                                break
                    except StopIteration:
                        break
        pass

    def __str__(self):
        return '股票调仓买入规则：现金比重式买入股票达目标股票数: [ %d ]' % self.buy_count


class Buy_stocks_var(Adjust_position):
    """使用 VaR 方法做调仓控制"""

    def __init__(self, params):
        self.buy_count = params.get('buy_count', 3)
        self.lowPEG_risk_ratio = params.get('lowPEG_risk_ratio', 0.03 / self.buy_count)
        self.lowPEG_ratio = params.get('lowPEG_ratio', 1.0)
        self.confidencelevel = params.get('confidencelevel', 1.96)

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)
        self.lowPEG_risk_ratio = params.get('lowPEG_risk_ratio', self.lowPEG_risk_ratio)
        self.lowPEG_ratio = params.get('lowPEG_ratio', self.lowPEG_ratio)
        self.confidencelevel = params.get('confidencelevel', self.confidencelevel)

    def adjust(self, context, data, buy_stocks):
        if not context.flags_can_buy:
            self.log_warn('无法执行买入!! context.flags_can_buy 未开启')
            return
        
        equity_ratio, bonds_ratio = assetAllocationSystem(context, buy_stocks)
        trade_ratio = fun_calPosition(context, equity_ratio, bonds_ratio, self.lowPEG_ratio, self.lowPEG_risk_ratio, self.confidencelevel)
        stock_list = list(get_all_securities(['stock']).index)
        for stock in context.portfolio.positions.keys():
            if stock not in trade_ratio and stock in stock_list:
                trade_ratio[stock] = 0

        context.trade_ratio = trade_ratio

    def __str__(self):
        return '股票调仓买入规则：使用 VaR 方式买入(小兵哥)'

# ==================== 选股query过滤器实现 ===========================================

class Pick_small_cap(Filter_query):
    """小市值选股器"""

    def filter(self, context, data, q):
        return query(valuation).order_by(valuation.market_cap.asc())

    def __str__(self):
        return '按市值倒序选取股票'


class Pick_by_market_cap(Filter_query):
    """限定区间小市值选股器"""

    def __init__(self, params):
        self.mcap_min = params.get('mcap_min', 0)
        self.mcap_max = params.get('mcap_max', 300)

    def filter(self, context, data, q):
        return query(valuation).filter(valuation.market_cap >= self.mcap_min,
                                       valuation.market_cap <= self.mcap_max).order_by(valuation.market_cap.asc())

    def __str__(self):
        return '按市值区间倒序选取股票: [ %d <= market_cap <= %d ]' % (self.mcap_min, self.mcap_max)


class Filter_pe(Filter_query):
    """PE范围选股器"""

    def __init__(self, params):
        self.pe_min = params.get('pe_min', 0)
        self.pe_max = params.get('pe_max', 200)

    def update_params(self, context, params):
        self.pe_min = params.get('pe_min', self.pe_min)
        self.pe_max = params.get('pe_max', self.pe_max)

    def filter(self, context, data, q):
        return q.filter(
            valuation.pe_ratio > self.pe_min,
            valuation.pe_ratio < self.pe_max
        )

    def __str__(self):
        return '根据PE范围选取股票: [ %d < pe < %d]' % (self.pe_min, self.pe_max)


class Filter_eps(Filter_query):
    """EPS范围选股器"""

    def __init__(self, params):
        self.eps_min = params.get('eps_min', 0)

    def update_params(self, context, params):
        self.eps_min = params.get('eps_min', self.eps_min)

    def filter(self, context, data, q):
        return q.filter(
            indicator.eps > self.eps_min,
        )

    def __str__(self):
        return '根据EPS范围选取股票: [ %f < eps ]' % (self.eps_min)
        

class Filter_limite(Filter_query):
    """选股计数器"""

    def __init__(self, params):
        self.pick_stock_count = params.get('pick_stock_count', 0)

    def update_params(self, context, params):
        self.pick_stock_count = params.get('pick_stock_count', self.pick_stock_count)

    def filter(self, context, data, q):
        return q.limit(self.pick_stock_count)

    def __str__(self):
        return '初选股票数量: [ %d ]' % (self.pick_stock_count)

# ===================== 选股过滤器实现,对 stock_list 做过滤 ==============================


class Filter_gem(Filter_stock_list):
    """过滤创业板股票"""

    def filter(self, context, data, stock_list):
        return [stock for stock in stock_list if stock[0:3] != '300']

    def __str__(self):
        return '过滤创业板股票'


class Filter_sz(Filter_stock_list):
    def filter(self, context, data, stock_list):
        return [stock for stock in stock_list if stock[0:1] != '0']

    def __str__(self):
        return '过滤深证股票'


class Filter_sh(Filter_stock_list):
    def filter(self, context, data, stock_list):
        return [stock for stock in stock_list if stock[0:1] != '6']

    def __str__(self):
        return '过滤上证股票'


class Filter_paused_stock(Filter_stock_list):
    """过滤停牌股票"""

    def filter(self, context, data, stock_list):
        current_data = get_current_data()
        return [stock for stock in stock_list if not current_data[stock].paused]

    def __str__(self):
        return '过滤停牌股票'


class Filter_limitup(Filter_stock_list):
    """过滤涨停股票"""

    def filter(self, context, data, stock_list):
        threshold = 1.00
        return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
                or data[stock].close < data[stock].high_limit * threshold]

    def __str__(self):
        return '过滤涨停股票'


class Filter_limitdown(Filter_stock_list):
    """过滤跌停股票"""

    def filter(self, context, data, stock_list):
        threshold = 1.00
        return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
                or data[stock].close > data[stock].low_limit * threshold]

    def __str__(self):
        return '过滤跌停股票'


class Filter_st(Filter_stock_list):
    """过滤ST股票"""

    def filter(self, context, data, stock_list):
        current_data = get_current_data()
        return [stock for stock in stock_list
                if not current_data[stock].is_st
                and not current_data[stock].name.startswith('退')]

    def __str__(self):
        return '过滤ST股票'


class Filter_growth_is_down(Filter_stock_list):
    """过滤增长率为负的股票"""

    def __init__(self, params):
        self.day_count = params.get('day_count', 20)

    def update_params(self, context, params):
        self.day_count = params.get('day_count', self.day_count)

    def filter(self, context, data, stock_list):
        return [stock for stock in stock_list if get_growth_rate(stock, self.day_count) > 0]

    def __str__(self):
        return '过滤n日增长率为负的股票'


class Filter_blacklist(Filter_stock_list):
    """过滤黑名单股票"""

    def __get_blacklist(self):
        # 黑名单一览表，更新时间 2016.7.10 by 沙米
        # 科恒股份、太空板业，一旦2016年继续亏损，直接面临暂停上市风险
        blacklist = ["600656.XSHG", "300372.XSHE", "600403.XSHG", "600421.XSHG", "600733.XSHG", "300399.XSHE",
                     "600145.XSHG", "002679.XSHE", "000020.XSHE", "002330.XSHE", "300117.XSHE", "300135.XSHE",
                     "002566.XSHE", "002119.XSHE", "300208.XSHE", "002237.XSHE", "002608.XSHE", "000691.XSHE",
                     "002694.XSHE", "002715.XSHE", "002211.XSHE", "000788.XSHE", "300380.XSHE", "300028.XSHE",
                     "000668.XSHE", "300033.XSHE", "300126.XSHE", "300340.XSHE", "300344.XSHE", "002473.XSHE"]
        return blacklist

    def process_initialize(self, context):
        # 抓取黑名单代码  抓取的保存在self.blacklist就行
        # 此处每天9点20执行
        pass

    def before_adjust_start(self, context, data):
        # 抓取黑名单代码  抓取的保存在self.blacklist就行
        # 此处调仓前执行
        pass

    def filter(self, context, data, stock_list):
        blacklist = self.__get_blacklist()
        return [stock for stock in stock_list if stock not in blacklist]

    def __str__(self):
        return '过滤黑名单股票'


class Filter_cash_flow_rank(Filter_stock_list):
    """庄股评分排序"""

    def __init__(self, params):
        self.rank_stock_count = params.get('rank_stock_count', 600)

    def update_params(self, context, params):
        self.rank_stock_count = params.get('self.rank_stock_count', self.rank_stock_count)

    def __str__(self):
        return '庄股评分排序, 评分股数: [ %d ]' % self.rank_stock_count

    def filter(self, context, data, stock_list):
        df = cow_stock_value(stock_list[:self.rank_stock_count])
        return df.index

# class Filter_chip_density(Filter_stock_list):
#     def __init__(self,params):
#         self.rank_stock_count = params.get('rank_stock_chip_density',200)
#     def update_params(self,context,params):
#         self.rank_stock_count = params.get('self.rank_stock_count',self.rank_stock_count)
#     def __str__(self):
#         return '筹码分布评分排序 [评分股数] %d' % self.rank_stock_count
#     def filter(self, context, data, stock_list):
#         density_dict = self.__chip_migration(context, data, stock_list[:self.rank_stock_count])
#         items = list(density_dict.items())
#         return [l[0] for l in sorted(items, key=lambda x : x[1], reverse=True)]

#     def __chip_migration(self, context, data, stock_list):
#         density_dict = {}
#         for stock in stock_list:
#             # print "working on stock %s" % stock
#             df = attribute_history(stock, count = 120, unit='1d', fields=('avg', 'volume'), skip_paused=True)
#             df_dates = df.index
#             for da in df_dates:
#                 df_fund = get_fundamentals(query(
#                         valuation.turnover_ratio
#                     ).filter(
#                         # 这里不能使用 in 操作, 要使用in_()函数
#                         valuation.code.in_([stock])
#                     ), date=da)
#                 if not df_fund.empty:
#                     df.loc[da, 'turnover_ratio'] = df_fund['turnover_ratio'][0]
#             df = df.dropna()
#             df = chip_migration(df)
#             concentration_number, latest_concentration_rate= self.__analyze_chip_density(df)
#             density_dict[stock] = concentration_number * latest_concentration_rate
#         return density_dict

#     def __analyze_chip_density(self, df):
#         df = df.dropna()
#         df = df.drop_duplicates(cols='chip_density')
#         bottomIndex = argrelextrema(df.chip_density.values, np.less_equal,order=3)[0]
#         concentration_num = len(bottomIndex)
#         latest_concentration_rate = df.chip_density.values[-1] / df.chip_density[bottomIndex[-1]]
#         # print df.chip_density.values
#         # print bottomIndex
#         # print concentration_num, latest_concentration_rate
#         return concentration_num, latest_concentration_rate


class Filter_rank(Filter_stock_list):
    """股票评分排序"""

    def __init__(self, params):
        self.rank_stock_count = params.get('rank_stock_count', 20)

    def update_params(self, context, params):
        self.rank_stock_count = params.get('rank_stock_count', self.rank_stock_count)

    def filter(self, context, data, stock_list):
        if len(stock_list) > self.rank_stock_count:
            stock_list = stock_list[:self.rank_stock_count]

        dst_stocks = {}
        for stock in stock_list:
            h = attribute_history(stock, 130, unit='1d', fields=('close', 'high', 'low'), skip_paused=True)
            low_price_130 = h.low.min()
            high_price_130 = h.high.max()

            avg_15 = data[stock].mavg(15, field='close')
            cur_price = data[stock].close

            score = (cur_price - low_price_130) + (cur_price - high_price_130) + (cur_price - avg_15)
            dst_stocks[stock] = score

        # log.error('stock_list: %s, dst_stocks: %s' % (stock_list, dst_stocks))
        if len(dst_stocks) == 0:
            return list()
        df = pd.DataFrame(dst_stocks.values(), index=dst_stocks.keys())
        df.columns = ['score']
        df = df.sort(columns='score', ascending=True)
        return list(df.index)

    def __str__(self):
        return '股票评分排序 [评分股数: %d ]' % (self.rank_stock_count)

class Filter_buy_count(Filter_stock_list):
    """最终待买股票计数"""

    def __init__(self, params):
        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def filter(self, context, data, stock_list):
        if len(stock_list) > self.buy_count:
            return stock_list[:self.buy_count]
        else:
            return stock_list

    def __str__(self):
        return '获取最终待购买股票数: [ %d ]' % (self.buy_count)


class Pick_lead_stock(Filter_stock_list):
    """选取指数的龙头股。该规则独立输出,会覆盖前面一级的输入。"""

    def __init__(self, params):
        self.index = params.get('index', '000001.XSHG')  # 指数股
        self.pastDay = params.get('pastDay', 90)  # 初始基准日期
        self.topK = params.get('topK', 3)  # 只操作topK只股票
        self.selectStockMethod = params.get('selectStockMethod', 3)  # 选股方法参数(可选：0,1,2,3)

    # 根据涨幅筛选股票
    def filtGain(self, stocks, pastDay):
        # 初始化参数信息
        numStocks = len(stocks)
        rankValue = []

        # 计算涨跌幅
        for security in stocks:
            # 获取过去pastDay的指数值
            stocksPrice = history(pastDay, '1d', 'close', [security])
            if len(stocksPrice) != 0:
                # 计算涨跌幅
                errCloseOpen = [
                    (float(stocksPrice.iloc[-1]) - float(stocksPrice.iloc[0])) / float(stocksPrice.iloc[0])]
                rankValue += errCloseOpen
            else:
                rankValue += [0]

        # 根据周涨跌幅排名
        filtStocks = {'code': stocks, 'rankValue': rankValue}
        filtStocks = pd.DataFrame(filtStocks)
        filtStocks = filtStocks.sort('rankValue', ascending=False)
        # 根据涨跌幅筛选
        filtStocks = filtStocks.head(self.topK)
        filtStocks = list(filtStocks['code'])

        return filtStocks

    # 根据成交量筛选股票
    def filtVol(self, stocks):
        # 初始化返回数组
        returnStocks = []

        # 筛选
        for security in stocks:
            stockVol = history(60, '1d', 'volume', [security])
            if float(stockVol.iloc[-5:].mean()) > float(stockVol.iloc[-30:].mean()):
                returnStocks += [security]
            else:
                continue
        return returnStocks

    # 根据流通市值筛选股票
    def filtMarketCap(self, context, stocks, index):
        # 初始化返回数组
        returnStocks = []

        # 计算指数的总流通市值
        oriStocks = get_index_stocks(index)
        indexMarketCap = get_fundamentals(
            query(valuation.circulating_market_cap).filter(valuation.code.in_(oriStocks)),
            date=context.current_dt)
        totalMarketCap = float(sum(indexMarketCap['circulating_market_cap']))

        # 计算个股流通市值占总市值百分比阈值：以四分位为阈值
        indexMarketCap = indexMarketCap.div(totalMarketCap, axis=0)
        porThre = indexMarketCap.describe()
        porThre = float(porThre.loc['25%'])

        # 筛选
        for security in stocks:
            stockMarketCap = get_fundamentals(
                query(valuation.circulating_market_cap).filter(valuation.code.in_([security])),
                date=context.current_dt)
            if float(stockMarketCap.iloc[0]) > totalMarketCap * porThre:
                returnStocks += [security]
            else:
                continue
        return returnStocks

    def filter(self, context, data, stock_list):
        # 规则
        # 1.涨幅大于阈值的topK只股票；
        # 3.过去一周成交量大于过去两周成交量；
        # 4.个股流通市值占总市值百分比达到阈值
        # 取出该指数的股票:
        oriStocks = get_index_stocks(self.index)
        # 根据个股涨幅筛选
        filtStocks = self.filtGain(oriStocks, self.pastDay)

        # 根据规则筛选绩优股
        if self.selectStockMethod == 0:
            # 基本限制
            pass
        elif self.selectStockMethod == 1:
            # 基本限制+成交量限制
            filtStocks = self.filtVol(filtStocks)
        elif self.selectStockMethod == 2:
            # 基本限制+流通市值限制
            filtStocks = self.filtMarketCap(context, filtStocks, self.index)
        elif self.selectStockMethod == 3:
            # 基本限制+流通市值限制+成交量限制
            filtStocks = self.filtVol(filtStocks)
            if len(filtStocks) != 0:
                filtStocks = self.filtMarketCap(context, filtStocks, self.index)
            else:
                pass
        else:
            log.error('[选取指数的龙头股] 错误的选股方式: %d' % self.selectStockMethod)

        return filtStocks

    def __str__(self):
        return '选取指数的龙头股: [ index: %s ]' % self.index


class Filter_cycle_industry(Filter_stock_list):
    """过滤周期性行业股票"""

    def __init__(self, params):
        pass

    def update_params(self, context, params):
        pass

    def filter(self, context, data, stock_list):
        return fun_remove_cycle_industry(context, stock_list)

    def __str__(self):
        return '过滤周期性行业股票'

class Filter_fcff(Filter_stock_list):
    """过滤周期性行业股票"""

    def __init__(self, params):
        pass

    def update_params(self, context, params):
        pass

    def filter(self, context, data, stock_list):
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
        stock_list = list(df['code'])
        return stock_list

    def __str__(self):
        return '过滤现金流差的股票'

class Filter_new_share(Filter_stock_list):
    """剔除上市时间较短的产品"""

    def __init__(self, params):
        self.deltaday = params.get('deltaday', 60)

    def update_params(self, context, params):
        self.deltaday = params.get('deltaday', self.deltaday)

    def filter(self, context, data, stock_list):
        deltaDate = context.current_dt.date() - dt.timedelta(self.deltaday)

        tmpList = []
        for stock in stock_list:
            if get_security_info(stock).start_date < deltaDate:
                tmpList.append(stock)

        return tmpList

    def __str__(self):
        return '剔除上市时间较短的产品'

class Filter_low_peg(Filter_stock_list):
    """PEG选择器"""
    def __init__(self, params):
        self.peg_max = params.get('peg_max', 0.75)
        self.peg_min = params.get('peg_min', 0)

    def update_params(self, context, params):
        self.peg_max = params.get('peg_max', self.peg_max)
        self.peg_min = params.get('peg_min', self.peg_min)

    def __str__(self):
        return 'PEG选择器: [ %f < peg < %f ]' % (self.peg_min, self.peg_max)

    # 取得净利润增长率参数
    def get_inc(self, context, stock_list):
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

    # 计算股票的peg
    def calc_stock_peg(self, context, stock_list, stock_dict):
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
        #df = fun_get_Divid_by_year(context, stock_list)
        # 聚源版本
        statsDate = context.current_dt.date() - dt.timedelta(1)
        df = fun_get_Dividend_yield(stock_list, statsDate)
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

    def filter(self, context, data, stock_list):
        # log.info("peg stock_list", stock_list)
        
        # 保存不要清仓的股票
        old_stock_list = []
        for stock in context.portfolio.positions.keys():
            if stock in stock_list:
                old_stock_list.append(stock)

        stock_dict = self.get_inc(context, stock_list)
        peg_dict = self.calc_stock_peg(context, stock_list, stock_dict)
        stock_list = []
        for stock in peg_dict:
            if peg_dict[stock] < self.peg_max and peg_dict[stock] > self.peg_min:
                stock_list.append(stock)

        stock_list = self.sort_stock_list(context, stock_list)

        # 对老的排序
        tmpDict = {}
        for stock in old_stock_list:
            if peg_dict[stock] < 1 and peg_dict[stock] > 0:
                tmpDict[stock] = peg_dict[stock]
        tmpDict = sorted(tmpDict.items(), key=lambda d:d[1], reverse=False)

        for idx in tmpDict:
            if idx[0] not in stock_list:
                stock_list.append(idx[0])

        return stock_list

    def sort_stock_list(self, context, stock_list):
        q = query(
                valuation.code,                         # 股票代码
                indicator.roe,
                indicator.roa,
            ).filter(
                valuation.code.in_(stock_list)
            )
        df = get_fundamentals(q).fillna(value=0).set_index('code')

        df = df.sort('roe',ascending=False)
        idx = pd.Series(np.arange(1,len(df)+1), index=df['roe'].index.values)
        roe_rank = pd.DataFrame({'roe_rank': idx})
        df = pd.concat([df, roe_rank], axis=1)

        df = df.sort('roa',ascending=False)
        idx = pd.Series(np.arange(1,len(df)+1), index=df['roa'].index.values)
        roa_rank = pd.DataFrame({'roa_rank': idx})
        df = pd.concat([df, roa_rank], axis=1)

        df['total_rank'] = df['roa_rank'] + df['roe_rank']

        df = df.sort('total_rank',ascending=True)
        return list(df.index)


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

# 创建一个规则执行器，并初始化一些通用事件
def create_rule(class_type, params, memo):
    obj = class_type(params)
    obj.on_open_position = open_position  # 买股
    obj.on_close_position = close_position  # 卖股
    obj.on_clear_position = clear_position  # 清仓
    obj.on_get_obj_by_class_type = get_obj_by_class_type  # 通过类名得到类的实例
    obj.memo = memo
    return obj


# 根据规则配置创建规则执行器
def create_rules(config):
    # 规则配置list下标描述变量。提高可读性与未来添加更多规则配置。
    # 0.是否启用，1.描述，2.规则实现类名，3.规则传递参数(dict)]
    g.cs_enabled, g.cs_memo, g.cs_class_name, g.cs_param = range(4)
    # config里 0.是否启用，1.描述，2.规则实现类名，3.规则传递参数(dict)]
    return [create_rule(c[g.cs_class_name], c[g.cs_param], c[g.cs_memo])
            for c in config if c[g.cs_enabled]]


# 获取前n个单位时间当时的收盘价
def get_close_price(security, n, unit='1d'):
    return attribute_history(security, n, unit, ('close'), True)['close'][0]


# 获取股票n日以来涨幅，根据当前价计算
# n 默认20日
def get_growth_rate(security, n=20):
    for i in range(1, 3):
        lc = get_close_price(security, n)
        c = get_close_price(security, 1, '1m')

        if not isnan(lc) and not isnan(c) and lc != 0:
            return (c - lc) / lc
        else:
            log.info("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" % (security, n, lc, c))
            log.info("第%d次重试" % i)
    log.error("数据非法")
    return 0


# 通过类的类型得到已创建的对象实例
def get_obj_by_class_type(class_type):
    for rule in g.all_rules:
        if rule.__class__ == class_type:
            return rule


# 剔除上市时间较短的基金产品
def delete_new_moneyfund(context, equity, deltaday):
    deltaDate = context.current_dt.date() - dt.timedelta(deltaday)

    tmpList = []
    for stock in equity:
        if get_security_info(stock).start_date < deltaDate:
            tmpList.append(stock)

    return tmpList


def generate_portion(num):
    total_portion = num * (num + 1) / 2
    start = num
    while num != 0:
        yield float(num) / float(total_portion)
        num -= 1


# 持仓操作函数: 开仓，买入指定价值的证券
# 报单成功并成交（包括全部成交或部分成交，此时成交量大于0），返回True
# 报单失败或者报单成功但被取消（此时成交量等于0），返回False
# 报单成功，触发所有规则的when_buy_stock函数
def open_position(sender, security, value):
    order = order_target_value_(sender, security, value)
    if order != None and order.filled > 0:
        for rule in g.all_rules:
            rule.when_buy_stock(security, order)
        return True
    return False


# 持仓操作函数: 平仓，卖出指定持仓
# 平仓成功并全部成交，返回True
# 报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
# 报单成功，触发所有规则的when_sell_stock函数
def close_position(sender, position, is_normal=True):
    security = position.security
    order = order_target_value_(sender, security, 0)  # 可能会因停牌失败
    if order != None:
        if order.filled > 0:
            for rule in g.all_rules:
                rule.when_sell_stock(position, order, is_normal)
            return True
    return False


# 持仓操作函数: 清空卖出所有持仓
# 清仓时，调用所有规则的 when_clear_position
def clear_position(sender, context):
    if context.portfolio.positions:
        sender.log_info("==> 清仓，卖出所有股票")
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            close_position(sender, position, False)
    for rule in g.all_rules:
        rule.when_clear_position(context)


# 持仓操作函数: 自定义下单
# 根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target_value）返回即表示报单完成
# 报单成功返回报单（不代表一定会成交），否则返回None
def order_target_value_(sender, security, value):
    if value == 0:
        sender.log_debug("Selling out %s" % (security))
    else:
        sender.log_debug("Order %s to value %f" % (security, value))

    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return order_target_value(security, value)

# 根据不同的时间段设置滑点与手续费
def set_slip_fee(context):
    # 将滑点设置为0
    slip_ratio = 0.02
    set_slippage(FixedSlippage(slip_ratio))
    log.info('设置滑点率: 固定滑点%f' % slip_ratio)

    # 根据不同的时间段设置手续费
    dt = context.current_dt

    if dt > datetime.datetime(2013, 1, 1):
        set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0003, min_cost=5))

    elif dt > datetime.datetime(2011, 1, 1):
        set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))

    elif dt > datetime.datetime(2009, 1, 1):
        set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))

    else:
        set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))


def fall_money_day_3line(security_list,n, n1=20, n2=60, n3=160):
    def fall_money_count(money, n, n1, n2, n3):
        i = 0
        count = 0
        while i < n:
            money_MA200 = money[i:n3-1+i].mean()
            money_MA60 = money[i+n3-n2:n3-1+i].mean()
            money_MA20 = money[i+n3-n1:n3-1+i].mean()
            if money_MA20 <= money_MA60 and money_MA60 <= money_MA200 :
                count = count + 1
            i = i + 1
        return count

    df = history(n+n3, unit='1d', field='money', security_list=security_list, skip_paused=True)
    s = df.apply(fall_money_count, args=(n,n1,n2,n3,))
    return s

def money_5_cross_60(security_list,n, n1=5, n2=60):
    def money_5_cross_60_count(money, n, n1, n2):
        i = 0
        count = 0
        while i < n :
            money_MA60 = money[i+1:n2+i].mean()
            money_MA60_before = money[i:n2-1+i].mean()
            money_MA5 = money[i+1+n2-n1:n2+i].mean()
            money_MA5_before = money[i+n2-n1:n2-1+i].mean()
            if (money_MA60_before-money_MA5_before)*(money_MA60-money_MA5) < 0 : 
                count=count+1
            i = i + 1
        return count

    df = history(n+n2+1, unit='1d', field='money', security_list=security_list, skip_paused=True)
    s = df.apply(money_5_cross_60_count, args=(n,n1,n2,))
    return s

def cow_stock_value(security_list):
    df = get_fundamentals(query(
                                valuation.code, valuation.pb_ratio, valuation.circulating_market_cap
                            ).filter(
                                valuation.code.in_(security_list),
                                valuation.circulating_market_cap <= 100
                            ))
    df.set_index('code', inplace=True, drop=True)
    s_fall = fall_money_day_3line(df.index.tolist(), 120, 20, 60, 160)
    s_cross = money_5_cross_60(df.index.tolist(), 120)
    df = pd.concat([df, s_fall, s_cross], axis=1, join='inner')
    df.columns = ['pb', 'cap', 'fall', 'cross']
    df['score'] = df['fall'] * df['cross'] / (df['pb']*(df['cap']**0.5))
    df.sort(['score'], ascending=False, inplace=True)
    return(df)

# 剔除周期性行业
def fun_remove_cycle_industry(context, stock_list):
    cycle_industry = [#'A01', # 农业  1993-09-17
                      #'A02', # 林业  1996-12-06
                      #'A03', # 畜牧业     1997-06-11
                      #'A04', # 渔业  1993-05-07
                      #'A05', # 农、林、牧、渔服务业  1997-05-30
                      'B06', # 煤炭开采和洗选业     1994-01-06
                      'B07', # 石油和天然气开采业    1996-06-28
                      'B08', # 黑色金属矿采选业     1997-07-08
                      'B09', # 有色金属矿采选业     1996-03-20
                      'B11', # 开采辅助活动   2002-02-05
                      #'C13', # 农副食品加工业     1993-12-15
                      #C14  食品制造业   1994-08-18
                      #C15  酒、饮料和精制茶制造业     1992-10-12
                      #C17  纺织业     1992-06-16
                      #C18  纺织服装、服饰业    1993-12-31
                      #C19  皮革、毛皮、羽毛及其制品和制鞋业    1994-04-04
                      #C20  木材加工及木、竹、藤、棕、草制品业   2005-05-10
                      #C21  家具制造业   1996-04-25
                      #C22  造纸及纸制品业     1993-03-12
                      #C23  印刷和记录媒介复制业  1994-02-24
                      #C24  文教、工美、体育和娱乐用品制造业    2007-01-10
                      'C25', # 石油加工、炼焦及核燃料加工业   1993-10-25
                      'C26', # 化学原料及化学制品制造业     1990-12-19
                      #C27  医药制造业   1993-06-29
                      'C28', # 化学纤维制造业  1993-07-28
                      'C29', # 橡胶和塑料制品业     1992-08-28
                      'C30', # 非金属矿物制品业     1992-02-28
                      'C31', # 黑色金属冶炼及压延加工业     1994-01-06
                      'C32', # 有色金属冶炼和压延加工业     1996-02-15
                      'C33', # 金属制品业    1993-11-30
                      'C34', # 通用设备制造业  1992-03-27
                      'C35', # 专用设备制造业  1992-07-01
                      'C36', # 汽车制造业    1992-07-24
                      'C37', # 铁路、船舶、航空航天和其它运输设备制造业     1992-03-31
                      'C38', # 电气机械及器材制造业   1990-12-19
                      #C39  计算机、通信和其他电子设备制造业    1990-12-19
                      #C40  仪器仪表制造业     1993-09-17
                      'C41', # 其他制造业    1992-08-14
                      #C42  废弃资源综合利用业   2012-10-26
                      'D44', # 电力、热力生产和供应业  1993-04-16
                      #D45  燃气生产和供应业    2000-12-11
                      #D46  水的生产和供应业    1994-02-24
                      'E47', # 房屋建筑业    1993-04-29
                      'E48', # 土木工程建筑业  1994-01-28
                      'E50', # 建筑装饰和其他建筑业   1997-05-22
                      #F51  批发业     1992-05-06
                      #F52  零售业     1992-09-02
                      'G53', # 铁路运输业    1998-05-11
                      'G54', # 道路运输业    1991-01-14
                      'G55', # 水上运输业    1993-11-19
                      'G56', # 航空运输业    1997-11-05
                      'G58', # 装卸搬运和运输代理业   1993-05-05
                      #G59  仓储业     1996-06-14
                      #H61  住宿业     1993-11-18
                      #H62  餐饮业     1997-04-30
                      #I63  电信、广播电视和卫星传输服务  1992-12-02
                      #I64  互联网和相关服务    1992-05-07
                      #I65  软件和信息技术服务业  1992-08-20
                      'J66', # 货币金融服务   1991-04-03
                      'J67', # 资本市场服务   1994-01-10
                      'J68', # 保险业  2007-01-09
                      'J69', # 其他金融业    2012-10-26
                      'K70', # 房地产业     1992-01-13
                      #L71  租赁业     1997-01-30
                      #L72  商务服务业   1996-08-29
                      #M73  研究和试验发展     2012-10-26
                      'M74', # 专业技术服务业  2007-02-15
                      #N77  生态保护和环境治理业  2012-10-26
                      #N78  公共设施管理业     1992-08-07
                      #P82  教育  2012-10-26
                      #Q83  卫生  2007-02-05
                      #R85  新闻和出版业  1992-12-08
                      #R86  广播、电视、电影和影视录音制作业    1994-02-24
                      #R87  文化艺术业   2012-10-26
                      #S90  综合  1990-12-10
                      ]
    today = context.current_dt
    for industry in cycle_industry:
        stocks = get_industry_stocks(industry, today)
        stock_list = list(set(stock_list).difference(set(stocks)))
        
    return stock_list

# 计算派息率
def fun_get_Dividend_yield(stocks, statsDate):
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

def fun_get_Divid_by_year(context, stocks):
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



def fun_do_trade(context, trade_ratio, moneyfund):

    def __fun_tradeStock(context, stock, ratio):
        total_value = context.portfolio.portfolio_value
        if stock in moneyfund:
            fun_tradeBond(context, stock, total_value * ratio)
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
                            fun_trade(context, stock, Quota)
                    else:
                        fun_trade(context, stock, Quota)
            else:
                fun_trade(context, stock, Quota)

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

def fun_tradeBond(context, stock, Value):
    hStocks = history(1, '1d', 'close', stock, df=False)
    curPrice = hStocks[stock]
    curValue = 0
    if stock in context.portfolio.positions.keys():
      curValue = float(context.portfolio.positions[stock].total_amount * curPrice)

    deltaValue = abs(Value - curValue)
    if deltaValue > (curPrice*100):
        if Value > curValue:
            cash = context.portfolio.cash
            if cash > (curPrice*100):
                fun_trade(context, stock, Value)
        else:
            # 如果是银华日利，多卖 100 股，避免个股买少了
            if stock == '511880.XSHG':
                Value -= curPrice*100
            fun_trade(context, stock, Value)


def fun_trade(context, stock, value):
    fun_setCommission(context, stock)
    order_target_value(stock, value)

def fun_setCommission(context, stock):
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

