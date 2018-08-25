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