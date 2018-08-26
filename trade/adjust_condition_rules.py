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
        if context.preReturn <= 0:
            self.t_can_adjust = False

        if stock_count > 0 : 
            self.day_count += 1    
        pass

    def before_trading_start(self, context):
        self.t_can_adjust = False

        pass

    def when_sell_stock(self, position, order, is_normal):
        # if not is_normal:
            # self.day_count = -15
        pass

    # 清仓时调用的函数
    def when_clear_position(self, context):
        # self.day_count = -15
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


class Precent_condition(Adjust_condition):
    """28指数涨幅调仓判断器"""
    # TODO 可以做性能优化,每天只需计算一次,不需要每分钟调用

    def __init__(self, params):
        self.t_can_adjust = False

    def update_params(self, context, params):
        pass

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
