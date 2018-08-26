# ===================== 各种止损实现 ================================================
class Stop_stocks_hmm(Rule):
    """日内个股止损器,日内价格低于当日最高值达到阈值则平仓止损"""

    def __init__(self, params):
        self.on_close_position = close_position  # 卖股回调函数

    def update_params(self, context, params):
        pass

    def before_trading_start(self, context):
        pass

    # 个股止损
    def handle_data(self, context, data):
        self.calc_prereturn(context)
        if context.preReturn <= 0 :
            for stock in context.trade_ratio:
                context.trade_ratio[stock] = 0
            
        pass

    def calc_prereturn(self, context):
        today = context.current_dt
        yestoday = today - datetime.timedelta(days=1)
        if today.month != context.lastMonth:
            context.lastMonth = today.month
            A, logReturn_1 = func_get_hmm_data('000001.XSHG', '2005-05-20', yestoday)
            g.hmm.fit(A)
            
        start_date = today - datetime.timedelta(days=91)
        A, Re = func_get_hmm_data('000001.XSHG', start_date, yestoday)
        hidden_states = g.hmm.predict(A)
        # print('Re', Re)
        expect = []
        for i in range(g.hmm.n_components):
            state = (hidden_states == i)
            idx = np.append(0,state[:-1])
            # print('idx', i, idx)
            expect.append(np.mean([idx[j] * Re[j] for j in range(len(idx))]))
            
        signl = 1
        # if expect
            
        # print("hidden_states[-1]", hidden_states[-1])
        pro=np.array([g.hmm.transmat_[hidden_states[-1],i] for i in range(g.hmm.n_components)])
        preReturn = pro.dot(expect) + 0.0025
        record(preReturn=preReturn*1000)
        context.preReturn = preReturn
        # print('expect=',expect, pro, preReturn)
        pass

    def __str__(self):
        return 'HMM止损'


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
            xi = attribute_history(stock, 2, '1d', fields=['close','high'], skip_paused=True)
            ma = xi['high'].max()
            cur_price = xi['close'][-1]

            if not self.last_high.has_key(stock) or self.last_high[stock] < cur_price:
                self.last_high[stock] = cur_price

            # threshold = self.__get_stop_loss_threshold(stock, self.period)
            # log.debug("个股止损阈值, stock: %s, threshold: %f" %(stock, threshold))
            if cur_price < self.last_high[stock] * 0.6:
                # msg = "==> 个股止损, stock: %s, cur_price: %f, last_high: %f, threshold: %f" \
                #               % (stock, cur_price, self.last_high[stock], threshold)
                # self.log_weixin(context, msg)
                position = context.portfolio.positions[stock]
                if stock in context.trade_ratio:
                    context.trade_ratio[stock] = 0
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
        return '股票调仓买入规则：使用 VaR 方式买入'