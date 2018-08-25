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