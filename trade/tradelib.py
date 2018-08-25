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