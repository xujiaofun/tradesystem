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