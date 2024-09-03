from mrjob.job import MRJob
from mrjob.step import MRStep

class MRStockPriceAnalysis(MRJob):
    def configure_args(self):
        super(MRStockPriceAnalysis, self).configure_args()
        # Adding command-line arguments to pass the stock name
        self.add_passthru_arg('-s', '--stock', type=str, help='Name of the stock to analyze')

    def mapper(self, _, line):
        # Extract data from each line
        data = line.split(',')
        stock_name, last_quote = data[0], float(data[1])
        # Emitting only the stock name as key to aggregate all dates
        if stock_name == self.options.stock:
            yield stock_name, last_quote

    def reducer_init(self):
        self.min_price = float('inf')
        self.max_price = float('-inf')
        self.initial_price = None  # Initialize initial price storage

    def reducer(self, stock_name, prices):
        sorted_prices = sorted(prices)
        if not self.initial_price:
            self.initial_price = sorted_prices[0]  # Set the first price as initial price

        for price in sorted_prices:
            if price < self.min_price:
                self.min_price = price
            if price > self.max_price:
                self.max_price = price

    def reducer_final(self):
        # Calculate percentage changes from the initial price to min and max prices
        if self.initial_price is not None:
            min_change = ((self.min_price - self.initial_price) / self.initial_price) * 100
            max_change = ((self.max_price - self.initial_price) / self.initial_price) * 100
            yield self.options.stock, {
                'Initial Price': self.initial_price,
                'Minimum Price': self.min_price,
                'Maximum Price': self.max_price,
                'Decrease % to Min': min_change,
                'Increase % to Max': max_change
            }

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final)
        ]

if __name__ == '__main__':
    MRStockPriceAnalysis.run()
