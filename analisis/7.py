from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import os

class StockPriceSpecificIncrease(MRJob):

    def configure_args(self):
        super(StockPriceSpecificIncrease, self).configure_args()
        self.add_passthru_arg('--percent', type=float, help='Target percent increase to filter for', default=5)  # Default 5% for demonstration

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_filter_by_percent),
            #MRStep(reducer=self.reducer_deduplicate)  # Add a second reducer step to deduplicate results
        ]

    def mapper(self, _, line):
        # Get the filename from input_split, which contains information about the input file
        filename = os.environ['mapreduce_map_input_file']
        # Extract date from filename, assuming format 'stocks_data_YYYY-MM-DD.csv'
        date_from_filename = filename.split('_')[-1].split('.')[0]
        reference_date = datetime.datetime.strptime(date_from_filename, '%Y-%m-%d')

        parts = line.split(',')
        name = parts[0]
        high_price = float(parts[2])

        yield name, (reference_date.strftime('%Y-%m-%d'), high_price)

    def reducer_filter_by_percent(self, key, values):
        prices = [(datetime.datetime.strptime(date_str, '%Y-%m-%d'), price) for date_str, price in values]
        prices.sort(key=lambda x: x[0])

        n = len(prices)
        for i in range(n):
            for j in range(i + 1, n):
                earlier_date, earlier_price = prices[i]
                later_date, later_price = prices[j]
                increase = (later_price - earlier_price) / earlier_price * 100

                if increase >= self.options.percent:
                    yield key,  f"{increase:.2f}%"

    def reducer_deduplicate(self, key, values):
        seen = set()
        for value in values:
            value_tuple = tuple(value)  # Convert list to tuple
            if value_tuple not in seen:
                yield key, value
                seen.add(value_tuple)

if __name__ == '__main__':
    StockPriceSpecificIncrease.run()
