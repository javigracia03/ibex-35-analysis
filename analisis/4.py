from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta

class MRStockPriceAnalysis(MRJob):

    def configure_args(self):
        super(MRStockPriceAnalysis, self).configure_args()
        self.add_passthru_arg('--action_name', help='Name of the stock to analyze')

    def is_valid_time(self, timestamp):
        try:
            datetime.strptime(timestamp, '%H:%M')
            return True
        except ValueError:
            return False

    def mapper(self, _, line):
        data = line.strip().split(',')
        if len(data) == 6:
            stock_name, open_price, high_price, low_price, timestamp, date = data
            if self.is_valid_time(timestamp) and stock_name == self.options.action_name:
                timestamp_str = datetime.strptime(timestamp, '%H:%M').strftime('%H:%M')  # Convert to string
                yield stock_name, (float(high_price), float(low_price),  timestamp_str, date)  # Yield string representation of timestamp

    def reducer(self, key, values):
        hour_prices = []
        week_prices = []
        month_prices = []
        now = datetime.now()

        for low_price, high_price, timestamp_str, date in values:
            timestamp = datetime.strptime(date + ' ' + timestamp_str, '%Y-%m-%d %H:%M')  # Combine date and time
            if now - timestamp <= timedelta(hours=1):
                hour_prices.extend([low_price, high_price])
            if now - timestamp <= timedelta(days=7):
                week_prices.extend([low_price, high_price])
            if now - timestamp <= timedelta(days=30):
                month_prices.extend([low_price, high_price])

        result = {
            'Minimum Price (Last Hour)': min(hour_prices) if hour_prices else None,
            'Maximum Price (Last Hour)': max(hour_prices) if hour_prices else None,
            'Minimum Price (Last Week)': min(week_prices) if week_prices else None,
            'Maximum Price (Last Week)': max(week_prices) if week_prices else None,
            'Minimum Price (Last Month)': min(month_prices) if month_prices else None,
            'Maximum Price (Last Month)': max(month_prices) if month_prices else None
        }

        yield key, result

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    MRStockPriceAnalysis.run()
