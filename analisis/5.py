from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import logging

class StockPriceIncrease(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_top)
        ]

    def mapper(self, _, line):
        # Split the line into parts: name, open_price, high_price, low_price, date
        parts = line.split(',')
        name = parts[0]
        price = float(parts[1])
        date = parts[4]  # Emit date as string
        yield name, (date, price)

    def reducer(self, key, values):
        # Sort values by date
        sorted_values = sorted(values, key=lambda x: datetime.datetime.strptime(x[0], '%Y-%m-%d'))

        # Get today's date as reference
        today = datetime.datetime.today()
        week_ago = today - datetime.timedelta(days=7)
        month_ago = today - datetime.timedelta(days=30)

        # Initialize variables to track the first relevant prices within the time frames
        last_week_price = None
        last_month_price = None

        # Iterate through sorted values to find the relevant prices
        for date_str, price in sorted_values:
            date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            # Checking for the first relevant price within the last month
            if date > month_ago and last_month_price is None:
                last_month_price = price
            # Checking for the first relevant price within the last week
            if date > week_ago and last_week_price is None:
                last_week_price = price

        # Calculate increases if the data is available
        latest_price = sorted_values[-1][1]
        if last_week_price:
            week_increase = ((latest_price - last_week_price) / last_week_price) * 100
            yield "last_week", (week_increase, key)
        if last_month_price:
            month_increase = ((latest_price - last_month_price) / last_month_price) * 100
            yield "last_month", (month_increase, key)

    def reducer_find_top(self, key, values):
        # Find top 5 stocks with the greatest increase
        top_stocks = sorted(values, reverse=True)[:5]
        # Emit results separately for the week and month
        yield key, [(stock_name, "{:.2f}%".format(increase)) for increase, stock_name in top_stocks]

if __name__ == '__main__':
    StockPriceIncrease.run()
