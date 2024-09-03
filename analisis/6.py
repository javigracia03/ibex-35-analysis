from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import logging

class StockPriceDecrease(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_bottom)
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
            if date > month_ago and last_month_price is None:
                last_month_price = price
            if date > week_ago and last_week_price is None:
                last_week_price = price

        # Calculate decreases if the data is available
        latest_price = sorted_values[-1][1]
        if last_week_price:
            week_decrease = ((latest_price - last_week_price) / last_week_price) * 100
            yield "last_week", (week_decrease, key)
        if last_month_price:
            month_decrease = ((latest_price - last_month_price) / last_month_price) * 100
            yield "last_month", (month_decrease, key)

    def reducer_find_bottom(self, key, values):
        # Find bottom 5 stocks with the greatest decrease
        bottom_stocks = sorted(values)[:5]  # Sort ascending by percentage (decrease)
        # Emit results separately for the week and month
        yield key, [(stock_name, "{:.2f}%".format(decrease)) for decrease, stock_name in bottom_stocks]

if __name__ == '__main__':
    StockPriceDecrease.run()
