from mrjob.job import MRJob
from mrjob.step import MRStep





class SectorGrowth(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_initial,
                reducer=self.reducer_combine_data
            ),
            MRStep(
                reducer=self.reducer_calculate_and_rank_sectors
            )
        ]

    def mapper_initial(self, _, line):
        # Split the input line by comma
        parts = line.split(',')
        
        # Determine if this is stock data or sector data based on column count
        if len(parts) == 5:  # Assuming stock data has 5 columns
            company, _, close_price, _, date = parts
            yield company, ('price_data', (date, float(close_price)))
        elif len(parts) == 4:  # Assuming sector data has 4 columns
            company, sector, _, _ = parts
            yield company, ('sector_data', sector)

    def reducer_combine_data(self, company, values):
        # Initialize data structures for stock prices and sector
        prices = []
        sector = None

        # Collect all price data and sector for each company
        for dtype, value in values:
            if dtype == 'price_data':
                prices.append(value)
            elif dtype == 'sector_data':
                sector = value

        # Emit combined data
        if sector:
            for date, price in prices:
                yield sector, (date, price)

    def reducer_calculate_and_rank_sectors(self, sector, values):
        # Process the list of prices to find the earliest and latest prices
        min_date = max_date = None
        min_price = max_price = 0

        for date, price in values:
            if min_date is None or date < min_date:
                min_date = date
                min_price = price
            if max_date is None or date > max_date:
                max_date = date
                max_price = price

        # Calculate growth
        if min_date != max_date:
            growth = (max_price - min_price) / min_price * 100
            yield sector, growth

if __name__ == '__main__':
    SectorGrowth.run()
