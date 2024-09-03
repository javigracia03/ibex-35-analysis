from mrjob.job import MRJob
from mrjob.step import MRStep

class SectorAveragePrice(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_initial,
                reducer=self.reducer_combine_data
            ),
            MRStep(
                mapper=self.mapper_pass_through,
                reducer=self.reducer_average_price
            )
        ]

    def mapper_initial(self, _, line):
        parts = line.split(',')
        
        if len(parts) == 5:  # Stock data format
            company, _, close_price, _, date = parts
            yield company, ('price_data', (date, float(close_price)))
        elif len(parts) == 4:  # Sector data format
            company, sector, _, _ = parts
            yield company, ('sector_data', sector)

    def reducer_combine_data(self, company, values):
        prices = []
        sector = None

        for dtype, value in values:
            if dtype == 'price_data':
                _, price = value
                prices.append(price)
            elif dtype == 'sector_data':
                sector = value

        if sector:
            for price in prices:
                yield sector, price

    def mapper_pass_through(self, sector, price):
        yield sector, price

    def reducer_average_price(self, sector, prices):
        prices = list(prices)  # Convert generator to list
        total_price = sum(prices)
        count = len(prices)
        average_price = total_price / count if count > 0 else 0
        yield sector, average_price


if __name__ == '__main__':
    SectorAveragePrice.run()
