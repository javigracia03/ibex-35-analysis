from mrjob.job import MRJob
from mrjob.step import MRStep

class CompanyGrowthAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_data,
                reducer=self.reducer_combine_data
            ),
            MRStep(
                reducer=self.reducer_calculate_growth
            )
        ]

    def mapper_get_data(self, _, line):
        parts = line.split(',')
        
        if len(parts) == 5:  
            company = parts[0]
            close_price = float(parts[1])
            date = parts[4]
            yield company, ('stock_data', close_price, date)
        elif len(parts) == 4:  
            company = parts[0]
            number_of_workers = int(parts[3])
            yield company, ('employee_data', number_of_workers)

    def reducer_combine_data(self, company, values):
        first_price = None
        last_price = None
        first_date = None
        last_date = None
        number_of_workers = None

        # Process all values to find the first and last stock price and employee data
        for dtype, *details in values:
            if dtype == 'stock_data':
                price, date = details
                if first_date is None or date < first_date:
                    first_date = date
                    first_price = price
                if last_date is None or date > last_date:
                    last_date = date
                    last_price = price
            elif dtype == 'employee_data':
                workers = details[0]
                number_of_workers = workers

        if number_of_workers is not None and first_price is not None and last_price is not None:
            yield company, (first_price, last_price, number_of_workers)

    def reducer_calculate_growth(self, company, values):
        for first_price, last_price, workers in values:
            if workers > 0 and first_price != last_price:
                growth = (last_price - first_price) / first_price
                productivity_growth_per_employee = growth / workers
                yield company, productivity_growth_per_employee

if __name__ == '__main__':
    CompanyGrowthAnalysis.run()
