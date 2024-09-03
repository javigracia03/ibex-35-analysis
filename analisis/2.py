from mrjob.job import MRJob
from mrjob.step import MRStep

class MRStockSummary(MRJob):

    def mapper(self, _, line):
        # Split the line into components
        data = line.split(',')
        stock_name = data[0]
        last_quote = float(data[1])
        max_session = float(data[2])
        min_session = float(data[3])
        date = data[4]

        # Emit stock name as key and a tuple of the rest as value
        yield stock_name, (date, last_quote, max_session, min_session)

    def reducer(self, stock_name, values):
        # Initialize the variables to find initial, final, min, and max values
        sorted_values = sorted(values, key=lambda x: x[0])  # Sort values by date
        initial_value = sorted_values[0][1]  # First entry's last quote
        final_value = sorted_values[-1][1]  # Last entry's last quote
        min_value = min(v[3] for v in sorted_values)  # Minimum of session minimums
        max_value = max(v[2] for v in sorted_values)  # Maximum of session maximums

        # Emit the stock name and the calculated values
        yield stock_name, (initial_value, final_value, min_value, max_value)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    MRStockSummary.run()
