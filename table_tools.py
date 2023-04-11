from src.techops_tools.data_cleaner import DataCleaner
import petl as etl


class TableTools():
    def __init__(self):
        self.cleaner = DataCleaner

    def add_value_from_column(self, table, header, column):
        table2 = etl.replace(table, header, None, 0)
        table3 = etl.replace(table2, column, None, 0)
        table4 = etl.convert(table3, header,
                             lambda v, row: float(v) + float(row[column]), pass_row=True)  # noqa
        return table4

    def convert_payroll_earning_hours(self, value):
        if not value:
            return 0
        return float(value)

    def merge_columns(self, table, columns, header):
        if header not in etl.header(table):
            table = etl.addfield(table, header, 0)
        present_columns = [column for column in columns if column in table.header()]  # noqa
        for column in present_columns:
            table = self.add_value_from_column(table, header, column)
            table = etl.transform.headers.rename(table, column, f"ADP {column}")
        return table

    def sum_rows(self, key, rows):
        return [key, sum(row[1] for row in rows)]
