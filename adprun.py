from argparse import ArgumentParser
from bs4 import BeautifulSoup
import datetime as dt
import re
import sys
import yaml
import petl as etl
import pandas as pd
from src.techops_tools.data_cleaner import DataCleaner
from src.techops_tools.gsheets_handler import GSheetsHandler
from src.techops_tools.table_tools import TableTools


class ADPRun:
    def __init__(self, environment, census_filename, payroll_filename, w2_filename):  # noqa
        self.load_column_mapper()
        self.cleaner = DataCleaner()
        self.census_filename = census_filename
        self.payroll_filename = payroll_filename
        self.pay_date = None
        self.w2_filename = w2_filename
        self.gsheets = GSheetsHandler(environment)

    def load_column_mapper(self):
        with open("src/columns/adprun.yml", "r") as f:
            self.columns = yaml.safe_load(f)

    def add_contribution_columns(self):
        """Adds column from the columns yaml file."""
        for column in self.columns:
            self.full_table = TableTools().merge_columns(self.full_table, self.columns[column], column)  # noqa

    def drop_old_terminated_participants(self, table, days=550):
        table2 = etl.addfield(
            table, "DateDiff",
            lambda x: self.cleaner.get_date_diff(self.pay_date, x["Termination Date"]))
        table3 = etl.convert(table2, "DateDiff", lambda x: 0, where=lambda x: x.DateDiff is None)
        table4 = etl.select(table3, lambda x: x.DateDiff < days)
        table5 = etl.cutout(table4, "DateDiff")
        return table5

    def final_cleanup(self):
        self.full_table = etl.transform.selects.selectisnot(self.full_table, 'SSN', None)  # remove null ssns  # noqa
        hours_columns = ["Period Hours Worked", "Year to Date Hours Worked"]
        for column in hours_columns:
            if column not in etl.header(self.full_table):
                self.full_table = etl.addfield(self.full_table, column, None)
        while "" in etl.header(self.full_table):
            self.full_table = etl.cutout(self.full_table, "")
        self.full_table = etl.cutout(self.full_table, "Employee Name")
        if "Regular" in etl.header(self.full_table):
            self.full_table = etl.rename(self.full_table, "Regular", "Period Gross Pay")  # noqa
        else:
            self.full_table = etl.addfield(self.full_table, "Period Gross Pay", None)
        self.full_table = self.drop_old_terminated_participants(self.full_table)

    def set_pay_date(self):
        """Gets the paydate from the payroll file."""
        pay_date = dt.datetime.strptime(self.cleaner.normalize_date_format(
            next(d for d in self.payroll_table.values("Payroll Check Date") if d)), "%m/%d/%Y").date()  # noqa
        self.pay_date = pay_date

    def get_png_file_name(self):
        return f"ADPRun-{self.company}-{self.pay_date}.csv"

    def convert_date_column(self, table, column):
        table = table.convert(column, lambda d: self.cleaner.normalize_date_format(d))  # noqa
        return table

    def merge_files(self):
        """Merges and transforms the census and payroll files."""
        full_table = etl.leftjoin(self.census_table, self.payroll_table, key='Employee XID')  # noqa
        date_columns = ["Birth Date", "Hire Date", "Termination Date", "Payroll Check Date"]  # noqa
        for column in date_columns:
            full_table = self.convert_date_column(full_table, column)
        self.full_table = full_table

    def parse_census_file(self, census_filename, w2_filename):
        census_table = etl.fromcsv(census_filename)
        census_table = census_table.skip(1)
        self.employee_names = list(census_table["Employee Name"])
        ssn_table = self.parse_w2_file(w2_filename)
        census_table = etl.leftjoin(census_table, ssn_table, key="Employee Name")  # noqa
        census_table = census_table.addfield("Employee XID", lambda rec: self.cleaner.generate_xid(rec["Employee Name"], rec["Birth Date"]))  # noqa
        self.census_table = census_table

    def parse_payroll_file(self, payroll_filename):
        paytable = etl.fromcsv(payroll_filename)
        paytable = paytable.skip(1)
        paytable = paytable.convert('Payroll Deduction Description',
                                    lambda field: field.replace(' %', ' $'))
        paytable = paytable.addfield('Employee XID', lambda rec: self.cleaner.generate_xid(  # noqa
            rec['Employee Name'], rec['Birth Date']))
        pt2 = paytable.convertnumbers().recast(
            key=['Employee XID', 'Payroll Check Date'],
            variablefield='Payroll Earning Description',
            valuefield='Payroll Earning Amount', reducers={'Regular': sum})
        pt2 = etl.cutout(pt2, 'Payroll Check Date')
        if "Payroll Earning Hours" in etl.header(paytable):
            pt3 = etl.cut(paytable, 'Employee XID', 'Payroll Earning Hours')
            pt3 = etl.convert(pt3, 'Payroll Earning Hours', lambda x: TableTools().convert_payroll_earning_hours(x))  # noqa
            pt3 = etl.rowreduce(pt3, key='Employee XID', reducer=TableTools().sum_rows, header=['Employee XID', 'Period Hours Worked'])  # noqa
        paytable = paytable.recast(key=['Employee XID', 'Payroll Check Date'],
                                   variablefield='Payroll Deduction Description',  # noqa
                                   valuefield='Payroll Deduction Amount')
        paytable = etl.leftjoin(paytable, pt2, key='Employee XID')
        if 'pt3' in locals():
            paytable = etl.leftjoin(paytable, pt3, key='Employee XID')
        self.payroll_table = paytable

    def parse_w2_file(self, w2_filename):
        ssn_file = open(w2_filename)
        ssn_soup = BeautifulSoup(ssn_file, 'html.parser')
        ssn_file.close()
        company_tag = ssn_soup.find(string=re.compile('Company:'))
        if not company_tag:
            company_tag = ssn_soup.find(string=re.compile("Employer's Name")).findNext('td').contents[0]
            self.company = str(company_tag.string).strip().replace(' ', '-')
        else:
            company_str = str(company_tag.string).strip()
            self.company = re.match(r'Company: ([\w ]+)', company_str).group(1).strip().replace(' ', '-')  # noqa

        # for cleaning the name/ssn block
        regex = re.compile('\t')
        regex_b = re.compile('\n')
        regex_c = re.compile('\xa0')
        regex_d = re.compile('SSN :                    ')

        # extract names and ssns
        name_ssn_block = ssn_soup.find_all('td', 'NameHeader', colspan='3')
        name_ssn = []
        for element in name_ssn_block:
            column_label = element.get_text(strip=True)
            column_label = regex.sub('', column_label)
            column_label = regex_b.sub('', column_label)
            column_label = regex_c.sub(' ', column_label)
            column_label = regex_d.sub('', column_label)
            name_ssn.append(column_label)
        names = name_ssn[::2]
        ssn_df = name_ssn[1::2]
        ssn_df = [e for e in ssn_df]  # ssn cleanup

        # gather names and ssns
        last_name = []
        first_name = []
        found_str = "name {} found in employee names"
        not_found_str = "name {} NOT found in employee names"
        for idx, name in enumerate(names):
            name_no_middle = self.cleaner.remove_middle_initial(name)
            last, first = name_no_middle.split(',')
            first = first.strip()
            last_name.append(last)

            if name not in self.employee_names:

                # handle "LAST,FIRSTM" and "LAST,FIRST"
                if first.isupper():
                    if name not in self.employee_names:
                        print(not_found_str.format(name))

                        # assume no middle name first
                        name = f"{last}, {first}"
                        names[idx] = name

                        # if name not found, last letter may be middle initial
                        if name not in self.employee_names:
                            print(not_found_str.format(name))
                            name = f"{last}, {first[:-1]} {first[-1]}"
                            names[idx] = name
                            if name not in self.employee_names:
                                print(not_found_str.format(name))
                            else:
                                print(found_str.format(name))
                        else:
                            print(found_str.format(name))
                    else:
                        print(found_str.format(name))

                # handle format "Last,FirstM" AND "Last,First"
                else:
                    if first[-1].isupper():
                        middle = first[-1]
                        first = first[:-1]
                        name = f"{last}, {first} {middle}"
                        names[idx] = name
                        if name in self.employee_names:
                            print(found_str.format(name))
                    else:
                        name = f"{last}, {first}"
                        names[idx] = name
                        if name in self.employee_names:
                            print(found_str.format(name))
                        
            else:
                print(found_str.format(name))
            first_name.append(first)
        ssn_table = etl.fromcolumns([names,
                                     ssn_df,
                                     first_name,
                                     last_name])
        ssn_table = ssn_table.setheader(['Employee Name', 'SSN', 'First Name', 'Last Name'])  # noqa
        ssn_table_clean = etl.distinct(ssn_table, 'SSN')  # drop duplicate ssns
        return ssn_table_clean

    def convert_files_to_png(self):
        self.parse_census_file(f"src/raw_files/{self.census_filename}", f"src/raw_files/{self.w2_filename}")
        self.parse_payroll_file(f"src/raw_files/{self.payroll_filename}")
        self.set_pay_date()
        self.merge_files()
        self.add_contribution_columns()
        self.final_cleanup()
        self.full_table.tocsv("full_table.csv")
        self.png_file = pd.read_csv("full_table.csv")
        self.format_for_jiffy()
        return self.full_table

    def format_for_jiffy(self):
        column_mappings = {
            "Employee Address Line 1": "Street Address #1",
            "Employee Address Line 2": "Street Address #2",
            "Employee City": "City",
            "Employee State": "State",
            "Employee ZIP": "Zip Code",
            "Employee Telephone Number": "Phone",
            "Personal Email": "Email Address",
            "Work Email": "Secondary Email Address",
            "Hire Date": "Date of Hire",
            "Birth Date": "Date of Birth",
            "": "Date of Rehire",
            "Termination Date": "Date of Termination",
            "SSN": "Social Security Number",
            "Period Gross Pay": "Current Period Compensation",
            "Period Hours Worked": "Current Period Hours",
            "Pre-tax Contribution": "Pre-tax Contribution Amount",
            "Roth Contribution": "Roth Contribution Amount",
            "Loan Repayment": "Loan Payment",
            "Year to Date Hours Worked": "YTD Hours",
        }
        self.png_file.rename(columns=column_mappings, inplace=True)
        for col in ["Date  of Rehire", "Division", "Gross Salary"]:
            self.png_file[col] = ""


def main():
    parser = ArgumentParser()
    parser.add_argument("-e", "--environment")
    parser.add_argument("-g", "--google_drive_link")
    args = parser.parse_args()

    # get quickbook files from google drive
    gsheets = GSheetsHandler(args.environment)
    gsheets.get_adprun_files(args.google_drive_link)

    # convert files to png format
    adprun = ADPRun(
        args.environment,
        gsheets.census_filename,
        gsheets.payroll_filename,
        gsheets.w2_filename
    )
    adprun.convert_files_to_png()

    # upload the file to the google drive folder
    gsheets.upload_gsheet(
        df=adprun.png_file,
        folder_id=gsheets.folder_id,
        title=adprun.get_png_file_name()
    )


if __name__ == "__main__":
    sys.exit(main())
