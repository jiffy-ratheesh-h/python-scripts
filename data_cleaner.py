import datetime as dt
from decimal import Decimal
import re


class DataCleaner():
    def decimalize(string_num):
        return Decimal(string_num or "0.0")

    def remove_middle_initial(self, name):
        with_middle_initial = re.match(r"(.+)( [a-zA-z]$)", name)
        if with_middle_initial:
            return with_middle_initial.group(1).strip()
        else:
            return name.strip()

    @staticmethod
    def get_date_diff(date1, date2):
        if date1 and date2:
            if type(date1) == str:
                date1 = dt.datetime.strptime(date1, "%m/%d/%Y").date()
            if type(date2) == str:
                date2 = dt.datetime.strptime(date2, "%m/%d/%Y").date()
            return abs((date2 - date1).days)

    def normalize_date_format(self, datestr, outfmt="%m/%d/%Y"):
        datestr = re.match(r"(\d+/\d+/\d+)", datestr).group(1)
        fmt = "%m/%d/%y" if ((len(datestr) - 1 - datestr.rfind("/")) == 2) else "%m/%d/%Y"  # noqa
        return dt.datetime.strptime(datestr, fmt).date().strftime(outfmt)

    def generate_xid(self, name, datestr):
        normaldate = ""
        if datestr:
            normaldate = self.normalize_date_format(datestr, "%m/%d/%y")
            return f"{self.remove_middle_initial(name)} {normaldate}"
