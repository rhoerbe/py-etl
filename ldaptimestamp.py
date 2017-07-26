import ldap3
import pytz
import datetime


class LdapTimeStamp(object):
    def __init__(self, attribute=None):
        if attribute:
            if isinstance(attribute, datetime.datetime):
                self.value = attribute
            else:
                #TODO instance check
                self.value = attribute.value
        else:
            self.value = None

    def as_generalized_time(self):
        return self.generalized_time(self.value)

    def __repr__(self):
        return str(self.value)

    def __lt__(self,other):
        if isinstance(other, LdapTimeStamp):
            return self.value < other.value
        else:
            return self.value < other

    @staticmethod
    def generalized_time(ts_value):
        utc_date_time = ts_value.astimezone(pytz.utc)
        gt_string = ldap3.protocol.formatters.validators.validate_time(utc_date_time)
        return gt_string
