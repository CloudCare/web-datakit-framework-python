from collections import OrderedDict

class Checker:
    def check_measurement(self, measurement):
        return isinstance(measurement, str)

    def check_tags(self, tags):
        # tags可以为空
        if not tags:
            return True

        if not isinstance(tags, dict) and not isinstance(tags, OrderedDict):
            return False

        ks = [isinstance(k, str) for k in tags.keys()]
        key_valid = all(ks)

        vs = [isinstance(v, str) for v in tags.values()]
        val_valid = all(vs)

        return  key_valid and val_valid

    def check_fields(self, fields):
        # fileds必须非空
        if not fields:
            return False

        if not isinstance(fields, dict) and not isinstance(fields, OrderedDict):
            return False

        ks = [isinstance(k, str) for k in fields.keys()]
        key_valid = all(ks)

        vs = [isinstance(v, (str, int, float, bool)) for v in fields.values()]
        val_valid = all(vs)

        return key_valid and val_valid

    def check_tags_fields(self, tags, fields):
        return self.check_tags(tags) and self.check_fields(fields)

    def check_timestamp(self, timestamp):
        return isinstance(timestamp, int)

    def check(self, measurement=None, tags=None, fields=None, timestamp=None):
        return self.check_measurement(measurement) and self.check_tags_fields(tags, fields) and self.check_timestamp(timestamp)