class LineProtoBuilder:
    def build(self, measurement=None, tags=None, fields=None, timestamp=None):
        line_proto_data = ""
        line_proto_data += "{}".format(measurement)
        is_frist_field = True
        # tags可选
        if tags:
            for key, val in tags.items():
                line_proto_data += ",{}={}".format(key, val)
        # fields必填
        for key, val in fields.items():
            if is_frist_field:
                prefix = " "
                is_frist_field = False
            else:
                prefix = ","

            line_proto_data += "{}{}={}".format(prefix, key, self._conv_field_str(val))
        if timestamp:
            line_proto_data += " {}".format(timestamp)
        line_proto_data += "\n"
        return line_proto_data

    def _conv_field_str(self, value):
        type_str = type(value).__name__
        if type_str == "int":
            return "{}i".format(value)
        elif type_str == "str":
            return '"{}"'.format(value)
        else:
            return "{}".format(value)