

class Event(object):
    def __init__(self, *args):
        for k, v in zip(self.FIELDS, args):
            setattr(self, k, v)

    @classmethod
    def new(cls, name, fields, **kwargs):
        kwargs['FIELDS'] = fields.split(" ")
        return type(name, (cls,), kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return all((getattr(self, f) == getattr(other, f) for f in self.FIELDS))
        return False

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        reprtxt = ', '.join('%s=%r' % (name, getattr(self, name)) for name in self.FIELDS)
        return "%s(%s)" % (self.__class__.__name__, reprtxt)
