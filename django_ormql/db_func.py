from django.db.models import Func


class Equal(Func):
    arg_joiner = ' = '
    arity = 2
    function = ''
    conditional = True


class NotEqual(Func):
    arg_joiner = ' != '
    arity = 2
    function = ''
    conditional = True


class GreaterThan(Func):
    arg_joiner = ' > '
    arity = 2
    function = ''


class GreaterEqualThan(Func):
    arg_joiner = ' >= '
    arity = 2
    function = ''


class LowerEqualThan(Func):
    arg_joiner = ' <= '
    arity = 2
    function = ''


class LowerThan(Func):
    arg_joiner = ' < '
    arity = 2
    function = ''


class Add(Func):
    arg_joiner = ' + '
    arity = 2
    function = ''


class Sub(Func):
    arg_joiner = ' - '
    arity = 2
    function = ''


class Mul(Func):
    arg_joiner = ' * '
    arity = 2
    function = ''


class Div(Func):
    arg_joiner = ' / '
    arity = 2
    function = ''
