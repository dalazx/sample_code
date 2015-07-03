import re


class BaseTemplateCompiler(object):
    def compile(self, template_str):
        raise NotImplementedError()


class TemplateCompiler(object):
    MACRO_RE = re.compile(r'\$\$|(?<!\$)\$[^\$]+\$(?!\$)')
    FUNCTION_RE = re.compile(r'(\w*)\((.*)\)')

    def __init__(self):
        self._macro_token = '$'
        self._macro_token_item = StringTemplateItem(self._macro_token)

    def compile(self, template_str):
        template_str_end = 0
        template = Template()

        for match in self.MACRO_RE.finditer(template_str):
            original_macro = match.group()
            macro = original_macro.strip(self._macro_token)
            match_start, match_end = match.span()

            if template_str_end < match_start:
                template.add_item(StringTemplateItem(
                    template_str[template_str_end:match_start]))
            template_str_end = match_end

            if macro:
                template.add_item(self._parse_macro(macro))
            else:
                template.add_item(self._macro_token_item)

        if template_str_end < len(template_str):
            template.add_item(StringTemplateItem(
                template_str[template_str_end:len(template_str)]))

        return template

    def _parse_macro(self, macro):
        function_match = self.FUNCTION_RE.search(macro)
        if function_match:
            function_macro = function_match.group(1)

            # TODO: implement multiple arguments
            function_argument_macros = []
            if function_match.group(2):
                function_argument_macros.append(function_match.group(2))
            function_argument_items = [
                self._parse_macro(function_argument_macro)
                for function_argument_macro in function_argument_macros]

            return FunctionTemplateItem(
                function_macro, function_argument_items)

        return MacroTemplateItem(macro)


class BaseTemplateItem(object):
    def render(self, context):
        raise NotImplementedError()


class StringTemplateItem(BaseTemplateItem):
    def __init__(self, string):
        self._string = string

    def render(self, context):
        return self._string


class MacroTemplateItem(BaseTemplateItem):
    def __init__(self, macro):
        self._macro = macro

    def render(self, context):
        # TODO: find a match or return an empty string
        try:
            value = context.lookup(self._macro)
            if callable(value):
                return value()
            return value
        except LookupException:
            pass


class FunctionTemplateItem(MacroTemplateItem):
    def __init__(self, macro, argument_items):
        super(FunctionTemplateItem, self).__init__(macro)

        self._argument_items = argument_items

    def render(self, context):
        try:
            function = context.lookup(self._macro)
        except LookupException:
            return

        if not callable(function):
            return

        args = [item.render(context) for item in self._argument_items]
        try:
            return function(*args)
        except:
            pass


class BaseTemplateContext(object):
    def lookup(self, attr):
        raise NotImplementedError()


class LookupException(Exception):
    pass


class TemplateContext(BaseTemplateContext):
    def __init__(self):
        self._namespaces = []

    def add_namespace(self, namespace):
        self._namespaces.append(namespace)

    @classmethod
    def lookup_namespace(cls, namespace, attrs):
        attr = attrs[0]
        attrs = attrs[1:]

        if not attr:
            raise LookupException('Empty attribute')

        if isinstance(namespace, dict) and attr in namespace:
            value = namespace[attr]
        elif hasattr(namespace, attr):
            value = getattr(namespace, attr)
        else:
            raise LookupException('Not found')

        if attrs:
            return cls.lookup_namespace(value, attrs)
        return value

    def lookup(self, attr):
        attrs = attr.split('.')
        for namespace in self._namespaces:
            try:
                return self.lookup_namespace(namespace, [attr])
            except LookupException:
                pass
            try:
                return self.lookup_namespace(namespace, attrs)
            except LookupException:
                pass
        raise LookupException('Not found')


class Template(BaseTemplateItem):
    def __init__(self):
        self._items = []

    def add_item(self, item):
        self._items.append(item)

    def _render_items(self, context):
        for item in self._items:
            value = item.render(context)
            if value is None:
                yield ''
            elif isinstance(value, basestring):
                yield value
            else:
                yield str(value)

    def render(self, context):
        return ''.join(self._render_items(context))
