from unittest import TestCase

from .base import TemplateCompiler, Template, TemplateContext


class TemplateCompilerTestCase(TestCase):
    def test_1(self):
        template_compiler = TemplateCompiler()

        self.assertRaises(TypeError, template_compiler.compile, None)


class TemplateTestCase(TestCase):
    def setUp(self):
        self.template_compiler = TemplateCompiler()
        self.template_context = TemplateContext()

    def test_empty(self):
        template = self.template_compiler.compile('')
        self.assertTrue(template)
        self.assertIsInstance(template, Template)
        self.assertEqual(template.render(self.template_context), '')

    def test_string(self):
        template = self.template_compiler.compile('test')
        self.assertTrue(template)
        self.assertIsInstance(template, Template)
        self.assertEqual(template.render(self.template_context), 'test')

    def test_escaped_macro_token(self):
        template = self.template_compiler.compile('$$test$')
        self.assertTrue(template)
        self.assertIsInstance(template, Template)
        self.assertEqual(template.render(self.template_context), '$test$')

        template = self.template_compiler.compile('$$test$$')
        self.assertTrue(template)
        self.assertIsInstance(template, Template)
        self.assertEqual(template.render(self.template_context), '$test$')

    def test_macro(self):
        template = self.template_compiler.compile('test $test$ test')
        self.assertTrue(template)
        self.assertIsInstance(template, Template)

        self.assertEqual(
            template.render(self.template_context),
            'test  test')

        self.template_context.add_namespace({'test': 'abc'})
        self.assertEqual(
            template.render(self.template_context),
            'test abc test')

        self.template_context = TemplateContext()
        self.template_context.add_namespace({'test': 'def'})
        self.assertEqual(
            template.render(self.template_context),
            'test def test')

    def test_callable_macro(self):
        template = self.template_compiler.compile('test $test$ test')
        self.template_context = TemplateContext()
        self.template_context.add_namespace({'test': lambda: 123})
        self.assertEqual(
            template.render(self.template_context),
            'test 123 test')

    def test_empty_function(self):
        self.template_context.add_namespace({
            '': lambda: 123
        })

        template = self.template_compiler.compile('test $()$ test')
        self.assertEqual(
            template.render(self.template_context),
            'test  test')

    def test_function(self):
        self.template_context.add_namespace({
            'test': lambda: 123
        })

        template = self.template_compiler.compile('test $test()$ test')
        self.assertEqual(
            template.render(self.template_context),
            'test 123 test')

    def test_function_args(self):
        self.template_context.add_namespace({
            'arg': 3,
            'function': lambda x: x * x
        })

        template = self.template_compiler.compile('test $function(arg)$ test')
        self.assertEqual(
            template.render(self.template_context),
            'test 9 test')

    def test_nested_function(self):
        self.template_context.add_namespace({
            'arg': 3,
            'function': lambda x: x * x
        })

        template = self.template_compiler.compile(
            'test $function(function(arg))$ test')
        self.assertEqual(
            template.render(self.template_context),
            'test 81 test')
