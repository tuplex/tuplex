#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

import ast
import astor
import os
import sys
from types import LambdaType, CodeType
import logging

def supports_lambda_closure():
    """
    source code of lambdas can't be extracted, because there's no column information available
    in code objects. This can be achieved by patching 4 lines in the cpython source code.
    This function checks whether a patched interpreter is used or not.
    Returns: True if operated with patched interpreter, False otherwise

    """
    f = lambda x: x * x  # dummy function
    return hasattr(f.__code__, 'co_firstcolno')


def extract_all_lambdas(tree):
    lambdas = []

    class Visitor(ast.NodeVisitor):
        def visit_Lambda(self, node):
            lambdas.append(node)

    Visitor().visit(tree)

    return lambdas


# extract for lambda incl. default values
# annotations are not possible with the current syntax...
def args_for_lambda_ast(lam):
    return [getattr(n, 'arg') for n in lam.args.args]


def gen_code_for_lambda(lam):
    # surround in try except if user provided malformed lambdas
    try:
        s = astor.to_source(lam)

        # hack for noparam lambda.
        # astor generates here lambda :
        # but we want lambda:
        if 0 == len(lam.args.args):
            assert 'lambda :' in s
            s = s.replace('lambda :', 'lambda:')

        return s.strip()[1:-1]
    except Exception as e:
        logging.debug('gen_code_for_lambda via astor failed with {}'.format(e))

        # python3.9+ has ast.unparse
        if sys.version_info.major >= 3 and sys.version_info.minor >= 9:
            import ast

            try:
                s = ast.unparse(lam)
                return s
            except Exception as e:
                logging.debug('gen_code_for_lambda via ast (python3.9+) failed with {}'.format(e))

        return ''


def hash_code_object(code):
    # can't take the full object because this includes memory addresses
    # need to hash contents
    # for this use bytecode, varnames & constants
    # the list comprehension constant shows up as a code object in itself, so we have to recursively hash the constants
    ret = code.co_code + bytes(str(code.co_varnames), 'utf8') + b'('
    for c in code.co_consts:
        if isinstance(c, CodeType):
            ret += hash_code_object(c)
        elif isinstance(c, str) and c.endswith('<lambda>.<locals>.<listcomp>'):
            continue
        else:
            ret += bytes(str(c), 'utf8')
        ret += b','
    return ret + b')'


# join lines and remove stupid \\n
def remove_line_breaks(source_lines):
    """
    expressions may be defined over multiple line using \ in python. This function removes this and joins lines.
    Args:
        source_lines:

    Returns:
        joined source without \ line breaks

    """
    source = ''
    last_line_had_break = False
    for line in source_lines:
        this_line_had_break = False
        if line.endswith('\\\n'):
            line = line[:-len('\\\n')]
            this_line_had_break = True

        # remove leading whitespace if last line had break
        if last_line_had_break:
            line = line.lstrip()

        source += line
        last_line_had_break = this_line_had_break
    return source


# singleton class holding code lookup for lambdas/functions
class SourceVault:
    # borg pattern
    __shared_state = {}

    def __init__(self):
        self.__dict__ = self.__shared_state
        self.lambdaDict = {}

        # new: lookup via filename, lineno and colno
        self.lambdaFileDict = {}

    # def get(self, obj):
    #     """
    #     returns source code for given object
    #     :param codeboj:
    #     :return:
    #     """
    #     assert isinstance(obj, LambdaType), 'object needs to be a lambda object'
    #     return self.lambdaDict[hash_code_object(obj.__code__)]
    def get(self, ftor, filename, lineno, colno, globs):
        assert isinstance(ftor, LambdaType), 'object needs to be a lambda object'

        # perform multiway lookup for code
        if filename and lineno:
            key = (filename, lineno)
            entries = self.lambdaFileDict[key]

            # TODO: There's a bug here, i.e. if some function is called within a loop multiple entries get produced.
            # e.g. the question is how many "lambda" keywords are there per line
            # => if it's a single lambda keyword, return the most recent entry because globals can get overwritten
            # if i.e. a call is placed within a loop.

            if len(entries) == 1:
                return entries[0]['code']
            else:
                # patched interpreter?
                if hasattr(ftor.__code__, 'co_firstcolno'):
                    raise Exception('patched interpreter not yet implemented')
                else:
                    # multiple lambda entries. Can only search for lambda IFF no globs
                    if len(globs) != 0:
                        raise KeyError("Multiple lambdas found in {}:+{}, can't extract source code for "
                                       "lambda expression. Please either patch the interpreter or write at "
                                       "most a single lambda using global variables "
                                       "per line.".format(os.path.basename(filename), lineno))
                    # search for entry with matching hash of codeobject!
                    codeobj_hash = hash_code_object(ftor.__code__)
                    for entry in entries:
                        if entry['code_hash'] == codeobj_hash:
                            return entry['code']
                raise KeyError('Multiple lambdas found, but failed to retrieve code for this lambda expression.')
        else:
            raise KeyError('could not find lambda function')

    def extractAndPutAllLambdas(self, src_info, filename, lineno, colno, globals):
        """
        extracts the source code from all lambda functions and stores them in the source vault
        :param source:
        :return:
        """

        lines, start_lineno = src_info

        assert lineno >= start_lineno, 'line numbers sound off. please fix!'
        f_lines = lines[lineno - start_lineno:]

        take_only_first_lambda = False

        # are there two lambda's defined in this line?
        # if so, in unpatched interpreter raise exception!
        lam_count_in_target_line = f_lines[0].count('lambda')
        if lam_count_in_target_line != 1:
            if lam_count_in_target_line == 0:
                raise Exception('internal extract error, no lambda in source lines?')
            if len(globals) != 0 and not supports_lambda_closure():
                raise Exception('Found {} lambda expressions in {}:{}. Please patch your interpreter or '
                                'reformat so Tuplex can extract the source code.'.format(lam_count_in_target_line,
                                                                                         os.path.basename(filename),
                                                                                         lineno))
            else:
                if supports_lambda_closure():
                    assert colno, 'colno has to be valid'
                    # simply cut off based on col no!
                    f_lines[0] = f_lines[0][colno:]
                    take_only_first_lambda = True

        # if the first line contains only one lambda, simply the first lambda is taken.
        # else, multiple lambdas per
        if f_lines[0].count('lambda') <= 1:
            take_only_first_lambda = True

        # get the line corresponding to the object
        source = remove_line_breaks(f_lines)

        # form ast & extract all lambdas
        # need to strip leading \t
        tree = None
        # special case for line breaks (this is a bad HACK! However, don't want to write own AST parser again in python)
        try:
            tree = ast.parse(source.lstrip())
        except SyntaxError as se:
            # we could have a lambda that is broken because of \ at the end of lines
            # i.e. the source object is something like '\t\t.filter(lambda x: x * x)'
            # search till first lambda keyword
            source = source[source.find('lambda'):]

            try:
                # now another exception may be raised, i.e. when parsing fails
                tree = ast.parse(source.strip())
            except SyntaxError as se2:

                # try to parse partially till where syntax error occured.
                source_lines = source.split('\n')
                lines = source_lines[:se2.lineno]
                lines[se2.lineno - 1] = lines[se2.lineno - 1][:se2.offset - 1]
                source = '\n'.join(lines)
                tree = ast.parse(source.strip())

        Lams = extract_all_lambdas(tree)

        # take only first lambda?
        if take_only_first_lambda:
            Lams = [Lams[0]]

        # how many lambdas are there?
        # if it's a single lambda per line, super easy!
        # => can store it directly and look it up via line number!
        if len(Lams) == 1:
            lam = Lams[0]
            code = gen_code_for_lambda(lam)

            if 0 == len(code):
                raise Exception('Couldn\'t generate code again for lambda function.')

            # Note: can get colno from ast!
            colno = lam.col_offset + len(source) - len(source.lstrip())
            # => could also extract code from the string then via col_offsets etc.s
            # however, to simplify code, use astor.
            key = (filename, lineno)

            codeobj = compile(code, '<string>', 'eval')
            # hash evaluated code object's code
            codeobj_hash = hash_code_object(eval(codeobj).__code__)

            entry = {'code': code, 'code_hash': codeobj_hash,
                     'globals': globals, 'colno': colno}

            if key in self.lambdaFileDict.keys():
                # when declaration is placed within a loop, and e.g. globals are updated things might change.
                # in particular, the code + code_hash stay the same, yet the source code changes
                existing_entries = self.lambdaFileDict[key]  # how many can there be? assume 1 at most!
                updated_existing = False
                for i, existing_entry in enumerate(existing_entries):
                    if existing_entry['code'] == entry['code'] and \
                            existing_entry['code_hash'] == entry['code_hash'] and \
                            existing_entry['colno'] == entry['colno']:
                        self.lambdaFileDict[key][i] = entry  # update entry in existing file/lineno dict
                        updated_existing = True
                if not updated_existing:
                    # add new entry
                    self.lambdaFileDict[key].append(entry)
            else:
                self.lambdaFileDict[key] = [entry]
        else:
            # check that there are no globals when extracting function!
            if colno is None and len(globals) != 0:
                raise Exception('Found more than one lambda expression on {}:+{}. Either use '
                                'a patched interpreter, which supports __code__.co_firstcolno for lambda '
                                'expressions or make sure to have at most one lambda expression '
                                'on this line'.format(os.path.basename(filename), lineno))

            for lam in Lams:
                code = gen_code_for_lambda(lam)
                if 0 == len(code):
                    raise Exception('Couldn\'t generate code again for lambda function.')

                lam_colno = lam.col_offset + len(source) - len(source.lstrip())
                # => could also extract code from the string then via col_offsets etc.s
                # however, to simplify code, use astor.
                key = (filename, lineno)

                codeobj = compile(code, '<string>', 'eval')
                # hash evaluated code object's code
                codeobj_hash = hash_code_object(eval(codeobj).__code__)

                if colno is None:  # interpreter not patched
                    assert len(globals) == 0, 'this path should only be taken if there are no globs'

                    # can't associate globals clearly
                    entry = {'code': code, 'code_hash': codeobj_hash,
                             'globals': {}, 'colno': lam_colno}

                    if key in self.lambdaFileDict.keys():
                        self.lambdaFileDict[key].append(entry)
                    else:
                        self.lambdaFileDict[key] = [entry]
                else:
                    # simply add the lambda with colno & co.
                    entry = {'code': code, 'code_hash': codeobj_hash,
                             'globals': globals, 'colno': colno}

                    if key in self.lambdaFileDict.keys():
                        self.lambdaFileDict[key].append(entry)
                    else:
                        self.lambdaFileDict[key] = [entry]
