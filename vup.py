import re
import sys
from pprint import pprint

from git import Repo


def find_version(FILENAME='setup.py'):
    with open(FILENAME, 'r') as file:
        lines = file.read()

    # lines = lines.split('\n')

    RE = re.compile(r'^[\t ]+version=[\'"](?P<version>.+)[\'"][,]*$', re.MULTILINE)

    def get_span_val(x, g):
        return x.span(g), x.group(g)

    items = [get_span_val(x, 'version') for x in RE.finditer(lines)]

    return items


def fix_version(span, new_item, FILENAME='setup.py'):
    with open(FILENAME, 'r') as file:
        lines = file.read()

    span_mi, span_ma = span

    lines = lines[:span_mi] + new_item + lines[span_ma:]

    with open(FILENAME, 'w') as file:
        file.write(lines)


def check_nv(curr_old_version, old_version, new_version):
    assert curr_old_version == old_version, (old_version, curr_old_version)
    assert new_version > old_version, (new_version, old_version)


def repo_tags_check(old_version, new_version):
    repo = Repo()

    names = set(x.name for x in repo.tags)
    assert 'v' + old_version in names
    assert 'v' + new_version not in names


def repo_diff_check():
    repo = Repo()
    hcomm = repo.head.commit

    x = hcomm.diff(None)

    assert not len(x), len(x)


def repo_add(new_version, FILENAME='setup.py', ):
    repo = Repo()
    hcomm = repo.head.commit

    diff = hcomm.diff(None, create_patch=True)

    assert len(diff) == 1

    diff_file = diff[0]

    print(diff_file.diff.decode())
    print(diff_file.score)

    assert diff_file.a_path == FILENAME, diff_file.a_path
    assert diff_file.b_path == FILENAME, diff_file.b_path

    repo.index.add([FILENAME])
    repo.index.commit('v' + new_version)
    repo.create_tag('v' + new_version)


def main(old_version, new_version):
    its = find_version()
    assert len(its) == 1

    curr_old_version_span, curr_old_version = its[0]
    check_nv(curr_old_version, old_version, new_version)

    repo_tags_check(old_version, new_version)

    repo_diff_check()

    fix_version(curr_old_version_span, new_version)

    repo_add(new_version)


main(sys.argv[1], sys.argv[2])

# class RewriteVer(ast.NodeTransformer):
#
#     def visit_keyword(self, node):
#         print(node)
#         print(dir(node), node.arg)
#         return node
#         return ast.copy_location(ast.Subscript(
#             value=ast.Name(id='data', ctx=ast.Load()),
#             slice=ast.Index(value=ast.Str(s=node.id)),
#             ctx=node.ctx
#         ), node)
#
#
# xs = RewriteVer().visit(xs)
# lines_new = astunparse.unparse(xs)
#
# for x in difflib.context_diff(lines.split('\n'), lines_new.split('\n'), FILENAME, '<generated>'):
#     print(x)
