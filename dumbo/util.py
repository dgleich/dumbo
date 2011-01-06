# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
import re
import subprocess

from cStringIO import StringIO

def sorted(iterable, piecesize=None, key=None, reverse=False):
    if not piecesize:
        values = list(iterable)
        values.sort(key=key, reverse=reverse)
        for value in values:
            yield value
    else:  # piecewise sorted
        sequence = iter(iterable)
        while True:
            values = list(sequence.next() for i in xrange(piecesize))
            values.sort(key=key, reverse=reverse)
            for value in values:
                yield value
            if len(values) < piecesize:
                break

def incrcounter(group, counter, amount):
    print >> sys.stderr, 'reporter:counter:%s,%s,%s' % (group, counter, amount)


def setstatus(message):
    print >> sys.stderr, 'reporter:status:%s' % message
            
            
def dumpcode(outputs):
    for output in outputs:
        yield map(repr, output)


def loadcode(inputs):
    for input in inputs:
        try:
            yield map(eval, input.split('\t', 1))
        except (ValueError, TypeError):
            print >> sys.stderr, 'WARNING: skipping bad input (%s)' % input
            if os.environ.has_key('dumbo_debug'):
                raise
            incrcounter('Dumbo', 'Bad inputs', 1)


#def dumptext(outputs):
    #newoutput = []
    #for output in outputs:
        #for item in output:
            #if not hasattr(item, '__iter__'):
                #newoutput.append(str(item))
            #else:
                #newoutput.append('\t'.join(map(str, item)))
        #yield newoutput
        #del newoutput[:]

def dumptext(outputs):
    newoutput = []
    for output in outputs:
        yield [format_typedbytes(output)]
        
def format_typedbytes(output):
    out = StringIO()
    pprint = TypedBytesPrettyPrinter(out,indent=2,width=80,depth=4)
    pprint.pprint(output)
    return out.getvalue()

_commajoin = ", ".join
_id = id
_len = len
_type = type

class TypedBytesPrettyPrinter:
    def __init__(self, stream, indent=2, width=80, depth=None):
        """Handle pretty printing operations onto a stream using a set of
        configured parameters.

        indent
            Number of spaces to indent for each level of nesting.

        width
            Attempted maximum number of columns in the output.

        depth
            The maximum depth to print out nested structures.

        stream
            The desired output stream.  If omitted (or false), the standard
            output stream available at construction will be used.

        Based on pprint.PrettyPrinter
        """
        indent = int(indent)
        width = int(width)
        assert indent >= 0, "indent must be >= 0"
        assert depth is None or depth > 0, "depth must be > 0"
        assert width, "width must be != 0"
        self._depth = depth
        self._indent_per_level = indent
        self._width = width
        if stream is not None:
            self._stream = stream
        else:
            self._stream = _sys.stdout

    def pprint(self, object):
        self._format(object, self._stream, 0, 0, {}, 0)


    def _format(self, object, stream, indent, allowance, context, level):
        level = level + 1
        objid = _id(object)
        if objid in context:
            stream.write(_recursion(object))
            self._recursive = True
            self._readable = False
            return
        rep = self._repr(object, context, level - 1)
        typ = _type(object)
        sepLines = _len(rep) > (self._width - 1 - indent - allowance)
        write = stream.write

        if self._depth and level > self._depth:
            write(rep)
            return

        r = getattr(typ, "__repr__", None)
        if issubclass(typ, dict) and r is dict.__repr__:
            write('{')
            if self._indent_per_level > 1:
                write((self._indent_per_level - 1) * ' ')
            length = _len(object)
            if length:
                context[objid] = 1
                indent = indent + self._indent_per_level
                items  = object.items()
                items.sort()
                key, ent = items[0]
                rep = self._repr(key, context, level)
                write(rep)
                write(': ')
                self._format(ent, stream, indent + _len(rep) + 2,
                              allowance + 1, context, level)
                if length > 1:
                    for key, ent in items[1:]:
                        rep = self._repr(key, context, level)
                        if sepLines:
                            write(',\n%s%s: ' % (' '*indent, rep))
                        else:
                            write(', %s: ' % rep)
                        self._format(ent, stream, indent + _len(rep) + 2,
                                      allowance + 1, context, level)
                indent = indent - self._indent_per_level
                del context[objid]
            write('}')
            return

        if ((issubclass(typ, list) and r is list.__repr__) or
            (issubclass(typ, tuple) and r is tuple.__repr__)
           ):
            length = _len(object)
            if issubclass(typ, list):
                write('[')
                endchar = ']'
            else:
                write('(')
                endchar = ')'
            if self._indent_per_level > 1 and sepLines:
                write((self._indent_per_level - 1) * ' ')
            if length:
                context[objid] = 1
                indent = indent + self._indent_per_level
                self._format(object[0], stream, indent, allowance + 1,
                             context, level)
                if length > 1:
                    for ent in object[1:]:
                        if sepLines:
                            write(',\n' + ' '*indent)
                        else:
                            write(', ')
                        self._format(ent, stream, indent,
                                      allowance + 1, context, level)
                indent = indent - self._indent_per_level
                del context[objid]
            if issubclass(typ, tuple) and length == 1:
                write(',')
            write(endchar)
            return
            
            
        write(rep)

    def _repr(self, object, context, level):
        repr = self.format(object, context.copy(),
                    self._depth, level)

        return repr

    def format(self, object, context, maxlevels, level):
        """Format object for a specific context, returning a string
        and flags indicating whether the representation is 'readable'
        and whether the object represents a recursive construct.
        """
        return _safe_repr(object, context, maxlevels, level)
        
def _safe_repr(object, context, maxlevels, level):
    typ = _type(object)
    if typ is str:
        return repr(object)

    r = getattr(typ, "__repr__", None)
    if issubclass(typ, dict) and r is dict.__repr__:
        if not object:
            return "{}"
        objid = _id(object)
        if maxlevels and level >= maxlevels:
            return "{...}"
        if objid in context:
            return _recursion(object)
        context[objid] = 1
        components = []
        append = components.append
        level += 1
        saferepr = _safe_repr
        for k, v in sorted(object.items()):
            krepr = saferepr(k, context, maxlevels, level)
            vrepr = saferepr(v, context, maxlevels, level)
            append("%s: %s" % (krepr, vrepr))
        del context[objid]
        return "{%s}" % _commajoin(components)

    if (issubclass(typ, list) and r is list.__repr__) or \
       (issubclass(typ, tuple) and r is tuple.__repr__):
        if issubclass(typ, list):
            if not object:
                return "[]"
            format = "[%s]"
        elif _len(object) == 1:
            format = "(%s,)"
        else:
            if not object:
                return "()"
            format = "(%s)"
        objid = _id(object)
        if maxlevels and level >= maxlevels:
            return format % "..."
        if objid in context:
            return _recursion(object)
        context[objid] = 1
        components = []
        append = components.append
        level += 1
        for o in object:
            orepr = _safe_repr(o, context, maxlevels, level)
            append(orepr)
        del context[objid]
        return format % _commajoin(components)

    rep = repr(object)
    return rep


def _recursion(object):
    return ("<Recursion on %s with id=%s>"
            % (_type(object).__name__, _id(object)))
        


def loadtext(inputs):
    offset = 0
    for input in inputs:
        yield (offset, input)
        offset += len(input)


def parseargs(args):
    (opts, key, values) = ([], None, [])
    for arg in args:
        if arg[0] == '-' and len(arg) > 1:
            if key:
                opts.append((key, ' '.join(values)))
            (key, values) = (arg[1:], [])
        else:
            values.append(arg)
    if key:
        opts.append((key, ' '.join(values)))
    return opts


def getopts(opts, keys, delete=True):
    askedopts = dict((key, []) for key in keys)
    (key, delindexes) = (None, [])
    for (index, (key, value)) in enumerate(opts):
        key = key.lower()
        if askedopts.has_key(key):
            askedopts[key].append(value)
            delindexes.append(index)
    if delete:
        for delindex in reversed(delindexes):
            del opts[delindex]
    return askedopts


def getopt(opts, key, delete=True):
    return getopts(opts, [key], delete)[key]


def configopts(section, prog=None, opts=[]):
    from ConfigParser import SafeConfigParser, NoSectionError
    if prog:
        prog = prog.split('/')[-1]
        
        if prog.endswith('.py'): prog = prog[:-3]
 
        defaults = {'prog': prog}
    else:
        defaults = {}
    try:
        defaults.update([('user', os.environ['USER']), ('pwd',
                        os.environ['PWD'])])
    except KeyError:
        pass
    for (key, value) in opts:
        defaults[key.lower()] = value
    parser = SafeConfigParser(defaults)
    parser.read(['/etc/dumbo.conf', os.environ['HOME'] + '/.dumborc'])
    (results, excludes) = ([], set(defaults.iterkeys()))
    try:
        for (key, value) in parser.items(section):
            if not key.lower() in excludes:
                results.append((key.split('_', 1)[0], value))
    except NoSectionError:
        pass
    return results


def execute(cmd,
            opts=[],
            precmd='',
            printcmd=True,
            stdout=sys.stdout,
            stderr=sys.stderr):
    if precmd:
        cmd = ' '.join((precmd, cmd))
    args = ' '.join("-%s '%s'" % (key, value) for (key, value) in opts)
    if args:
        cmd = ' '.join((cmd, args))
    if printcmd:
        print >> stderr, 'EXEC:', cmd
    return system(cmd, stdout, stderr)


def system(cmd, stdout=sys.stdout, stderr=sys.stderr):
    if sys.version[:3] == '2.4':
        return os.system(cmd) / 256
    proc = subprocess.Popen(cmd, shell=True, stdout=stdout,
                            stderr=stderr)
    return os.waitpid(proc.pid, 0)[1] / 256


def findhadoop(optval):
    (hadoop, hadoop_shortcuts) = (optval, dict(configopts('hadoops')))
    if hadoop_shortcuts.has_key(hadoop.lower()):
        hadoop = hadoop_shortcuts[hadoop.lower()]
    if not os.path.exists(hadoop):
        print >> sys.stderr, 'ERROR: directory %s does not exist' % hadoop
        sys.exit(1)
    return hadoop


def findjar(hadoop, name):
    """Tries to find a JAR file based on given
    hadoop home directory and component base name (e.g 'streaming')"""

    jardir_candidates = filter(os.path.exists, [
        os.path.join(hadoop, 'mapred', 'build', 'contrib', name),
        os.path.join(hadoop, 'build', 'contrib', name),
        os.path.join(hadoop, 'mapred', 'contrib', name, 'lib'),
        os.path.join(hadoop, 'contrib', name, 'lib'),
        os.path.join(hadoop, 'mapred', 'contrib', name),
        os.path.join(hadoop, 'contrib', name),
        os.path.join(hadoop, 'mapred', 'contrib'),
        os.path.join(hadoop, 'contrib')
    ])
    regex = re.compile(r'hadoop.*%s.*\.jar' % name)

    for jardir in jardir_candidates:
        matches = filter(regex.match, os.listdir(jardir))
        if matches:
            return os.path.join(jardir, matches[-1])

    return None


def envdef(varname,
           files,
           optname=None,
           opts=None,
           commasep=False,
           shortcuts={},
           quote=True,
           trim=False,
           extrapaths=None):
    (pathvals, optvals) = ([], [])
    for file in files:
        if shortcuts.has_key(file.lower()):
            file = shortcuts[file.lower()]
        if file.startswith('path://'):
            pathvals.append(file[7:])
        else:
            if not '://' in file:
                if not os.path.exists(file):
                    raise ValueError('file "' + file + '" does not exist')
                file = 'file://' + os.path.abspath(file)
            if not trim:
                pathvals.append(file.split('://', 1)[1])
            else:
                pathvals.append(file.split('/')[-1])
            optvals.append(file)
    if extrapaths:
        pathvals.extend(extrapaths)
    path = ':'.join(pathvals)
    if optname and optvals:
        if not commasep:
            for optval in optvals:
                opts.append((optname, optval))
        else:
            opts.append((optname, ','.join(optvals)))
    if not quote:
        return '%s=%s' % (varname, path)
    else:
        return '%s="%s"' % (varname, ':'.join((path, '$' + varname)))


def getclassname(cls):
    return cls.__module__ + "." + cls.__name__


def loadclassname(name):
    parts = name.split('.')
    modname = '.'.join(parts[0:-1])
    clsname = parts[-1]
    mod = __import__(modname, globals(), locals(), [clsname])
    return getattr(mod, clsname)
