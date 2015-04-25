#!/usr/bin/python3
#
#   This program parses the google books ngram dataset and adds it to a
#   sqlite database.
#
#   Copyright 2015 Peter M. Jones
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import sys
import gzip
import sqlite3

from itertools import accumulate, repeat

class AlreadyCreated(RuntimeError):
    pass

class DBBaseObject:
    creates = []
    data = []
    _insert = None
    tablename = None
    _instantiate = False
    _parents = []

    def __init__(self, db=None):
        self._db = db

    @property
    def db(self):
        return self._db

    def insert_parents(self):
        for p in self._parents:
            objs = getattr(self, p)
            try:
                for o in objs:
                    o.insert()
            except TypeError:
                objs.insert()

    def insert(self):
        self.insert_parents()
        #print("%s.insert()" % (repr(self),))
        #print("%s.data: %s" % (repr(self), self.data))
        #for x in self.data:
        #    print("%s '%s'" % (self._insert, x))
        self.db.executemany(self._insert, self.data)

class DBObject(DBBaseObject):
    _keys = []
    _value = None

    def __init__(self, db=None):
        super(DBObject, self).__init__(db=db)
        self._pk = None

    @property
    def keys(self):
        for k in self._keys:
            yield k

    @property
    def values(self):
        return [self._values]

    @property
    def pk(self):
        if self._pk is None:
            select = "select id from %s where" % (self.table,)
            where = " and ".join(["%s=?" % (k,) for k in self.keys])
            select = "%s %s" % (select, where)
            cu = self._db.execute(select, self.values)
            value = cu.fetchone()
            if value is not None:
                self._pk = value[0]
        return self._pk

class DBMapObject(DBBaseObject):
    def __init__(self, db):
        super(DBMapObject, self).__init__(db=db)

# word: [[:alnum:]_.']+
# type: _ADJ _ADP _ADV _CONJ _DET _NOUN _NUM _PRON _PRT _VERB
# 0gram: word type | word
# 1gram: 0gram TAB year TAB matches TAB volumes NEWLINE
# 2gram: 1gram SPACE 1gram TAB matches TAB volumes NEWLINE

class ZeroGramTypeStateholder(DBObject):
    _shared_state = {}
    def __init__(self, name, *args, **kwargs):
        try:
            self.__dict__ = self._shared_state[name]
        except KeyError:
            self.__dict__ = {}
            self._shared_state[name] = self.__dict__
            if kwargs is None:
                kwargs = {}
            try:
                del kwargs['name']
            except KeyError:
                pass
            super(ZeroGramTypeStateholder, self).__init__(*args, **kwargs)

class ZeroGramType(ZeroGramTypeStateholder):
    init_data = tuple(["_ADJ", "_ADP", "_ADV", "_CONJ", "_DET", \
                    "_NOUN", "_NUM", "_PRON", "_PRT", "_VERB", ""])
    _keys = ["name"]
    creates =["create table if not exists ZeroGramType ("\
                "id integer primary key,"\
                "name text"\
                ");",
               "create unique index if not exists zgt_name on "\
                "ZeroGramType(name);",
               "create unique index if not exists zgt_id on "\
                "ZeroGramType(id);"]
    _insert = "insert or ignore into ZeroGramType(name) values (?)"
    table = "ZeroGramType"
    _instantiate = True

    def __init__(self, name, db=None):
        super(ZeroGramType, self).__init__(name=name, db=db)
        if name.endswith('_'):
            name = name[:-1]
        elif not name in ZeroGramType.init_data:
            raise ValueError(name)
        self.name = name

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        if self.name is None:
            return ""
        return self.name

    def __repr__(self):
        return self.name.__repr__()

    @property
    def values(self):
        return [self.name]

class ZeroGramStateholder(DBObject):
    _shared_state = {}
    def __init__(self, name, *args, **kwargs):
        try:
            self.__dict__ = self._shared_state[name]
        except KeyError:
            self.__dict__ = {}
            self._shared_state[name] = self.__dict__
            if kwargs is None:
                kwargs = {}
            try:
                del kwargs['name']
            except KeyError:
                pass
            super(ZeroGramStateholder, self).__init__(*args, **kwargs)

class ZeroGram(ZeroGramStateholder):
    creates = ["create table if not exists ZeroGram ("\
                    "id integer primary key,"\
                    "word text,"\
                    "type num,"\
                    "foreign key (type) references ZeroGramType(id)"\
                ");",]
    _parents = ["type"]
    keys = ["word", "type"]
    _insert = "insert into ZeroGram(word, type) values (?, ?)"
    table = "ZeroGram"

    def __init__(self, text, db=None):
        super(ZeroGram, self).__init__(name=text, db=db)
        self._type = None
        self._word = None
        for t in ZeroGramType.init_data:
            if text.endswith('_'):
                t += '_'
            if text.endswith(t):
                wl = len(text)
                tl = len(t)
                self._word = text[:(wl-tl)]
                self._type = ZeroGramType(text[wl:], db=self._db)
        if self._word is None:
            self._word = text

    @property
    def values(self):
        return [self.word, self.type.pk]

    @property
    def data(self):
        yield [self.word, self.type.pk]

    @property
    def type(self):
        return self._type

    @property
    def word(self):
        return self._word

    def __lt__(self, other):
        if self.word != other.word:
            r = self.word < other.word
            return r
        else:
            r = self.type < other.type
            return r

    def __eq__(self, other):
        if self.word != other.word:
            return False
        return self.type == other.type

    def __repr__(self):
        return "ZeroGram(\"%s\")" % (self.word,)

    def __str__(self):
        return "%s%s" % (self.word, self.type)

    def __hash__(self):
        return hash(str(self))

    def insert(self, *args, **kwargs):
        return super(ZeroGram, self).insert(*args, **kwargs)

class SkipIt(ValueError):
    pass

class NGramMap(DBMapObject):
    creates = ["create table if not exists NGramMap ("\
                    "ngram num,"\
                    "zerogram num,"\
                    "position num,"\
                    "foreign key (ngram) references NGram(id),"\
                    "foreign key (zerogram) references ZeroGram(id)"\
                ");",]
    _insert = "insert into NGramMap(position, zerogram, ngram) values (?, ?, ?)"
    #_parents = ["ngram"]
    table = "NGramMap"

    def __init__(self, ngram, db=None):
        super(NGramMap, self).__init__(db=db)
        self.ngram = [ngram]

    @property
    def data(self):
        ngram = self.ngram[0].pk
        zerograms = [x.pk for x in self.ngram[0].words]
        return [[x]+list(y) for x,y in enumerate(zip(zerograms, repeat(ngram)))]

class NGramStateholder(DBObject):
    _shared_state = {}
    def __init__(self, name, *args, **kwargs):
        try:
            self.__dict__ = self._shared_state[name]
        except KeyError:
            self.__dict__ = {}
            self._shared_state[name] = self.__dict__
            if kwargs is None:
                kwargs = {}
            try:
                del kwargs['name']
            except KeyError:
                pass
            super(NGramStateholder, self).__init__(*args, **kwargs)

class NGram(NGramStateholder):
    creates = ["create table if not exists NGram ("\
                    "id integer primary key,"\
                    "text text,"\
                    "words num,"\
                    "matches num,"\
                    "volumes num"\
                ");",
                "create unique index if not exists ng_text on NGram(text);",
                "create unique index if not exists ng_id on NGram(id);"]
    table = "NGram"
    _keys = ["text"]
    _insert = "insert into NGram(text, words, matches, volumes) values (?, ?, ?, ?);"
    _parents = ["words", ]

    _year_limit = 1900

    def __init__(self, words, year, matches, volumes, db=None):
        super(NGram, self).__init__(name=words, db=db)
        self.text = words
        year = int(year)
        if year < self._year_limit:
            raise SkipIt
        self._years = set([year,])

        self._matches = int(matches)
        self._volumes = int(volumes)

        self._words = []
        for text in words.split(' '):
            self._words.append(ZeroGram(text, db=self._db))

    @property
    def values(self):
        return [self.text]

    @property
    def data(self):
        yield [self.text, len(self.words), self.matches, self.volumes]

    @property
    def subs(self):
        return len(self.words)

    @property
    def words(self):
        return self._words

    @property
    def years(self):
        return self._years

    @property
    def matches(self):
        return self._matches

    @property
    def volumes(self):
        return self._volumes

    def insert(self):
        #print("inserting ngram %s" % (self.words,))
        super(NGram, self).insert()
        ngm = NGramMap(self, db=self.db)
        ngm.insert()

    def __add__(self, other):
        self._years = self._years.union(other._years)
        self._matches += other._matches
        self._volumes += other._volumes
        return self

    def __lt__(self, other):
        if self.matches == other.matches:
            return self.volumes < other.volumes
        return self.matches < other.matches
        lw = list(self.words)
        rw = list(other.words)
        ll = len(lw)
        rl = len(rw)
        total = max(ll,rl)
        if ll < total:
            lw += [None] * (total-ll)
        if rl < total:
            rw += [None] * (total-rl)

        r = False
        equal = True
        for x in range(0, total):
            if lw[x] is None:
                r = True
                equal = False
                break

            if rw[x] is None:
                equal = False
                break

            if lw[x] < rw[x]:
                r = True
                equal = False
                break

            if lw[x] > rw[x]:
                r = False
                equal = False
                break

        if equal:
            if self.matches != other.matches:
                r = self.matches < other.matches
            elif self.volumes != other.volumes:
                r = self.volumes < other.volumes

        return r

    def __eq__(self, other):
        r = self.matches == other.matches
        if r:
            return self.volumes == other.volumes
        return r

        lw = list(self.words)
        rw = list(other.words)
        ll = len(lw)
        rl = len(rw)
        total = max(ll,rl)
        if ll < total:
            lw += [None] * (total-ll)
        if rl < total:
            rw += [None] * (total-rl)

        r = True
        for x in range(0, total):
            if lw[x] != rw[x]:
                r = False
                break
        return r

    def __hash__(self):
        s = str(self)
        return hash(s)

    def __repr__(self):
        words = [repr(word) for word in self.words]
        words = ','.join(words)
        s = "NGram([%s], %s, %d, %d)" % (words, self.years, \
            self.matches, self.volumes)
        return s

    def __str__(self):
        words = [str(word) for word in self.words]
        words = ' '.join(words)
        return words

class Database(object):
    _types = [ZeroGramType, ZeroGram, NGram, NGramMap]

    def __init__(self, pool, file):
        self._db = sqlite3.connect(file)
        self.genesis(pool)

    def __del__(self):
        self._db.close()

    @property
    def db(self):
        return self._db

    @property
    def types(self):
        return self._types

    def genesis(self, pool):
        for t in self.types:
            for create in t.creates:
                self.execute(create)
        self.commit()
        for t in self.types:
            if t._instantiate:
                self.executemany(t._insert, [[d] for d in t.init_data])
                for d in t.data:
                    o = t(d, db=self.db)
        self.commit()

    def add(self, ngram):
        pass

    def execute(self, *args, **kwargs):
        return self.db.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self.db.executemany(*args, **kwargs)

    def commit(self, *args, **kwargs):
        return self.db.commit(*args, **kwargs)

    def fetchone(self, *args, **kwargs):
        return self.db.fetchone(*args, **kwargs)

    def fetchmany(self, *args, **kwargs):
        return self.db.fetchmany(*args, **kwargs)

    def fetchall(self, *args, **kwargs):
        return self.db.fetchall(*args, **kwargs)

    def commit(self, *args, **kwargs):
        return self.db.commit(*args, **kwargs)

def usage(exitcode, output):
    output.write("usage: db <output0> [... <outputN>]\n")
    sys.exit(exitcode)

if __name__ == '__main__':
    for x in sys.argv:
        if x == "--help" or x == "-?" or x == "-h":
            usage(0, sys.stdout)

    if len(sys.argv) < 2:
        usage(1, sys.stderr)

    x = 0
    y = 0
    prev_y = 0
    modorama=107
    modoramarama=1
    for filename in sys.argv[1:]:
        pool = set()
        db = Database(pool, "words.db")
        print("\rReading input from \"%s\"" % (filename,), file=sys.stderr)
        f = gzip.open(filename, "rt", newline='\n', encoding='utf-8')
        prev = None
        while True:
            line = f.readline().strip()
            if len(line) == 0:
                break
            items = line.split('\t')
            try:
                ngram = NGram(db=db, *items)
            except SkipIt:
                continue
            except:
                print("line: \"%s\"" % (line,))
                raise
            #print("prev: %s ngram: %s" % (prev, ngram))
            #print("hash prev: %s ngram: %s" % (hash(prev), hash(ngram)))
            if prev and hash(prev) == hash(ngram):
                new = prev+ngram
                prev = new
                x+=1
            else:
                prev = ngram
                pool.add(ngram)
                y+=1
            if y%modorama==0: print("\r%d/%d" % (y,x), file=sys.stderr, end='')
            modorama += modoramarama * 5
            modoramarama *= -1
        print("\r%d/%d" % (y,x), file=sys.stderr, end='')
        print("\nAdding to DB", file=sys.stderr)
        x = prev_y
        for n in pool:
            if x%modorama==0:
                print("\r%d/%d" % (y,x), file=sys.stderr, end='')
                db.commit()
            modorama += modoramarama * 5
            modoramarama *= -1
            x+=1
            if n.pk is None:
                n.insert()
        print("\r%d/%d" % (y,x), file=sys.stderr, end='')
        print("", file=sys.stderr)
        prev_y = y
        db.commit()

    #t = list(pool)
    #l = [str(x) for x in t]
    #pprint(l)
    #t.sort()
    #[print("%s %d" % (x, x.matches)) for x in t]
    #t.reverse()
    #l = [str(x) for x in t]
    #pprint(l)
