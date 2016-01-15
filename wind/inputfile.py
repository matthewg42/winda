"""
Provide input file reading functions
"""

import os
import logging
import re

log = logging

class InputFile:
    def __init__(self, path, database):
        """ 
        Construct an InputFile object for a given input file path.

        An InputFile object is supposed to be transient, intended for
        use during import of data into the database only.
        """
        log.debug('Constructing InputFile object for path: %s' % path)
        self._path = path
        self._db = database
        self._field_map = []
        self._records = []
        self.read_file()
 
    def __getitem__(self, index):
        return self._records[index]

    def read_file(self):
        with open(self._path, 'r') as f:
            try:
                self.interpret_headers(f.readline())
            except Exception as e:
                log.exception('Failed to read input file %s : %s / %s' % (
                            self._path, type(e), e))
            while True:
                line = f.readline()
                if line == '':
                    break
                else:
                    self._records.append(self.read_line(line.strip()))

    def interpret_headers(self, line):
        log.debug('InputFile.interpret_headers(%s)' % line)
        if line == '':
            raise Exception('InputFile.interpret_headers() no header found')
        field_map = self.get_field_map()
        for header_text in re.split(r'\s*,\s*', line.strip()):
            destination = None
            try:
                destination = field_map[header_text.lower()]
            except:
                log.warning('InputFile.interpret_headers() No mapping found for header "%s" ignoring this column' % header_text)
            self._field_map.append(destination)
        log.debug('InputFile.interpret_headers() field map: %s' % self._field_map)

    def get_field_map(self):
        field_map = dict()
        cur = self._db._conn.cursor()
        cur.execute("""
                    SELECT header, field, cast_type
                    FROM field_mapping
                    """)
        for rec in cur.fetchall():
            k = rec[0].lower()
            if k in field_map:
                log.warning('InputFile.get_field_map() duplicate header: %s' % k)
            field_map[k] = (rec[1], rec[2]) 
        return field_map

    def read_line(self, line):
        """
        Return an dict of values from input record

        The keys in the dict should be: ref, dt, tm, wind_1, wind_2, 
        direction, irradiance, batt_v.
        """
        log.debug('InputFile.read_line(%s)' % line)
        ret = dict()
        fields = re.split(r'\s*,\s*', line.strip())
        for i in range(len(fields)):
            try:
                if self._field_map[i] is not None:
                    dest = self._field_map[i][0]
                    cast = self._field_map[i][1]
                    ret[dest] = fields[i]
                    if cast == 'int':
                        ret[dest] = int(ret[dest])
                    elif cast == 'float':
                        ret[dest] = float(ret[dest])
                    elif cast is not None:
                        log.warning('InputFile.read_line unknown cast type: %s' % cast)
                else:
                    raise Exception('bad column %d' % i)
            except:
                log.warning('InputFile.read_line(), skipping value in column %d : %s' % (i, fields[i]))
        log.debug('InputFile.read_line() -> %s' % ret)
        return ret
