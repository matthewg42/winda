import logging
from wind.database import result_as_dict_array

log = logging

""" Class which describes data filters """
class Filter:
    def __init__(self, cursor, file_filter=None, date_filter=None, from_filter=None, to_filter=None):
        """
        Create a Filter object.

        file_filter should be a path in the input_file table
        date_filter is a datetime.date object
        from_filter is a datetime.datetime object
        to_filter is a datetime.datetime object
        """
        self.file_filter = file_filter
        self.date_filter = date_filter
        self.from_filter = from_filter
        self.to_filter = to_filter
        self.cursor = cursor
        self.cursor.execute("""CREATE TEMP TABLE tmp_event_rids (rid INT)""")
        self.cursor.execute("""INSERT INTO tmp_event_rids SELECT rowid FROM event""")
        self.cursor.execute("""CREATE TEMP TABLE tmp_raw_data_rids (rid INT)""")
        self.cursor.execute("""INSERT INTO tmp_raw_data_rids SELECT rowid FROM raw_data""")

    def __del__(self):
        self.cursor.execute("""DROP TABLE IF EXISTS tmp_event_rids""")
        self.cursor.execute("""DROP TABLE IF EXISTS tmp_raw_data_rids""")

    def get_all(self):
        return self.file_filter is None and self.date_filter is None and self.from_filter is None and self.to_filter is None

    def select_events(self):
        if self.get_all():
            self.cursor.execute("""SELECT * FROM event""")
            return result_as_dict_array(self.cursor)
        log.debug('Events selected before filtering: %d' % self.count_selected_events())
        # For each filter which is defined remove the set of records not matching the filter
        if self.file_filter is not None:
            self.cursor.execute("""
                           DELETE FROM tmp_event_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          event e,
                                             input_file i
                               WHERE         rid = e.rowid
                               AND           i.id = e.file_id
                               AND           i.path = ?
                           )
                           """, (self.file_filter,))
        log.debug('Events selected after file_filter(%s): %d' % (
            self.file_filter, self.count_selected_events()))

        if self.date_filter is not None:
            self.cursor.execute("""
                           DELETE FROM tmp_event_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          event e
                               WHERE         rid = e.rowid
                               AND           event_end LIKE ?
                           )
                           """, (self.date_filter.strftime('%Y-%m-%d%%'),))
        log.debug('Events selected after date_filter(%s): %d' % (
                    str(self.date_filter), self.count_selected_events()))

        if self.from_filter is not None and self.to_filter is not None:
            self.cursor.execute("""
                           DELETE FROM tmp_event_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          event e
                               WHERE         rid = e.rowid
                               AND           event_end >= ?
                               AND           event_end <= ?
                           )
                           """, (self.from_filter.strftime('%Y-%m-%d %T'),
                                 self.to_filter.strftime('%Y-%m-%d %T')))
        elif self.from_filter is not None and self.to_filter is None:
            self.cursor.execute("""
                           DELETE FROM tmp_event_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          event e
                               WHERE         rid = e.rowid
                               AND           event_end >= ?
                           )
                           """, (self.from_filter.strftime('%Y-%m-%d %T'),))
        elif self.from_filter is None and self.to_filter is not None:
            self.cursor.execute("""
                           DELETE FROM tmp_event_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          event e
                               WHERE         rid = e.rowid
                               AND           event_end <= ?
                           )
                           """, (self.to_filter.strftime('%Y-%m-%d %T'),))
        log.debug('Events selected after from_filter(%s) to_filter(%s): %d' % (
                    str(self.from_filter), str(self.to_filter), self.count_selected_events()))

        self.cursor.execute("""
                       SELECT        *
                       FROM          event e
                       WHERE         EXISTS (
                           SELECT       1
                           FROM         tmp_event_rids t
                           WHERE        e.rowid = t.rid
                       )
                       """)
        return result_as_dict_array(self.cursor)
            
    def count_selected_events(self):
        try:
            self.cursor.execute("""
                           SELECT       1
                           FROM         event e,
                                        tmp_event_rids t
                           WHERE        e.rowid = t.rid
                           """)
            return len(self.cursor.fetchall())
        except Exception as e:
            log.warning('Filter.count_selected_events() exception: %s / %s' % (type(e), e))
            return 0

    def select_raw_data(self):
        if self.get_all():
            self.cursor.execute("""SELECT * FROM raw_data""")
            return result_as_dict_array(self.cursor)
        log.debug('Raw Data selected before filtering: %d' % self.count_selected_raw_data())
        # For each filter which is defined remove the set of records not matching the filter
        if self.file_filter is not None:
            self.cursor.execute("""
                           DELETE FROM tmp_raw_data_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          raw_data r,
                                             input_file i
                               WHERE         rid = r.rowid
                               AND           i.id = r.file_id
                               AND           i.path = ?
                           )
                           """, (self.file_filter,))
        log.debug('Raw Data selected after file_filter(%s): %d' % (
            self.file_filter, self.count_selected_raw_data()))

        if self.date_filter is not None:
            self.cursor.execute("""
                           DELETE FROM tmp_raw_data_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          raw_data r
                               WHERE         rid = r.rowid
                               AND           dt = ?
                           )
                           """, (self.date_filter.strftime('%d-%m-%Y'),))
        log.debug('Raw Data selected after date_filter(%s): %d' % (
                    str(self.date_filter), self.count_selected_raw_data()))

        if self.from_filter is not None and self.to_filter is not None:
            self.cursor.execute("""
                           DELETE FROM tmp_raw_data_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          raw_data r
                               WHERE         rid = r.rowid
                               AND           ts >= ?
                               AND           ts <= ?
                           )
                           """, (self.from_filter.strftime('%Y-%m-%d %T'),
                                 self.to_filter.strftime('%Y-%m-%d %T')))
        elif self.from_filter is not None and self.to_filter is None:
            self.cursor.execute("""
                           DELETE FROM tmp_raw_data_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          raw_data r
                               WHERE         rid = r.rowid
                               AND           ts >= ?
                           )
                           """, (self.from_filter.strftime('%Y-%m-%d %T'),))
        elif self.from_filter is None and self.to_filter is not None:
            self.cursor.execute("""
                           DELETE FROM tmp_raw_data_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          raw_data r
                               WHERE         rid = r.rowid
                               AND           ts <= ?
                           )
                           """, (self.to_filter.strftime('%Y-%m-%d %T'),))
        log.debug('Raw Data selected after from_filter(%s) to_filter(%s): %d' % (
                    str(self.from_filter), str(self.to_filter), self.count_selected_raw_data()))

        self.cursor.execute("""
                       SELECT        *
                       FROM          raw_data r
                       WHERE         EXISTS (
                           SELECT       1
                           FROM         tmp_raw_data_rids t
                           WHERE        r.rowid = t.rid
                       )
                       """)
        return result_as_dict_array(self.cursor)

    def count_selected_raw_data(self):
        try:
            self.cursor.execute("""
                           SELECT       1
                           FROM         raw_data r,
                                        tmp_raw_data_rids t
                           WHERE        r.rowid = t.rid
                           """)
            return len(self.cursor.fetchall())
        except Exception as e:
            log.warning('Filter.count_selected_raw_data() exception: %s / %s' % (type(e), e))
            return 0



