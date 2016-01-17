import logging
from wind.database import result_as_dict_array

log = logging

""" Class which describes data filters """
class Filter:
    def __init__(self, file_filter=None, date_filter=None, from_filter=None, to_filter=None):
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

    def get_all(self):
        return self.file_filter is None and self.date_filter is None and self.from_filter is None and self.to_filter is None

    def select_events(self, cursor):
        if self.get_all():
            cursor.execute("""SELECT * FROM event""")
            return result_as_dict_array(cursor)
        cursor.execute("""DROP TABLE IF EXISTS tmp_event_rids""")
        cursor.execute("""CREATE TEMP TABLE tmp_event_rids (rid INT)""")
        cursor.execute("""INSERT INTO tmp_event_rids SELECT rowid FROM event""")
        log.debug('Events selected before filtering: %d' % self.count_selected_events(cursor))
        # For each filter which is defined remove the set of records not matching the filter
        if self.file_filter is not None:
            cursor.execute("""
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
            self.file_filter, self.count_selected_events(cursor)))

        if self.date_filter is not None:
            cursor.execute("""
                           DELETE FROM tmp_event_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          event e
                               WHERE         rid = e.rowid
                               AND           event_end LIKE ?
                           )
                           """, (self.date_filter.strftime('%Y-%m-%d%%'),))
        log.debug('Events selected after date_filter(%s): %d' % (
                    str(self.date_filter), self.count_selected_events(cursor)))

        if self.from_filter is not None and self.to_filter is not None:
            cursor.execute("""
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
            cursor.execute("""
                           DELETE FROM tmp_event_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          event e
                               WHERE         rid = e.rowid
                               AND           event_end >= ?
                           )
                           """, (self.from_filter.strftime('%Y-%m-%d %T'),))
        elif self.from_filter is None and self.to_filter is not None:
            cursor.execute("""
                           DELETE FROM tmp_event_rids
                           WHERE NOT EXISTS (
                               SELECT        1
                               FROM          event e
                               WHERE         rid = e.rowid
                               AND           event_end <= ?
                           )
                           """, (self.to_filter.strftime('%Y-%m-%d %T'),))
        log.debug('Events selected after from_filter(%s) to_filter(%s): %d' % (
                    str(self.from_filter), str(self.to_filter), self.count_selected_events(cursor)))

        cursor.execute("""
                       SELECT        *
                       FROM          event e
                       WHERE         EXISTS (
                           SELECT       1
                           FROM         tmp_event_rids t
                           WHERE        e.rowid = t.rid
                       )
                       """)
        return result_as_dict_array(cursor)
            
    def count_selected_events(self, cursor):
        try:
            cursor.execute("""
                           SELECT       1
                           FROM         event e,
                                        tmp_event_rids t
                           WHERE        e.rowid = t.rid
                           """)
            return len(cursor.fetchall())
        except Exception as e:
            log.warning('Filter.count_selected_events() exception: %s / %s' % (type(e), e))
            return 0

    def select_raw_data(self, cursor):
        if self.get_all():
            cursor.execute("""SELECT * FROM raw_data""")
            return cursor.fetchall()
        return []



