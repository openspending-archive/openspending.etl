from pylons import config

class Globals(object):
    """\
    Globals acts as a container for objects available throughout the
    life of the application

    One instance of Globals is created during application
    initialization and is available during requests via the
    'app_globals' variable
    """

    def __init__(self):
        self.site_title = config.get(
            'openspending.site_title',
            'OpenSpending ETL'
        )
        self.site_slogan = config.get(
            'openspending.site_slogan',
            'Extract, Transform, Load'
        )
        self.site_logo = config.get(
            'openspending.site_logo',
            '/images/datastore-logo.png'
        )
        self.wiki_link = config.get(
            'openspending.wiki_link',
            'http://wiki.openspending.org'
        )
        self.api_link = config.get(
            'openspending.api_link',
            'http://wiki.openspending.org/API'
        )
        self.lists_link = config.get(
            'openspending.lists_link',
            'http://lists.okfn.org/mailman/listinfo/openspending-discuss'
        )
        self.sandbox_mode = _parse_bool(config.get(
            'openspending.sandbox_mode',
            'False'
        ))
        self.use_celery = _parse_bool(config.get(
            'use_celery',
            'False'
        ))

def _parse_bool(s):
    return s.lower() in ("yes", "true", "on", "t", "1")
