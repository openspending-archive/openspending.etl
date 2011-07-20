"""Routes configuration

The more specific and detailed routes should be defined first so they
may take precedent over the more generic routes. For more information
refer to the routes manual at http://routes.groovie.org/docs/
"""
from pylons import config
from routes import Mapper

def make_map():
    """Create, configure and return the routes Mapper"""
    map = Mapper(directory=config['pylons.paths']['controllers'],
                 always_scan=config['debug'], explicit=True)
    map.minimization = False

    # The ErrorController route (handles 404/500 error pages); it should
    # likely stay at the top, ensuring it can always be resolved
    map.connect('/error/{action}', controller='error')
    map.connect('/error/{action}/{id}', controller='error')

    map.connect('/', controller='home', action='index')

    map.connect('/login', controller='account', action='login')
    map.connect('/register', controller='account', action='register')
    map.connect('/settings', controller='account', action='settings')
    map.connect('/after_login', controller='account', action='after_login')
    map.connect('/after_logout', controller='account', action='after_logout')

    map.connect('/sources', controller='sources', action='index')
    map.connect('/sources/_ckan_packages', controller='sources', action='ckan_packages')

    map.connect('/sources/validate/{package}/{resource}', controller='sources',
                action='validate')
    map.connect('/sources/describe/{package}/{resource}', controller='sources',
                action='describe_form', conditions=dict(method=['GET']))
    map.connect('/sources/describe/{package}/{resource}', controller='sources',
                action='describe_save', conditions=dict(method=['POST']))
    map.connect('/sources/model/{id}', controller='sources',
                action='model', conditions=dict(method=['GET']))

    map.connect('/sources/mapping/{package}/{resource}', controller='sources',
                action='mapping_form')

    map.connect('/sources/load/{package}/{resource}/{model}',
                controller='sources', action='load')
    map.connect('/sources/task/{operation}/{task_id}',
                controller='sources', action='task')


    return map
