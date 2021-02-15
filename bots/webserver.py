#!/usr/bin/env python
from __future__ import print_function
import sys
import os
try:        #newer cherrypy versions have cheroot as seperate web-server. But bots still needs cherrypy to server static files.
    from cheroot.wsgi import WSGIServer as wsgiserver
    from cheroot.wsgi import PathInfoDispatcher as infodispatcher
    from cheroot.server import get_ssl_adapter_class as get_ssl_adapter_class
except ImportError:
    from cherrypy.wsgiserver import CherryPyWSGIServer as wsgiserver
    from cherrypy.wsgiserver import WSGIPathInfoDispatcher as infodispatcher
    from cherrypy.wsgiserver import get_ssl_adapter_class as get_ssl_adapter_class
import cherrypy
from . import botsglobal
from . import botsinit


def start():
    #NOTE: bots directory should always be on PYTHONPATH - otherwise it will not start.
    #***command line arguments**************************
    usage = '''
    This is "%(name)s" version %(version)s, part of Bots open source edi translator (http://bots.sourceforge.net).
    The %(name)s is the web server for bots; the interface (bots-monitor) can be accessed in a
    browser, eg 'http://localhost:8080'.
    Usage:
        %(name)s  -c<directory>
    Options:
        -c<directory>   directory for configuration files (default: config).

    '''%{'name':os.path.basename(sys.argv[0]),'version':botsglobal.version}
    configdir = 'config'
    for arg in sys.argv[1:]:
        if arg.startswith('-c'):
            configdir = arg[2:]
            if not configdir:
                print('Error: configuration directory indicated, but no directory name.')
                sys.exit(1)
        else:
            print(usage)
            sys.exit(0)
    #***end handling command line arguments**************************
    botsinit.generalinit(configdir)     #find locating of bots, configfiles, init paths etc.
    process_name = 'webserver'
    botsglobal.logger = botsinit.initserverlogging(process_name)    #initialise file-logging for web-server. This logging only contains the logging from bots-webserver, not from cherrypy.
    #~ os.environ['DJANGO_SETTINGS_MODULE'] = importnameforsettings
    from django.core.handlers.wsgi import WSGIHandler

    #***init cherrypy as webserver*********************************************
    #global configuration for cherrypy
    cherrypy.config.update({'global': {'log.screen': botsglobal.ini.get('webserver','log_console_level','') == 'DEBUG'}}) # log to screen only for DEBUG level
    #setup handling of serving static files via cherrypy's tools
    static_config = {'/': {'tools.staticdir.on' : True,'tools.staticdir.dir' : 'media' ,'tools.staticdir.root': botsglobal.ini.get('directories','botspath')}}
    staticfile_app = cherrypy.tree.mount(None, '/media', static_config)    #None: no cherrypy application (as this only serves static files)
    #setup django as WSGI application.
    django_app = WSGIHandler()
    #indicate to cherrypy which apps are used
    dispatcher = infodispatcher({'/': django_app, '/media': staticfile_app})    #UNICODE PROBLEM: needs to be binary!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                                                                    #else get error: objname = name.translate(self.translate) typeerror: character mapping must return integer, none or unicode
    botswebserver = wsgiserver(bind_addr=('0.0.0.0', botsglobal.ini.getint('webserver','port',8080)), wsgi_app=dispatcher, server_name=botsglobal.ini.get('webserver','name','bots-webserver'))
    botsglobal.logger.log(25,'Bots %(process_name)s started.',
                                {'process_name':process_name})
    botsglobal.logger.log(25,'Bots %(process_name)s configdir: "%(configdir)s".',
                                {'process_name':process_name, 'configdir':botsglobal.ini.get('directories','config')})
    botsglobal.logger.log(25,'Bots %(process_name)s serving at port: "%(port)s".',
                                {'process_name':process_name,'port':botsglobal.ini.getint('webserver','port',8080)})
    #handle ssl: cherrypy < 3.2 always uses pyOpenssl. cherrypy >= 3.2 uses python ssl.
    ssl_certificate = botsglobal.ini.get('webserver','ssl_certificate',None)
    ssl_private_key = botsglobal.ini.get('webserver','ssl_private_key',None)
    if ssl_private_key:
        if cherrypy.__version__ >= '3.2.0':
            adapter_class = get_ssl_adapter_class('builtin')
            botswebserver.ssl_adapter = adapter_class(ssl_certificate,ssl_private_key)
        else:
            #but: pyOpenssl should be there!
            botswebserver.ssl_certificate = ssl_certificate
            botswebserver.ssl_private_key = ssl_private_key
        botsglobal.logger.log(25,'Bots %(process_name)s uses ssl (https).',{'process_name':process_name})
    else:
        botsglobal.logger.log(25,'Bots %(process_name)s uses plain http (no ssl).',{'process_name':process_name})

    #***start the cherrypy webserver.************************************************
    try:
        botswebserver.start()
    except KeyboardInterrupt:
        botswebserver.stop()
    else:
        botswebserver.stop()


if __name__ == '__main__':
    start()
