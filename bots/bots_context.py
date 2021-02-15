from . import botsglobal
from . import models

my_context = {}     #save vars initialised at startup

def set_context(request):
    ''' set variables in the context of templates.
    '''
    global my_context
    if not my_context:
        #most context vars are from bots.ini or database. initialise these at startup
        my_context['bots_environment_text'] = botsglobal.ini.get('webserver','environment_text','')
        my_context['bots_environment_text_color'] = botsglobal.ini.get('webserver','environment_text_color','#000000')
        my_context['botslogo'] = botsglobal.ini.get('webserver','botslogo','bots/botslogo.html')
        my_context['bots_touchscreen'] = botsglobal.ini.getboolean('webserver','bots_touchscreen',False)
        my_context['bots_mindate'] = 0 - botsglobal.ini.getint('settings','maxdays',30)
        my_context['menu_automaticretrycommunication'] = botsglobal.ini.getboolean('webserver','menu_automaticretrycommunication',False)
        my_context['menu_cleanup'] = botsglobal.ini.getboolean('webserver','menu_cleanup',False)
        my_context['menu_changepassword'] = botsglobal.ini.getboolean('webserver','menu_changepassword',True)
        #in bots.ini it is possible to add custom menus
        if botsglobal.ini.has_section('custommenus'):
            my_context['custom_menuname'] = botsglobal.ini.get('custommenus','menuname','Custom')
            my_context['custom_menus'] = [(key.title(),value) for key,value in botsglobal.ini.items('custommenus') if key != 'menuname']

    #in bots.ini can be indicated that all routes (in config->routes, if route is activated) can be run individually via menu
    if botsglobal.ini.get('webserver','menu_all_routes','') == 'notindefaultrun':
        my_context['menu_all_routes'] = list(models.routes.objects.values_list('idroute', flat=True).filter(active=True).filter(notindefaultrun=True).order_by('idroute').distinct())
    elif botsglobal.ini.getboolean('webserver','menu_all_routes',False):
        my_context['menu_all_routes'] = list(models.routes.objects.values_list('idroute', flat=True).filter(active=True).order_by('idroute').distinct())

    #bots_http_path is used in name of browser-tab;
    #eg: /admin/bots/routes/, /home, /incoming/?lastrun=0,
    #I want it to be: bots_environment_text/bots/place; eg test/bots/incoming
    # if no specific environment this would be bots/incoming.
    bots_http_path = request.get_full_path()
    my_context['bots_http_path'] = (my_context['bots_environment_text'] or 'bots') + '/' + (bots_http_path.split('/')[-2] or 'home')
    my_context['bots_minDate'] = 0 - botsglobal.ini.getint('settings','maxdays',30)


    #***variables are set now for template use, eg {{ bots_environment_text }}
    return my_context
