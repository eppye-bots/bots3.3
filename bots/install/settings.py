# Django settings for bots project.
import os
import bots
PROJECT_PATH = os.path.abspath(os.path.dirname(bots.__file__))

#*******settings for sending bots error reports via email**********************************
MANAGERS = (    #bots will send error reports to the MANAGERS
    ('name_manager', 'adress@test.com'),
    )
EMAIL_HOST = 'localhost'             #Default: 'localhost'
EMAIL_PORT = '25'             #Default: 25
EMAIL_USE_TLS = False       #Default: False
EMAIL_HOST_USER = ''        #Default: ''. Username to use for the SMTP server defined in EMAIL_HOST. If empty, Django won't attempt authentication.
EMAIL_HOST_PASSWORD = ''    #Default: ''. PASSWORD to use for the SMTP server defined in EMAIL_HOST. If empty, Django won't attempt authentication.
#~ SERVER_EMAIL = 'user@gmail.com'           #Sender of bots error reports. Default: 'root@localhost'
#~ EMAIL_SUBJECT_PREFIX = ''   #This is prepended on email subject.

#*********database settings*************************
#SQLite database (default bots database)
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(PROJECT_PATH, 'botssys/sqlitedb/botsdb'),
        'USER': '',         #not needed for SQLite
        'PASSWORD': '',     #not needed for SQLite
        'HOST': '',         #not needed for SQLite
        'PORT': '',         #not needed for SQLite
        'OPTIONS': {},      #not needed for SQLite
        }
    }
#MySQL:
#~ DATABASES = {
    #~ 'default': {
        #~ 'ENGINE': 'django.db.backends.mysql',
        #~ 'NAME': 'botsdb',
        #~ 'USER': 'bots',
        #~ 'PASSWORD': 'botsbots',
        #~ 'HOST': '127.0.0.1',
        #~ 'PORT': '3306',
        #~ 'OPTIONS': {'use_unicode':True,'charset':'utf8','init_command': 'SET storage_engine=INNODB'},
        #~ }
    #~ }
#~ #PostgreSQL:
#~ DATABASES = {
    #~ 'default': {
        #~ 'ENGINE': 'django.db.backends.postgresql_psycopg2',
        #~ 'NAME': 'botsdb',
        #~ 'USER': 'bots',
        #~ 'PASSWORD': 'botsbots',
        #~ 'HOST': '127.0.0.1',
        #~ 'PORT': '5432',
        #~ 'OPTIONS': {},
        #~ }
    #~ }

#*********setting date/time zone and formats *************************
# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'Europe/Amsterdam'

#~ *********language code/internationalization*************************
# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'
#~ LANGUAGE_CODE = 'nl'
USE_I18N = True

#*************************************************************************
#*********other django setting. please consult django docs.***************
#*************************************************************************
#*************************************************************************

#*********path settings*************************
STATIC_URL = '/media/'
STATIC_ROOT = PROJECT_PATH + '/'
ROOT_URLCONF = 'bots.urls'
LOGIN_URL = '/login/'
LOGIN_REDIRECT_URL = '/home'
LOGOUT_URL = '/logout/'
#~ LOGOUT_REDIRECT_URL = #not such parameter; is set in urls.py
ALLOWED_HOSTS = ['*']

#*********sessions, cookies, log out time*************************
SESSION_EXPIRE_AT_BROWSER_CLOSE = True      #True: always log in when browser is closed
SESSION_COOKIE_AGE = 3600                   #seconds a user needs to login when no activity
SESSION_SAVE_EVERY_REQUEST = True           #if True: SESSION_COOKIE_AGE is interpreted as: since last activity
SESSION_COOKIE_NAME = 'Bots-0001'           #make this unique for each environment if you need to log in to multiple at the same time

#set in bots.ini
#~ DEBUG = True
SITE_ID = 1
# Make this unique, and don't share it with anybody.
SECRET_KEY = 'm@-u37qiujmeqfbu$daaaaz)sp^7an4u@h=wfx9dd$$$zl2i*x9#awojdc'

#*******template handling and finding*************************************************************************
# new TEMPLATES structure for Django 1.8+
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(PROJECT_PATH, 'templates'),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.contrib.auth.context_processors.auth', 
                'django.template.context_processors.debug',
                'django.template.context_processors.i18n',
                'django.template.context_processors.request',
                'django.contrib.messages.context_processors.messages',
                'django.template.context_processors.static',
                'bots.bots_context.set_context',
            ],
        },
    },
]
#*******includes for django*************************************************************************
LOCALE_PATHS = (
    os.path.join(PROJECT_PATH, 'locale'),
    )
#save uploaded file (=plugin) always to file. no path for temp storage is used, so system default is used.
FILE_UPLOAD_HANDLERS = (
    'django.core.files.uploadhandler.TemporaryFileUploadHandler',
    )
MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    )
INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.admin',
    'django.contrib.messages',
    'bots',
    )
