from logging import getLogger
import imp
import importlib

LOG = getLogger(__name__)

_RACE_PROTECTION = False  # protect against shenanigans.


def autodiscover_bus_handlers():
    """Include handlers for all applications in ``INSTALLED_APPS``."""
    from django.conf import settings
    #from django.conf import settings
    global _RACE_PROTECTION

    if _RACE_PROTECTION:
        return
    _RACE_PROTECTION = True
    try:
        LOG.info("Discovering bus handlers...")
        return filter(None, [find_related_module(app, "handlers")
                             for app in settings.INSTALLED_APPS])
    finally:
        _RACE_PROTECTION = False


def find_related_module(app, related_name):
    """Given an application name and a module name, tries to find that
    module in the application."""

    try:
        app_path = importlib.import_module(app).__path__
    except AttributeError:
        return

    try:
        imp.find_module(related_name, app_path)
    except ImportError:
        return

    return importlib.import_module("%s.%s" % (app, related_name))
