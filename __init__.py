# -*- coding: utf-8 -*-
"""ANNCSU Loader — entry point plugin."""

def classFactory(iface):
    from .main import AnncsuLoader
    return AnncsuLoader(iface)
