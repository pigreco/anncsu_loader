# -*- coding: utf-8 -*-
"""ANNCSU Loader — classe principale plugin."""

import os
from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtCore import QCoreApplication


class AnncsuLoader:

    def __init__(self, iface):
        self.iface      = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.actions    = []
        self.menu       = "&ANNCSU"
        self.toolbar    = None
        self.dialog     = None

    def tr(self, message):
        return QCoreApplication.translate("AnncsuLoader", message)

    def initGui(self):
        icon_path = os.path.join(self.plugin_dir, "icon.png")
        action = QAction(
            QIcon(icon_path),
            self.tr("Carica indirizzi ANNCSU"),
            self.iface.mainWindow()
        )
        action.triggered.connect(self.run)
        action.setEnabled(True)
        action.setToolTip("ANNCSU — Carica indirizzi per Comune")

        # Toolbar dedicata (visibile in QGIS 3 e 4)
        self.toolbar = self.iface.addToolBar("ANNCSU")
        self.toolbar.setObjectName("AnncsuToolbar")
        self.toolbar.addAction(action)

        # Voce di menu Plugin
        self.iface.addPluginToMenu(self.menu, action)

        self.actions.append(action)

    def unload(self):
        for action in self.actions:
            self.iface.removePluginMenu(self.menu, action)
        if self.toolbar:
            self.toolbar.deleteLater()
            self.toolbar = None

    def run(self):
        from .dialog import AnncsuDialog
        if self.dialog is None or not self.dialog.isVisible():
            self.dialog = AnncsuDialog(self.iface)
            self.dialog.show()
        else:
            self.dialog.raise_()
            self.dialog.activateWindow()
