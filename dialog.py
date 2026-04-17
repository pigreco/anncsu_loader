# -*- coding: utf-8 -*-
"""ANNCSU Loader — dialogo principale (Qt6 / QGIS 4 compatibile)."""

import os

from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout,
    QPushButton, QLabel, QLineEdit, QListWidget,
    QListWidgetItem, QFileDialog, QProgressBar,
    QComboBox, QGroupBox, QAbstractItemView,
    QMessageBox, QCheckBox, QTabWidget, QWidget,
    QTableWidget, QTableWidgetItem, QHeaderView,
    QSpinBox, QFrame,
)
from qgis.PyQt.QtCore import Qt, QSettings
from qgis.PyQt.QtGui import QFont, QColor

from qgis.core import (
    Qgis,
    QgsVectorLayer, QgsProject, QgsPointXY,
    QgsGeometry, QgsFeature,
    QgsWkbTypes, QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
)
from qgis.gui import QgsVertexMarker, QgsRubberBand


# ── helper enum Qt5/Qt6 ───────────────────────────────────────────────────────

def _multi_selection():
    try:
        return QAbstractItemView.SelectionMode.MultiSelection
    except AttributeError:
        return QAbstractItemView.MultiSelection

def _user_role():
    try:
        return Qt.ItemDataRole.UserRole
    except AttributeError:
        return Qt.UserRole

try:
    _HEADER_STRETCH   = QHeaderView.ResizeMode.Stretch
    _SELECT_ROWS      = QAbstractItemView.SelectionBehavior.SelectRows
    _NO_EDIT_TRIGGERS = QAbstractItemView.EditTrigger.NoEditTriggers
    _MSGBOX_YES       = QMessageBox.StandardButton.Yes
    _MSGBOX_NO        = QMessageBox.StandardButton.No
    _TEXT_RICH        = Qt.TextFormat.RichText
    _FRAME_HLINE      = QFrame.Shape.HLine
    _FRAME_SUNKEN     = QFrame.Shadow.Sunken
except AttributeError:
    _HEADER_STRETCH   = QHeaderView.Stretch
    _SELECT_ROWS      = QAbstractItemView.SelectRows
    _NO_EDIT_TRIGGERS = QAbstractItemView.NoEditTriggers
    _MSGBOX_YES       = QMessageBox.Yes
    _MSGBOX_NO        = QMessageBox.No
    _TEXT_RICH        = Qt.RichText
    _FRAME_HLINE      = QFrame.HLine
    _FRAME_SUNKEN     = QFrame.Sunken


# ─────────────────────────────────────────────────────────────────────────────
# DIALOGO
# ─────────────────────────────────────────────────────────────────────────────

class AnncsuDialog(QDialog):

    SETTINGS_KEY_PARQUET    = "anncsu_loader/parquet_path"
    SETTINGS_KEY_OUTPUT_DIR = "anncsu_loader/output_dir"
    ANNCSU_URL = "https://media.githubusercontent.com/media/quattochiacchiereinquattro/anncus/main/data/anncsu-indirizzi.parquet"
    ISTAT_URL  = "https://media.githubusercontent.com/media/quattochiacchiereinquattro/anncus/main/data/istat-boundaries.parquet"

    def __init__(self, iface, parent=None):
        super().__init__(parent or iface.mainWindow())
        self.iface         = iface
        self.worker        = None
        self.settings      = QSettings()
        self._comuni_cache = []
        self._marker       = None   # QgsVertexMarker sulla mappa
        self._download_istat_pending = False

        self.setWindowTitle("ANNCSU — Loader")
        self.setMinimumWidth(560)
        self.setMinimumHeight(700)
        self._build_ui()
        self._ripristina_parquet()

    # =========================================================================
    # BUILD UI
    # =========================================================================

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setSpacing(8)

        # ── SEZIONE: file Parquet (comune a tutti i tab) ──────────────────
        grp_file = QGroupBox("File Parquet ANNCSU")
        lay_file = QHBoxLayout(grp_file)

        self.txt_parquet = QLineEdit()
        self.txt_parquet.setPlaceholderText("Seleziona o scarica il file anncsu-indirizzi.parquet...")
        self.txt_parquet.setReadOnly(True)

        self.btn_sfoglia = QPushButton("Sfoglia…")
        self.btn_sfoglia.setFixedWidth(80)
        self.btn_sfoglia.clicked.connect(self._scegli_parquet)

        self.btn_carica_comuni = QPushButton("Carica comuni")
        self.btn_carica_comuni.setFixedWidth(110)
        self.btn_carica_comuni.setEnabled(False)
        self.btn_carica_comuni.clicked.connect(self._avvia_lettura_comuni)

        lay_file.addWidget(self.txt_parquet)
        lay_file.addWidget(self.btn_sfoglia)
        lay_file.addWidget(self.btn_carica_comuni)
        root.addWidget(grp_file)

        # ── TAB WIDGET ────────────────────────────────────────────────────
        self.tabs = QTabWidget()
        root.addWidget(self.tabs)

        self.tabs.addTab(self._tab_scarica(),   "⬇  Scarica")
        self.tabs.addTab(self._tab_esporta(),   "💾  Esporta per Comune")
        self.tabs.addTab(self._tab_cerca(),     "🔍  Cerca Indirizzo")

        # ── barra avanzamento (globale) ───────────────────────────────────
        grp_prog = QGroupBox("Avanzamento")
        lay_prog = QVBoxLayout(grp_prog)

        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        lay_prog.addWidget(self.progress_bar)

        self.lbl_stato = QLabel("In attesa...")
        self.lbl_stato.setStyleSheet("font-size: 11px;")
        lay_prog.addWidget(self.lbl_stato)
        root.addWidget(grp_prog)

        # ── pulsanti globali ──────────────────────────────────────────────
        hl_btn = QHBoxLayout()
        self.btn_annulla = QPushButton("✖  Annulla")
        self.btn_chiudi  = QPushButton("Chiudi")
        self.btn_annulla.setEnabled(False)
        self.btn_annulla.clicked.connect(self._annulla)
        self.btn_chiudi.clicked.connect(self.close)
        hl_btn.addStretch()
        hl_btn.addWidget(self.btn_annulla)
        hl_btn.addWidget(self.btn_chiudi)
        root.addLayout(hl_btn)

    # =========================================================================
    # TAB: SCARICA
    # =========================================================================

    def _tab_scarica(self) -> QWidget:
        w   = QWidget()
        lay = QVBoxLayout(w)
        lay.setSpacing(10)

        lbl_info = QLabel(
            "Scarica il file Parquet ANNCSU completo dal repository ufficiale.\n"
            f"URL: {self.ANNCSU_URL}"
        )
        lbl_info.setWordWrap(True)
        lbl_info.setStyleSheet("color: gray; font-size: 11px;")
        lay.addWidget(lbl_info)

        # cartella di destinazione
        grp_dest = QGroupBox("Cartella di destinazione")
        lay_dest = QHBoxLayout(grp_dest)

        self.txt_download_dir = QLineEdit()
        self.txt_download_dir.setPlaceholderText("Scegli cartella...")
        self.txt_download_dir.setReadOnly(True)
        # ripristina ultima cartella
        ultima_dir = self.settings.value(self.SETTINGS_KEY_OUTPUT_DIR, "")
        if ultima_dir:
            self.txt_download_dir.setText(ultima_dir)

        btn_scegli_dir = QPushButton("Sfoglia…")
        btn_scegli_dir.setFixedWidth(80)
        btn_scegli_dir.clicked.connect(self._scegli_dir_download)

        lay_dest.addWidget(self.txt_download_dir)
        lay_dest.addWidget(btn_scegli_dir)
        lay.addWidget(grp_dest)

        # nome file
        form = QFormLayout()
        self.txt_nome_file = QLineEdit("anncsu-indirizzi.parquet")
        form.addRow("Nome file:", self.txt_nome_file)
        lay.addLayout(form)

        self.chk_scarica_istat = QCheckBox(
            "Scarica anche i confini ISTAT dei comuni (istat-boundaries.parquet)"
        )
        self.chk_scarica_istat.setToolTip(
            f"Scarica in aggiunta il file dei confini amministrativi comunali\n{self.ISTAT_URL}"
        )
        lay.addWidget(self.chk_scarica_istat)

        self.lbl_download_path = QLabel("")
        self.lbl_download_path.setStyleSheet("font-size: 11px; color: palette(highlight);")
        self.lbl_download_path.setWordWrap(True)
        lay.addWidget(self.lbl_download_path)

        self.txt_download_dir.textChanged.connect(self._aggiorna_preview_download)
        self.txt_nome_file.textChanged.connect(self._aggiorna_preview_download)

        self.btn_scarica = QPushButton("⬇  Avvia download")
        self.btn_scarica.setStyleSheet("font-weight: bold; padding: 6px 16px;")
        self.btn_scarica.setEnabled(bool(ultima_dir))
        self.btn_scarica.clicked.connect(self._avvia_download)
        lay.addWidget(self.btn_scarica)

        # ── info PNRR ────────────────────────────────────────────────────
        sep = QFrame()
        sep.setFrameShape(_FRAME_HLINE)
        sep.setFrameShadow(_FRAME_SUNKEN)
        lay.addWidget(sep)

        lbl_pnrr = QLabel(
            '<p align="center" style="font-size:15px; color:gray;">'
            'Progetto finanziato nell\'ambito del '
            '<a href="https://www.italiadomani.gov.it/">PNRR</a>'
            ' — Missione 1, Componente 1, Investimento 1.3 '
            '"Dati e interoperabilità" — '
            '<a href="https://padigitale2026.gov.it/">Misura 1.3.1</a>'
            ' per la digitalizzazione dell\''
            '<a href="https://www.anncsu.gov.it/">Archivio Nazionale dei '
            'Numeri Civici e delle Strade Urbane (ANNCSU)</a>.'
            '</p>'
        )
        lbl_pnrr.setTextFormat(_TEXT_RICH)
        lbl_pnrr.setOpenExternalLinks(True)
        lbl_pnrr.setWordWrap(True)
        lay.addWidget(lbl_pnrr)

        lay.addStretch()
        self._aggiorna_preview_download()
        return w

    # =========================================================================
    # TAB: ESPORTA PER COMUNE
    # =========================================================================

    def _tab_esporta(self) -> QWidget:
        w   = QWidget()
        lay = QVBoxLayout(w)
        lay.setSpacing(8)

        # ricerca comuni
        self.txt_cerca = QLineEdit()
        self.txt_cerca.setPlaceholderText("Cerca comune...")
        self.txt_cerca.setClearButtonEnabled(True)
        self.txt_cerca.setEnabled(False)
        self.txt_cerca.textChanged.connect(self._filtra_comuni)
        lay.addWidget(self.txt_cerca)

        self.lbl_comuni_info = QLabel("Prima carica i comuni con il pulsante in alto")
        self.lbl_comuni_info.setStyleSheet("color: gray; font-size: 11px;")
        lay.addWidget(self.lbl_comuni_info)

        self.lista_comuni = QListWidget()
        self.lista_comuni.setSelectionMode(_multi_selection())
        self.lista_comuni.setAlternatingRowColors(True)
        self.lista_comuni.setEnabled(False)
        self.lista_comuni.itemSelectionChanged.connect(self._aggiorna_info_comuni)
        lay.addWidget(self.lista_comuni)

        hl_sel = QHBoxLayout()
        self.btn_desel_tutti = QPushButton("Deseleziona tutti")
        self.btn_desel_tutti.setFixedHeight(26)
        self.btn_desel_tutti.setEnabled(False)
        self.btn_desel_tutti.clicked.connect(self._deseleziona_tutti)
        hl_sel.addStretch()
        hl_sel.addWidget(self.btn_desel_tutti)
        lay.addLayout(hl_sel)

        # opzioni output
        grp_out = QGroupBox("Opzioni output")
        lay_out = QFormLayout(grp_out)

        self.cmb_formato = QComboBox()
        self.cmb_formato.addItems(["Parquet (.parquet)", "GeoPackage (.gpkg)"])
        lay_out.addRow("Formato:", self.cmb_formato)

        # cartella output
        hl_out = QHBoxLayout()
        self.txt_output_dir = QLineEdit()
        self.txt_output_dir.setPlaceholderText("Default: stessa cartella del Parquet sorgente")
        self.txt_output_dir.setReadOnly(True)
        btn_scegli_out = QPushButton("Sfoglia…")
        btn_scegli_out.setFixedWidth(80)
        btn_scegli_out.clicked.connect(self._scegli_dir_output)
        hl_out.addWidget(self.txt_output_dir)
        hl_out.addWidget(btn_scegli_out)
        lay_out.addRow("Cartella:", hl_out)

        self.chk_carica = QCheckBox("Carica layer in QGIS al termine")
        self.chk_carica.setChecked(True)
        lay_out.addRow("", self.chk_carica)

        self.lbl_output_path = QLabel("")
        self.lbl_output_path.setStyleSheet("color: gray; font-size: 10px;")
        self.lbl_output_path.setWordWrap(True)
        lay_out.addRow("Output:", self.lbl_output_path)

        self.cmb_formato.currentIndexChanged.connect(self._aggiorna_preview_output)
        lay.addWidget(grp_out)

        self.btn_esegui = QPushButton("▶  Esporta")
        self.btn_esegui.setStyleSheet("font-weight: bold; padding: 6px 16px;")
        self.btn_esegui.setEnabled(False)
        self.btn_esegui.clicked.connect(self._avvia_esportazione)
        lay.addWidget(self.btn_esegui)

        return w

    # =========================================================================
    # TAB: CERCA INDIRIZZO
    # =========================================================================

    def _tab_cerca(self) -> QWidget:
        w   = QWidget()
        lay = QVBoxLayout(w)
        lay.setSpacing(8)

        # filtri di ricerca
        grp_filtri = QGroupBox("Parametri di ricerca")
        form = QFormLayout(grp_filtri)

        self.txt_cerca_comune = QLineEdit()
        self.txt_cerca_comune.setPlaceholderText("es. NAPOLI  (lascia vuoto per tutti)")
        form.addRow("Comune:", self.txt_cerca_comune)

        self.txt_cerca_via = QLineEdit()
        self.txt_cerca_via.setPlaceholderText("es. TOLEDO  (parte del nome)")
        form.addRow("Via/Odonimo:", self.txt_cerca_via)

        self.txt_cerca_civico = QLineEdit()
        self.txt_cerca_civico.setPlaceholderText("es. 10  (lascia vuoto per tutti i civici)")
        form.addRow("Civico:", self.txt_cerca_civico)

        hl_max = QHBoxLayout()
        self.spin_max_risultati = QSpinBox()
        self.spin_max_risultati.setRange(1, 500)
        self.spin_max_risultati.setValue(50)
        self.spin_max_risultati.setSuffix(" risultati")
        hl_max.addWidget(self.spin_max_risultati)
        hl_max.addStretch()
        form.addRow("Limite:", hl_max)

        lay.addWidget(grp_filtri)

        self.btn_cerca_indirizzo = QPushButton("🔍  Cerca")
        self.btn_cerca_indirizzo.setStyleSheet("font-weight: bold; padding: 6px 16px;")
        self.btn_cerca_indirizzo.clicked.connect(self._avvia_ricerca_indirizzo)
        lay.addWidget(self.btn_cerca_indirizzo)

        self.lbl_risultati_info = QLabel("")
        self.lbl_risultati_info.setStyleSheet("color: gray; font-size: 11px;")
        lay.addWidget(self.lbl_risultati_info)

        # tabella risultati
        self.tbl_risultati = QTableWidget()
        self.tbl_risultati.setColumnCount(6)
        self.tbl_risultati.setHorizontalHeaderLabels(
            ["Comune", "Via", "Civico", "Esp.", "Lat", "Lon"]
        )
        self.tbl_risultati.horizontalHeader().setSectionResizeMode(1, _HEADER_STRETCH)
        self.tbl_risultati.setSelectionBehavior(_SELECT_ROWS)
        self.tbl_risultati.setEditTriggers(_NO_EDIT_TRIGGERS)
        self.tbl_risultati.setAlternatingRowColors(True)
        self.tbl_risultati.itemSelectionChanged.connect(self._on_risultato_selezionato)
        lay.addWidget(self.tbl_risultati)

        self.btn_zoom = QPushButton("📍  Zoom al selezionato")
        self.btn_zoom.setEnabled(False)
        self.btn_zoom.clicked.connect(self._zoom_a_selezionato)
        lay.addWidget(self.btn_zoom)

        return w

    # =========================================================================
    # LOGICA: FILE PARQUET
    # =========================================================================

    def _scegli_parquet(self):
        ultima = self.settings.value(self.SETTINGS_KEY_PARQUET, "")
        cartella = os.path.dirname(ultima) if ultima else ""
        path, _ = QFileDialog.getOpenFileName(
            self, "Seleziona file Parquet ANNCSU",
            cartella, "Parquet (*.parquet);;Tutti i file (*)"
        )
        if path:
            self._imposta_parquet(path)

    def _imposta_parquet(self, path: str):
        self.txt_parquet.setText(path)
        self.settings.setValue(self.SETTINGS_KEY_PARQUET, path)
        self.btn_carica_comuni.setEnabled(True)
        self._reset_comuni()
        self._aggiorna_preview_output()

    def _ripristina_parquet(self):
        path = self.settings.value(self.SETTINGS_KEY_PARQUET, "")
        if path and os.path.exists(path):
            self.txt_parquet.setText(path)
            self.btn_carica_comuni.setEnabled(True)
            self._aggiorna_preview_output()

    # =========================================================================
    # LOGICA: DOWNLOAD
    # =========================================================================

    def _scegli_dir_download(self):
        cartella = QFileDialog.getExistingDirectory(
            self, "Scegli cartella di destinazione",
            self.txt_download_dir.text() or ""
        )
        if cartella:
            self.txt_download_dir.setText(cartella)
            self.settings.setValue(self.SETTINGS_KEY_OUTPUT_DIR, cartella)
            self.btn_scarica.setEnabled(True)

    def _aggiorna_preview_download(self):
        d = self.txt_download_dir.text().strip()
        n = self.txt_nome_file.text().strip()
        if d and n:
            self.lbl_download_path.setText(os.path.join(d, n))
        else:
            self.lbl_download_path.setText("")

    def _avvia_download(self):
        dest_dir  = self.txt_download_dir.text().strip()
        nome_file = self.txt_nome_file.text().strip()
        if not dest_dir or not nome_file:
            QMessageBox.warning(self, "Dati mancanti",
                                "Specifica la cartella di destinazione e il nome del file.")
            return

        dest_path = os.path.join(dest_dir, nome_file)
        if os.path.exists(dest_path):
            QMessageBox.warning(
                self, "File esistente",
                f"Il file esiste già:\n{dest_path}\n\nRinomina o elimina il file prima di procedere."
            )
            return

        self._download_istat_pending = self.chk_scarica_istat.isChecked()
        self._set_ui_occupata(True, "Download ANNCSU in corso...")

        from .worker import AnncsuWorker
        self.worker = AnncsuWorker(
            "", AnncsuWorker.MODE_DOWNLOAD,
            output_path=dest_path, url=self.ANNCSU_URL
        )
        self.worker.progresso.connect(self._on_progresso)
        self.worker.completato.connect(self._on_download_completato)
        self.worker.errore.connect(self._on_errore)
        self.worker.finished.connect(lambda: self._set_ui_occupata(False))
        self.worker.start()

    def _on_download_completato(self, path: str, n: int):
        self.progress_bar.setValue(100)

        if self._download_istat_pending:
            self._download_istat_pending = False
            self._imposta_parquet(path)
            # Disconnette finished del primo worker per evitare reset prematuro della UI
            try:
                self.worker.finished.disconnect()
            except Exception:
                pass

            dest_dir  = os.path.dirname(path)
            istat_path = os.path.join(dest_dir, "istat-boundaries.parquet")
            if os.path.exists(istat_path):
                QMessageBox.warning(
                    self, "File esistente",
                    f"Il file esiste già:\n{istat_path}\n\nRinomina o elimina il file prima di procedere."
                )
                self._set_ui_occupata(False)
                return
            self.lbl_stato.setText("ANNCSU scaricato. Avvio download confini ISTAT...")

            from .worker import AnncsuWorker
            self.worker = AnncsuWorker(
                "", AnncsuWorker.MODE_DOWNLOAD,
                output_path=istat_path, url=self.ISTAT_URL
            )
            self.worker.progresso.connect(self._on_progresso)
            self.worker.completato.connect(self._on_download_completato)
            self.worker.errore.connect(self._on_errore)
            self.worker.finished.connect(lambda: self._set_ui_occupata(False))
            self._set_ui_occupata(True, "Download ISTAT in corso...")
            self.worker.start()
            return

        self.lbl_stato.setText(f"✓ Download completato: {path}")
        QMessageBox.information(
            self, "Download completato",
            f"File scaricato in:\n{path}"
        )
        self._imposta_parquet(path)

    # =========================================================================
    # LOGICA: COMUNI
    # =========================================================================

    def _avvia_lettura_comuni(self):
        parquet = self.txt_parquet.text().strip()
        if not parquet or not os.path.exists(parquet):
            QMessageBox.warning(self, "File non trovato",
                                "Seleziona un file Parquet valido.")
            return
        self._set_ui_occupata(True, "Lettura comuni in corso...")
        self._reset_comuni()

        from .worker import AnncsuWorker
        self.worker = AnncsuWorker(parquet, AnncsuWorker.MODE_COMUNI)
        self.worker.progresso.connect(self._on_progresso)
        self.worker.comuni_pronti.connect(self._on_comuni_pronti)
        self.worker.errore.connect(self._on_errore)
        self.worker.finished.connect(lambda: self._set_ui_occupata(False))
        self.worker.start()

    def _on_comuni_pronti(self, comuni: list):
        self._comuni_cache = comuni
        self.lista_comuni.clear()
        role = _user_role()
        for nome, istat, count in comuni:
            item = QListWidgetItem(f"{nome}  ({istat})  —  {count:,} civici")
            item.setData(role, nome)
            self.lista_comuni.addItem(item)
        self.lista_comuni.setEnabled(True)
        self.txt_cerca.setEnabled(True)
        self.btn_desel_tutti.setEnabled(True)
        self._aggiorna_info_comuni()

    def _filtra_comuni(self, testo):
        testo_up = testo.upper().strip()
        role = _user_role()
        self.lista_comuni.clear()
        if not testo_up:
            filtrati = self._comuni_cache
        else:
            starts   = [c for c in self._comuni_cache if c[0].upper().startswith(testo_up)]
            contains = [c for c in self._comuni_cache if testo_up in c[0].upper()
                        and not c[0].upper().startswith(testo_up)]
            filtrati = starts + contains
        for nome, istat, count in filtrati:
            item = QListWidgetItem(f"{nome}  ({istat})  —  {count:,} civici")
            item.setData(role, nome)
            self.lista_comuni.addItem(item)
        self._aggiorna_info_comuni()

    def _aggiorna_info_comuni(self):
        tot = len(self._comuni_cache)
        vis = self.lista_comuni.count()
        sel = len(self.lista_comuni.selectedItems())
        self.lbl_comuni_info.setText(
            f"{vis} comuni visualizzati su {tot} — {sel} selezionati"
        )
        self.btn_esegui.setEnabled(sel > 0)
        self._aggiorna_preview_output()

    def _deseleziona_tutti(self):
        self.lista_comuni.clearSelection()

    def _reset_comuni(self):
        self.lista_comuni.clear()
        self.lista_comuni.setEnabled(False)
        self.txt_cerca.setEnabled(False)
        self.txt_cerca.clear()
        self.btn_desel_tutti.setEnabled(False)
        self.btn_esegui.setEnabled(False)
        self.lbl_comuni_info.setText("Prima carica i comuni con il pulsante in alto")
        self._comuni_cache = []

    # =========================================================================
    # LOGICA: ESPORTA
    # =========================================================================

    def _scegli_dir_output(self):
        cartella = QFileDialog.getExistingDirectory(
            self, "Scegli cartella di output",
            self.txt_output_dir.text() or os.path.dirname(self.txt_parquet.text())
        )
        if cartella:
            self.txt_output_dir.setText(cartella)
            self._aggiorna_preview_output()

    def _aggiorna_preview_output(self):
        parquet = self.txt_parquet.text().strip()
        if not parquet:
            self.lbl_output_path.setText("")
            return
        cartella = self.txt_output_dir.text().strip() or os.path.dirname(parquet)
        comuni   = self._comuni_selezionati()
        if comuni:
            tag = "_".join(comuni[:3])[:50]
            if len(comuni) > 3:
                tag += f"_e_altri_{len(comuni)-3}"
        else:
            tag = "COMUNI"
        ext = ".parquet" if self.cmb_formato.currentIndex() == 0 else ".gpkg"
        self.lbl_output_path.setText(os.path.join(cartella, f"ANNCSU_{tag}{ext}"))

    def _comuni_selezionati(self) -> list:
        role = _user_role()
        return [item.data(role) for item in self.lista_comuni.selectedItems()]

    def _avvia_esportazione(self):
        parquet = self.txt_parquet.text().strip()
        comuni  = self._comuni_selezionati()
        if not comuni:
            QMessageBox.warning(self, "Nessun comune", "Seleziona almeno un comune.")
            return

        output_path = self.lbl_output_path.text()
        fmt = "parquet" if self.cmb_formato.currentIndex() == 0 else "gpkg"
        if os.path.exists(output_path):
            r = QMessageBox.question(
                self, "File esistente",
                f"Il file esiste già:\n{output_path}\n\nVuoi caricarlo in QGIS?",
                _MSGBOX_YES | _MSGBOX_NO
            )
            if r == _MSGBOX_YES:
                self._carica_in_qgis(output_path)
            return
        self._set_ui_occupata(True, "Esportazione in corso...")

        from .worker import AnncsuWorker
        self.worker = AnncsuWorker(
            parquet, AnncsuWorker.MODE_EXPORT,
            comuni=comuni, output_path=output_path, fmt=fmt
        )
        self.worker.progresso.connect(self._on_progresso)
        self.worker.completato.connect(self._on_completato)
        self.worker.errore.connect(self._on_errore)
        self.worker.finished.connect(lambda: self._set_ui_occupata(False))
        self.worker.start()

    def _on_completato(self, output_path: str, n_record: int):
        self.lbl_stato.setText(
            f"✓ Completato: {n_record:,} record → {os.path.basename(output_path)}"
        )
        self.progress_bar.setValue(100)
        if self.chk_carica.isChecked():
            self._carica_in_qgis(output_path)
        QMessageBox.information(
            self, "Esportazione completata",
            f"{n_record:,} record esportati in:\n{output_path}"
        )

    def _carica_in_qgis(self, path: str):
        nome = os.path.splitext(os.path.basename(path))[0]
        ext  = os.path.splitext(path)[1].lower()

        if ext == ".gpkg":
            uri = f"{path}|layername=ANNCSU_indirizzi"
            layer = QgsVectorLayer(uri, nome, "ogr")
        elif ext == ".parquet":
            path_unix = path.replace("\\", "/")
            vrt_path = os.path.splitext(path)[0] + ".vrt"
            vrt = (
                f'<OGRVRTDataSource>'
                f'<OGRVRTLayer name="{nome}">'
                f'<SrcDataSource>{path_unix}</SrcDataSource>'
                f'<GeometryType>wkbPoint</GeometryType>'
                f'<LayerSRS>EPSG:4326</LayerSRS>'
                f'<GeometryField encoding="PointFromColumns" x="longitude" y="latitude"/>'
                f'</OGRVRTLayer>'
                f'</OGRVRTDataSource>'
            )
            with open(vrt_path, "w", encoding="utf-8") as f:
                f.write(vrt)
            layer = QgsVectorLayer(vrt_path, nome, "ogr")
        else:
            layer = QgsVectorLayer(path, nome, "ogr")

        if layer.isValid():
            QgsProject.instance().addMapLayer(layer)
            self.iface.zoomToActiveLayer()
        else:
            QMessageBox.warning(self, "Layer non valido",
                                f"File salvato ma non caricabile in QGIS:\n{path}")

    # =========================================================================
    # LOGICA: CERCA INDIRIZZO
    # =========================================================================

    def _avvia_ricerca_indirizzo(self):
        parquet = self.txt_parquet.text().strip()
        if not parquet or not os.path.exists(parquet):
            QMessageBox.warning(self, "File non trovato",
                                "Prima seleziona un file Parquet valido.")
            return

        comune = self.txt_cerca_comune.text().strip().upper()
        via    = self.txt_cerca_via.text().strip().upper()
        civico = self.txt_cerca_civico.text().strip()
        limite = self.spin_max_risultati.value()

        if not via and not comune:
            QMessageBox.warning(self, "Parametri mancanti",
                                "Inserisci almeno il nome del comune o della via.")
            return

        self._set_ui_occupata(True, "Ricerca indirizzo in corso...")
        self.tbl_risultati.setRowCount(0)
        self.lbl_risultati_info.setText("Ricerca in corso...")

        from .worker import AnncsuWorker
        self.worker = AnncsuWorker(
            parquet, AnncsuWorker.MODE_CERCA,
            cerca_comune=comune, cerca_via=via,
            cerca_civico=civico, limite=limite
        )
        self.worker.progresso.connect(self._on_progresso)
        self.worker.risultati_pronti.connect(self._on_risultati_indirizzo)
        self.worker.errore.connect(self._on_errore)
        self.worker.finished.connect(lambda: self._set_ui_occupata(False))
        self.worker.start()

    def _on_risultati_indirizzo(self, righe: list):
        """righe = lista di dict con campi ANNCSU."""
        self.tbl_risultati.setRowCount(0)
        self._risultati_cache = righe

        for r, row in enumerate(righe):
            self.tbl_risultati.insertRow(r)
            self.tbl_risultati.setItem(r, 0, QTableWidgetItem(str(row.get("NOME_COMUNE", ""))))
            self.tbl_risultati.setItem(r, 1, QTableWidgetItem(str(row.get("DIZIONE_LINGUA1", ""))))
            self.tbl_risultati.setItem(r, 2, QTableWidgetItem(str(row.get("CIVICO", ""))))
            self.tbl_risultati.setItem(r, 3, QTableWidgetItem(str(row.get("ESPONENTE", ""))))
            self.tbl_risultati.setItem(r, 4, QTableWidgetItem(str(row.get("latitude", ""))))
            self.tbl_risultati.setItem(r, 5, QTableWidgetItem(str(row.get("longitude", ""))))

        n = len(righe)
        self.lbl_risultati_info.setText(
            f"{n} risultati trovati" + (" (limite raggiunto, raffina la ricerca)" if n == self.spin_max_risultati.value() else "")
        )
        self.btn_zoom.setEnabled(n > 0)
        self.progress_bar.setValue(100)
        self.lbl_stato.setText(f"✓ {n} indirizzi trovati.")

    def _on_risultato_selezionato(self):
        righe = self.tbl_risultati.selectedItems()
        self.btn_zoom.setEnabled(bool(righe))
        if righe:
            self._zoom_a_selezionato()

    def _zoom_a_selezionato(self):
        row_idx = self.tbl_risultati.currentRow()
        if row_idx < 0 or row_idx >= len(self._risultati_cache):
            return

        row = self._risultati_cache[row_idx]
        try:
            lat = float(row.get("latitude"))
            lon = float(row.get("longitude"))
        except (TypeError, ValueError):
            QMessageBox.warning(self, "Coordinate mancanti",
                                "Il record selezionato non ha coordinate valide.")
            return

        # ── zoom mappa ────────────────────────────────────────────────────
        canvas = self.iface.mapCanvas()
        crs_wgs84  = QgsCoordinateReferenceSystem("EPSG:4326")
        crs_canvas = canvas.mapSettings().destinationCrs()
        transform  = QgsCoordinateTransform(crs_wgs84, crs_canvas, QgsProject.instance())
        punto = transform.transform(QgsPointXY(lon, lat))

        canvas.setCenter(punto)
        canvas.zoomScale(2000)
        canvas.refresh()

        # ── marker ────────────────────────────────────────────────────────
        self._rimuovi_marker()
        self._marker = QgsVertexMarker(canvas)
        self._marker.setCenter(punto)
        try:
            self._marker.setIconType(QgsVertexMarker.IconType.ICON_CROSS)
        except AttributeError:
            self._marker.setIconType(QgsVertexMarker.ICON_CROSS)
        self._marker.setColor(QColor(220, 50, 50))
        self._marker.setIconSize(18)
        self._marker.setPenWidth(3)

        # ── popup con attributi ───────────────────────────────────────────
        campi_popup = [
            ("Comune",    row.get("NOME_COMUNE",     "")),
            ("Via",       row.get("DIZIONE_LINGUA1", "")),
            ("Civico",    row.get("CIVICO",          "")),
            ("Esponente", row.get("ESPONENTE",       "")),
            ("Specificità", row.get("SPECIFICITA",   "")),
            ("Metrico",   row.get("METRICO",         "")),
            ("Codice ISTAT", row.get("CODICE_ISTAT", "")),
            ("Lat",       lat),
            ("Lon",       lon),
        ]
        testo = "\n".join(
            f"<b>{k}:</b> {v}" for k, v in campi_popup
            if v is not None and str(v).strip() not in ("", "None", "nan")
        )
        self.iface.messageBar().pushMessage(
            "ANNCSU",
            f"{row.get('DIZIONE_LINGUA1','')} {row.get('CIVICO','')} — {row.get('NOME_COMUNE','')}",
            level=Qgis.MessageLevel.Info, duration=5
        )

        # dialogo popup attributi
        dlg_attr = QDialog(self)
        dlg_attr.setWindowTitle("Attributi indirizzo")
        dlg_attr.setMinimumWidth(320)
        v = QVBoxLayout(dlg_attr)
        lbl = QLabel(testo)
        lbl.setTextFormat(_TEXT_RICH)
        lbl.setWordWrap(True)
        v.addWidget(lbl)
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(dlg_attr.accept)
        v.addWidget(btn_ok)
        dlg_attr.exec()

    def _rimuovi_marker(self):
        if self._marker:
            self.iface.mapCanvas().scene().removeItem(self._marker)
            self._marker = None

    # =========================================================================
    # CALLBACK COMUNI
    # =========================================================================

    def _annulla(self):
        if self.worker and self.worker.isRunning():
            self.worker.cancel()
            self.worker.wait(2000)
            self.lbl_stato.setText("Operazione annullata.")
            self.progress_bar.setValue(0)
            self._set_ui_occupata(False)

    def _on_progresso(self, pct: int, msg: str):
        self.progress_bar.setValue(pct)
        self.lbl_stato.setText(msg)

    def _on_errore(self, msg: str):
        self.lbl_stato.setText(f"✗ Errore: {msg}")
        self.progress_bar.setValue(0)
        QMessageBox.critical(self, "Errore ANNCSU", msg)
        self._set_ui_occupata(False)

    def _set_ui_occupata(self, occupata: bool, msg: str = ""):
        self.btn_sfoglia.setEnabled(not occupata)
        self.btn_carica_comuni.setEnabled(not occupata and bool(self.txt_parquet.text()))
        self.btn_esegui.setEnabled(not occupata and len(self.lista_comuni.selectedItems()) > 0)
        self.btn_annulla.setEnabled(occupata)
        self.cmb_formato.setEnabled(not occupata)
        self.btn_cerca_indirizzo.setEnabled(not occupata)
        self.btn_scarica.setEnabled(not occupata and bool(self.txt_download_dir.text()))
        if msg:
            self.lbl_stato.setText(msg)
        if not occupata:
            self.btn_annulla.setEnabled(False)

    def closeEvent(self, event):
        self._rimuovi_marker()
        if self.worker and self.worker.isRunning():
            self.worker.cancel()
            self.worker.wait(2000)
        event.accept()
