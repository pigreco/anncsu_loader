# -*- coding: utf-8 -*-
"""ANNCSU Loader — worker thread (Qt6 / QGIS 4 compatibile)."""

import os
import math

from qgis.PyQt.QtCore import QThread, pyqtSignal


class AnncsuWorker(QThread):

    # Segnali
    comuni_pronti    = pyqtSignal(list)    # lista tuple (nome, istat)
    risultati_pronti = pyqtSignal(list)    # lista dict per ricerca indirizzo
    progresso        = pyqtSignal(int, str)
    completato       = pyqtSignal(str, int)
    errore           = pyqtSignal(str)

    MODE_COMUNI    = "comuni"
    MODE_EXPORT    = "export"
    MODE_CERCA     = "cerca"
    MODE_DOWNLOAD  = "download"

    def __init__(self, parquet_path: str, mode: str,
                 comuni=None, output_path=None, fmt="parquet",
                 cerca_comune="", cerca_via="", cerca_civico="", limite=50,
                 url=""):
        super().__init__()
        self.parquet_path  = parquet_path
        self.mode          = mode
        self.comuni        = comuni or []
        self.output_path   = output_path
        self.fmt           = fmt
        self.cerca_comune  = cerca_comune
        self.cerca_via     = cerca_via
        self.cerca_civico  = cerca_civico
        self.limite        = limite
        self.url           = url
        self._cancel       = False

    def cancel(self):
        self._cancel = True

    def run(self):
        try:
            if self.mode == self.MODE_DOWNLOAD:
                self._download()
                return

            try:
                import duckdb
            except ImportError:
                self.errore.emit(
                    "La libreria 'duckdb' non è installata.\n"
                    "Installa con:  pip install duckdb  (terminale OSGeo4W)"
                )
                return

            parquet_unix = self.parquet_path.replace("\\", "/")

            if self.mode == self.MODE_COMUNI:
                self._leggi_comuni(duckdb, parquet_unix)
            elif self.mode == self.MODE_EXPORT:
                self._esporta(duckdb, parquet_unix)
            elif self.mode == self.MODE_CERCA:
                self._cerca_indirizzo(duckdb, parquet_unix)

        except Exception as e:
            import traceback
            self.errore.emit(f"{e}\n{traceback.format_exc()}")

    # =========================================================================
    # DOWNLOAD
    # =========================================================================

    def _download(self):
        """Scarica il Parquet tramite QgsBlockingNetworkRequest (Qt5/Qt6 compatibile).

        QgsBlockingNetworkRequest è progettato per chiamate da thread secondari
        e non richiede un QEventLoop manuale, evitando l'antipattern Qt6 dove
        QgsNetworkAccessManager (main thread) non riceveva eventi dal thread figlio.
        """
        from qgis.core import QgsBlockingNetworkRequest
        from qgis.PyQt.QtNetwork import QNetworkRequest
        from qgis.PyQt.QtCore import QUrl

        try:
            _NO_ERROR = QgsBlockingNetworkRequest.ErrorCode.NoError  # QGIS 4 / Qt6
        except AttributeError:
            _NO_ERROR = QgsBlockingNetworkRequest.NoError             # QGIS 3 / Qt5

        self.progresso.emit(5, "Connessione al server...")

        net_request = QNetworkRequest(QUrl(self.url))
        try:
            net_request.setAttribute(
                QNetworkRequest.Attribute.RedirectPolicyAttribute,
                QNetworkRequest.RedirectPolicy.NoLessSafeRedirectPolicy
            )
        except AttributeError:
            pass

        req = QgsBlockingNetworkRequest()
        req.downloadProgress.connect(self._on_download_progress)
        err = req.get(net_request, forceRefresh=True)

        if err != _NO_ERROR:
            self.errore.emit(f"Errore download: {req.errorMessage()}")
            return

        self.progresso.emit(92, "Salvataggio file...")
        dest_dir = os.path.dirname(self.output_path)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
        content = bytes(req.reply().content())
        with open(self.output_path, "wb") as f:
            f.write(content)

        size_mb = os.path.getsize(self.output_path) / 1024 / 1024
        self.progresso.emit(100, f"✓ Scaricato ({size_mb:.1f} MB)")
        self.completato.emit(self.output_path, 0)

    def _on_download_progress(self, received, total):
        if total > 0:
            pct = int(received / total * 90)
            mb_r = received / 1024 / 1024
            mb_t = total / 1024 / 1024
            self.progresso.emit(pct, f"Download: {mb_r:.1f} / {mb_t:.1f} MB")

    # =========================================================================
    # LETTURA COMUNI
    # =========================================================================

    def _leggi_comuni(self, duckdb, parquet_unix):
        self.progresso.emit(20, "Apertura file Parquet...")
        con = duckdb.connect()
        self.progresso.emit(50, "Lettura lista comuni...")
        result = con.execute(f"""
            SELECT NOME_COMUNE, CODICE_ISTAT, COUNT(*) as N
            FROM read_parquet('{parquet_unix}')
            WHERE NOME_COMUNE IS NOT NULL
            GROUP BY NOME_COMUNE, CODICE_ISTAT
            ORDER BY NOME_COMUNE
        """).fetchall()
        self.progresso.emit(100, f"{len(result):,} comuni trovati.")
        self.comuni_pronti.emit(result)

    # =========================================================================
    # ESPORTAZIONE
    # =========================================================================

    def _esporta(self, duckdb, parquet_unix):
        con = duckdb.connect()

        if self.comuni:
            lista  = ", ".join(f"'{c}'" for c in self.comuni)
            filtro = f"WHERE NOME_COMUNE IN ({lista})"
        else:
            filtro = ""

        self.progresso.emit(15, "Conteggio record...")
        n_tot = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_unix}') {filtro}"
        ).fetchone()[0]
        self.progresso.emit(25, f"{n_tot:,} record da esportare...")

        if self._cancel:
            return

        output_unix = self.output_path.replace("\\", "/")

        if self.fmt == "parquet":
            self.progresso.emit(50, "Scrittura Parquet...")
            con.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{parquet_unix}') {filtro}
                ) TO '{output_unix}' (FORMAT PARQUET)
            """)
            self.progresso.emit(100, "Completato.")
            self.completato.emit(self.output_path, n_tot)

        elif self.fmt == "gpkg":
            self.progresso.emit(35, "Lettura dati in memoria...")
            df = con.execute(
                f"SELECT * FROM read_parquet('{parquet_unix}') {filtro}"
            ).fetchdf()
            if self._cancel:
                return
            self.progresso.emit(55, "Costruzione layer vettoriale...")
            n = self._scrivi_gpkg(df)
            self.progresso.emit(100, "GeoPackage salvato.")
            self.completato.emit(self.output_path, n)

    def _scrivi_gpkg(self, df) -> int:
        from qgis.core import (
            QgsVectorLayer, QgsField, QgsFeature,
            QgsGeometry, QgsPointXY, QgsProject,
            QgsVectorFileWriter,
        )

        # Qt5/Qt6 compat: QVariant → QMetaType.Type
        try:
            from qgis.PyQt.QtCore import QMetaType
            tipo_campo = {
                "object":  QMetaType.Type.QString,
                "int64":   QMetaType.Type.LongLong,
                "float64": QMetaType.Type.Double,
                "bool":    QMetaType.Type.Bool,
            }
        except (ImportError, AttributeError):
            from qgis.PyQt.QtCore import QVariant
            tipo_campo = {
                "object":  QVariant.String,
                "int64":   QVariant.LongLong,
                "float64": QVariant.Double,
                "bool":    QVariant.Bool,
            }

        # Qt5/Qt6 compat: QgsVectorFileWriter error code
        try:
            _WRITER_NO_ERROR = QgsVectorFileWriter.WriterError.NoError  # QGIS 4
        except AttributeError:
            _WRITER_NO_ERROR = QgsVectorFileWriter.NoError               # QGIS 3

        df_geo = df.dropna(subset=["longitude", "latitude"])
        _default_type = tipo_campo["object"]
        nome_layer = "ANNCSU_indirizzi"
        layer = QgsVectorLayer("Point?crs=EPSG:4326", nome_layer, "memory")
        pr    = layer.dataProvider()
        cols  = [c for c in df_geo.columns if c not in ("longitude", "latitude")]
        pr.addAttributes([
            QgsField(c, tipo_campo.get(str(df_geo[c].dtype), _default_type))
            for c in cols
        ])
        layer.updateFields()

        total    = len(df_geo)
        step     = max(1, total // 20)
        features = []
        for i, (_, row) in enumerate(df_geo.iterrows()):
            if self._cancel:
                return 0
            feat = QgsFeature()
            feat.setGeometry(QgsGeometry.fromPointXY(
                QgsPointXY(row["longitude"], row["latitude"])
            ))
            feat.setAttributes([
                None if (v is None or (isinstance(v, float) and math.isnan(v))) else v
                for v in (row[c] for c in cols)
            ])
            features.append(feat)
            if i % step == 0:
                self.progresso.emit(55 + int(i / total * 35), f"Feature {i:,}/{total:,}...")

        pr.addFeatures(features)
        layer.updateExtents()

        options = QgsVectorFileWriter.SaveVectorOptions()
        options.driverName   = "GPKG"
        options.fileEncoding = "UTF-8"
        options.layerName    = nome_layer

        # writeAsVectorFormatV3 (QGIS 3.20+/4.x) restituisce (err, msg, newfile, newlayer)
        # writeAsVectorFormatV2 (QGIS 3.10-3.x) restituisce (err, msg)
        if hasattr(QgsVectorFileWriter, "writeAsVectorFormatV3"):
            result = QgsVectorFileWriter.writeAsVectorFormatV3(
                layer, self.output_path,
                QgsProject.instance().transformContext(), options
            )
            err, msg = result[0], result[1]
        else:
            err, msg = QgsVectorFileWriter.writeAsVectorFormatV2(
                layer, self.output_path,
                QgsProject.instance().transformContext(), options
            )

        if err != _WRITER_NO_ERROR:
            raise Exception(f"Errore GeoPackage: {msg}")
        return len(features)

    # =========================================================================
    # RICERCA INDIRIZZO
    # =========================================================================

    def _cerca_indirizzo(self, duckdb, parquet_unix):
        self.progresso.emit(20, "Ricerca in corso...")
        con = duckdb.connect()

        condizioni = []
        if self.cerca_comune:
            condizioni.append(f"UPPER(NOME_COMUNE) LIKE '%{self.cerca_comune}%'")
        if self.cerca_via:
            condizioni.append(
                f"(UPPER(DIZIONE_LINGUA1) LIKE '%{self.cerca_via}%' "
                f"OR UPPER(ODONIMO) LIKE '%{self.cerca_via}%')"
            )
        if self.cerca_civico:
            try:
                n = int(self.cerca_civico)
                condizioni.append(f"CIVICO = {n}")
            except ValueError:
                condizioni.append(f"UPPER(CAST(CIVICO AS VARCHAR)) LIKE '%{self.cerca_civico.upper()}%'")

        where = ("WHERE " + " AND ".join(condizioni)) if condizioni else ""

        self.progresso.emit(50, "Esecuzione query...")
        rows = con.execute(f"""
            SELECT
                NOME_COMUNE, CODICE_ISTAT, ODONIMO,
                DIZIONE_LINGUA1, DIZIONE_LINGUA2,
                CIVICO, ESPONENTE, SPECIFICITA, METRICO,
                latitude, longitude, QUOTA, METODO
            FROM read_parquet('{parquet_unix}')
            {where}
            LIMIT {self.limite}
        """).fetchall()

        cols = [
            "NOME_COMUNE","CODICE_ISTAT","ODONIMO",
            "DIZIONE_LINGUA1","DIZIONE_LINGUA2",
            "CIVICO","ESPONENTE","SPECIFICITA","METRICO",
            "latitude","longitude","QUOTA","METODO"
        ]
        risultati = [dict(zip(cols, r)) for r in rows]
        self.progresso.emit(100, f"{len(risultati)} risultati trovati.")
        self.risultati_pronti.emit(risultati)
