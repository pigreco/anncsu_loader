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

        if os.path.exists(self.output_path):
            os.remove(self.output_path)

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
            # Escludi colonne GEOMETRY non convertibili in pandas da DuckDB
            rel = con.sql(f"SELECT * FROM read_parquet('{parquet_unix}') LIMIT 0")
            safe_cols = [
                col for col, dtype in zip(rel.columns, rel.dtypes)
                if "GEOMETRY" not in str(dtype).upper()
            ]
            col_list = ", ".join(f'"{c}"' for c in safe_cols)
            df = con.execute(
                f"SELECT {col_list} FROM read_parquet('{parquet_unix}') {filtro}"
            ).fetchdf()
            if "latitude" not in df.columns or "longitude" not in df.columns:
                raise Exception(
                    f"Colonne coordinate non trovate nel Parquet.\n"
                    f"Colonne disponibili: {list(df.columns)}"
                )
            if self._cancel:
                return
            self.progresso.emit(55, "Costruzione layer vettoriale...")
            n = self._scrivi_gpkg(df)
            self.progresso.emit(100, "GeoPackage salvato.")
            self.completato.emit(self.output_path, n)

    def _scrivi_gpkg(self, df) -> int:
        import pandas as _pd
        from osgeo import ogr, osr

        def _to_py(v):
            if v is None or v is _pd.NA or v is _pd.NaT:
                return None
            if hasattr(v, "item"):
                v = v.item()
            if isinstance(v, float) and math.isnan(v):
                return None
            return v

        df_geo = df.dropna(subset=["longitude", "latitude"])
        if df_geo.empty:
            raise Exception(
                f"Nessuna riga con coordinate valide.\n"
                f"Righe totali: {len(df)}, con lat/lon: {len(df_geo)}"
            )

        cols = [c for c in df_geo.columns if c not in ("longitude", "latitude")]

        # Crea GeoPackage via OGR (sempre disponibile in QGIS)
        import os
        if os.path.exists(self.output_path):
            os.remove(self.output_path)

        driver = ogr.GetDriverByName("GPKG")
        ds = driver.CreateDataSource(self.output_path)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        lyr = ds.CreateLayer("ANNCSU_indirizzi", srs, ogr.wkbPoint)

        # Mappa dtype → (OGR type, Python cast)
        ftypes = []
        for c in cols:
            s = str(df_geo[c].dtype).lower()
            if "bool" in s:
                ftypes.append((ogr.OFTInteger, lambda v: int(bool(v))))
            elif "int" in s:
                ftypes.append((ogr.OFTInteger64, int))
            elif "float" in s:
                ftypes.append((ogr.OFTReal, float))
            else:
                ftypes.append((ogr.OFTString, str))
            lyr.CreateField(ogr.FieldDefn(c, ftypes[-1][0]))

        defn  = lyr.GetLayerDefn()
        total = len(df_geo)
        step  = max(1, total // 20)
        n     = 0
        for i, (_, row) in enumerate(df_geo.iterrows()):
            if self._cancel:
                break
            feat = ogr.Feature(defn)
            pt = ogr.Geometry(ogr.wkbPoint)
            pt.AddPoint(float(row["longitude"]), float(row["latitude"]))
            feat.SetGeometry(pt)
            for j, c in enumerate(cols):
                v = _to_py(row[c])
                if v is not None:
                    _, cast = ftypes[j]
                    feat.SetField(j, cast(v))
            lyr.CreateFeature(feat)
            n += 1
            if i % step == 0:
                self.progresso.emit(55 + int(i / total * 35), f"Feature {i:,}/{total:,}...")

        ds.FlushCache()
        ds = None
        return n

    # =========================================================================
    # RICERCA INDIRIZZO
    # =========================================================================

    def _cerca_indirizzo(self, duckdb, parquet_unix):
        self.progresso.emit(20, "Ricerca in corso...")
        con = duckdb.connect()

        condizioni = []
        params = []
        if self.cerca_comune:
            condizioni.append("UPPER(NOME_COMUNE) LIKE $" + str(len(params) + 1))
            params.append(f"%{self.cerca_comune.upper()}%")
        if self.cerca_via:
            condizioni.append(
                "(UPPER(DIZIONE_LINGUA1) LIKE $" + str(len(params) + 1) +
                " OR UPPER(ODONIMO) LIKE $" + str(len(params) + 1) + ")"
            )
            params.append(f"%{self.cerca_via.upper()}%")
        if self.cerca_civico:
            try:
                n = int(self.cerca_civico)
                condizioni.append("CIVICO = $" + str(len(params) + 1))
                params.append(n)
            except ValueError:
                condizioni.append("UPPER(CAST(CIVICO AS VARCHAR)) LIKE $" + str(len(params) + 1))
                params.append(f"%{self.cerca_civico.upper()}%")

        where = ("WHERE " + " AND ".join(condizioni)) if condizioni else ""

        order_parts = []
        if self.cerca_comune:
            v = self.cerca_comune.upper().replace("'", "''")
            order_parts.append(f"CASE WHEN UPPER(NOME_COMUNE) LIKE '{v}%' THEN 0 ELSE 1 END")
        if self.cerca_via:
            v = self.cerca_via.upper().replace("'", "''")
            order_parts.append(f"CASE WHEN UPPER(DIZIONE_LINGUA1) LIKE '{v}%' THEN 0 ELSE 1 END")
        order_parts += ["NOME_COMUNE", "DIZIONE_LINGUA1", "CIVICO"]
        order_by = "ORDER BY " + ", ".join(order_parts)

        self.progresso.emit(50, "Esecuzione query...")
        rows = con.execute(f"""
            SELECT
                NOME_COMUNE, CODICE_ISTAT, ODONIMO,
                DIZIONE_LINGUA1, DIZIONE_LINGUA2,
                CIVICO, ESPONENTE, SPECIFICITA, METRICO,
                latitude, longitude, QUOTA, METODO
            FROM read_parquet('{parquet_unix}')
            {where}
            {order_by}
            LIMIT {self.limite}
        """, params).fetchall()

        cols = [
            "NOME_COMUNE","CODICE_ISTAT","ODONIMO",
            "DIZIONE_LINGUA1","DIZIONE_LINGUA2",
            "CIVICO","ESPONENTE","SPECIFICITA","METRICO",
            "latitude","longitude","QUOTA","METODO"
        ]
        risultati = [dict(zip(cols, r)) for r in rows]
        self.progresso.emit(100, f"{len(risultati)} risultati trovati.")
        self.risultati_pronti.emit(risultati)
