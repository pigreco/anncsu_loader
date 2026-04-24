# ANNCSU Loader

Plugin QGIS per caricare i dati **ANNCSU** (Archivio Nazionale Numeri Civici e Strade Urbane) da file Parquet locale, con filtro per comune ed esportazione in Parquet o GeoPackage.

Compatibile con **QGIS 3.20+** e **QGIS 4.x** (Qt5/Qt6).

## Requisiti

- QGIS ≥ 3.20
- Python: `pip install duckdb` (terminale OSGeo4W su Windows, o terminale di sistema su Linux/macOS)

## Installazione

1. Copia la cartella `anncsu_loader/` nella directory dei plugin di QGIS:
   - Linux: `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins/`
   - macOS: `~/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins/`
   - Windows: `%APPDATA%\QGIS\QGIS3\profiles\default\python\plugins\`
2. Installa la dipendenza: `pip install duckdb`
3. In QGIS: *Plugin → Gestisci e installa plugin → Installato* → abilita **ANNCSU Loader**

### Installazione su macOS

Su macOS il plugin va copiato nel profilo utente attivo di QGIS, in genere:

`~/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins/`

Se usi un profilo QGIS diverso da `default`, sostituisci il nome del profilo nel percorso.

Per installare `duckdb`, e' preferibile usare il Python incluso in QGIS. Nella maggior parte delle installazioni standard:

```bash
/Applications/QGIS.app/Contents/MacOS/bin/python3 -m pip install duckdb
```

Se quel percorso non e' presente, apri la console Python di QGIS e verifica l'interprete in uso:

```python
import sys
print(sys.executable)
```

Poi usa il percorso restituito per installare la dipendenza:

```bash
"/percorso/del/python/di/qgis" -m pip install duckdb
```

Dopo l'installazione, riavvia QGIS se il plugin non compare subito nella lista dei plugin installati.

## Agenti Copilot

Nel workspace sono disponibili due agenti personalizzati in `.github/agents/`:

- `Ricerca Installazione macOS`: cerca sul web i passaggi corretti per installare il plugin su macOS e restituisce una procedura verificata.
- `Aggiorna README Installazione`: aggiorna `README.md` con istruzioni operative, in particolare per installazione e prerequisiti.

## Funzionalità

### Tab Scarica
Scarica il file Parquet ANNCSU completo dal repository ufficiale direttamente in una cartella locale.

### Tab Esporta per Comune
1. Seleziona il file Parquet sorgente con **Sfoglia** e poi **Carica comuni**
2. Cerca e seleziona uno o più comuni dalla lista
3. Scegli il formato di output (Parquet o GeoPackage) e la cartella di destinazione
4. Clicca **Esporta** — il layer viene caricato automaticamente in QGIS se l'opzione è abilitata

### Tab Cerca Indirizzo
Cerca indirizzi per comune, via e/o numero civico. I risultati vengono mostrati in tabella; cliccando una riga la mappa si centra sull'indirizzo con un marker e un popup di attributi.

![](gui.png)

## File Parquet ANNCSU

Il dataset è disponibile su:
- Indirizzi: `https://gbvitrano.it/anncus/data/anncsu-indirizzi.parquet`
- Confini ISTAT: `https://gbvitrano.it/anncus/data/istat-boundaries.parquet`

Usare il tab **Scarica** del plugin per ottenerlo, oppure scaricarlo manualmente e puntare al file con **Sfoglia**.

Gli URL sono centralizzati in `urls.py` nella root del plugin: per aggiornarli basta modificare le due costanti in quel file.

## Changelog

### 2.1
- Fix separatore di percorso su Windows: `os.path.join` produceva un percorso misto (es. `F:/TEMP\file.parquet`) che impediva l'auto-popolamento del campo **File ParquetANNCSU** al termine del download — [issue #11](https://github.com/pigreco/anncsu_loader/issues/11).

### 2.0
- Fix crash esportazione comuni con apostrofo nel nome (es. `Reggio nell'Emilia`) — [issue #10](https://github.com/pigreco/anncsu_loader/issues/10): l'apostrofo non veniva escaped nella query SQL.
- Nome file di output sanificato: apostrofi, spazi e caratteri speciali sostituiti da `_` (es. `anncsu_Reggio_nell_Emilia.parquet`).
- Annotazioni `# nosec B608` su tutte le query DuckDB (falsi positivi Bandit: path Parquet da file picker Qt, non da input utente libero).

### 1.9
- URL di download (`ANNCSU_URL`, `ISTAT_URL`) spostati in `urls.py` nella root del plugin per facilitarne la manutenzione senza toccare il codice principale.
- Aggiornati URL al dominio `gbvitrano.it`.

### 1.8
- Aggiornati URL sorgente ANNCSU e confini ISTAT al nuovo repository `quattochiacchiereinquattro/anncus`.
- Gestione file esistente: download avvisa e blocca se il file esiste già; export offre di caricare il file esistente in QGIS.
- Prefisso `anncsu_` / `istat_` sul file esportato in base al nome del parquet sorgente.
- Export ANNCSU in formato Parquet ora produce un **GeoParquet** con geometria punto (OGR Parquet driver), evitando il caricamento errato come poligoni ISTAT.
- Credits [gbvitrano](https://github.com/gbvitrano) / [Geobeyond Srl](https://www.geobeyond.it/) nel tab Scarica.

### 1.7
- Filtro comuni: i risultati sono ora ordinati con i comuni che **iniziano con** il testo cercato in cima, seguiti da quelli che lo contengono nel nome.
- Ricerca indirizzi: la query DuckDB ordina i risultati prioritizzando i match "inizia con" su comune e via, così cercando "TOLE" nella via, "TOLEDO" compare prima di altri risultati parziali.

### 1.6
- Fix SQL injection nella ricerca indirizzo (issue #6): i campi comune, via e civico venivano interpolati direttamente nella query DuckDB con f-string. Ora vengono passati come parametri bind (`$1`, `$2`, …), eliminando crash con caratteri speciali come `'`.

### 1.5
- Rimosso bottone "Seleziona tutti" che bloccava il plugin con dataset di grandi dimensioni.

### 1.4
- Fix esportazione GeoPackage: il Parquet ANNCSU contiene una colonna `GEOMETRY` nativa che DuckDB non riesce a convertire in pandas. La colonna viene ora esclusa automaticamente prima della fetch; la scrittura del GeoPackage usa `osgeo.ogr` (GDAL) invece del memory layer PyQGIS, eliminando i problemi di tipo con numpy/pandas nullable dtypes.

### 1.3
- Aggiunta descrizione in inglese nel campo `about` (richiesta dal repository QGIS ufficiale).

### 1.2
- Aggiunge info PNRR con link nel tab Scarica; fix `pushMessage` per QGIS 4 (`Qgis.MessageLevel.Info`); fix `QFrame.Shape`/Shadow enum Qt6.

### 1.1
- Porting QGIS 4 / Qt6 / PyQt6: `QgsBlockingNetworkRequest`, enum qualificati, `QMetaType.Type`, `QgsVectorFileWriter.WriterError`, `writeAsVectorFormatV3`.

### 1.0
- Prima versione.

## Licenza

Il codice del plugin è rilasciato sotto licenza **MIT** — © 2025 Salvatore Fiandaca.
Vedi il file [LICENSE](LICENSE) per il testo completo.

I dati ANNCSU sono soggetti alla licenza del dataset ufficiale.
