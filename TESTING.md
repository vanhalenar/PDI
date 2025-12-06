# Dokumentace k testům pro PDI projekt

## Účel testů

Tento projekt obsahuje sadu jednotkových testů ověřujících funkčnost jednotlivých dotazů (queries) implementovaných v souboru pyspark_consumer.py.
Testy pracují s **offline testovacími daty** – tj. záznam reálných MediaWiki událostí uložených v souboru tests/testing_data.json.

Testy ověřují, že:

- výsledný DataFrame má správné schéma,
- neobsahuje neplatné hodnoty (NULL, záporné hodnoty),
- výsledky jsou správně seřazeny (pokud to dotaz vyžaduje),
- časová okna mají správné intervaly (60 s, 120 s),
- výsledky odpovídají fixním testovacím datům.

## Instalace závislostí

Testy vyžadují **Python 3.10+** a balíčky uvedené v `requirements.txt`.

Instalace:

```bash
pip install -r requirements.txt
```

## Struktura testovacího prostředí

Testy se nacházejí v souboru `tests/test_queries.py`.

Soubor s testovacími daty (MediaWiki RecentChanges JSON) je uložen v `tests/testing_data.json`

Testy spouštějí hlavní skript v testovacím módu:

```python
sys.argv = ["pyspark_consumer.py", "--test", "--server-counts"]
```

Každá query vrací přímo DataFrame, který se v testu dále validuje.

## Formát testovacích dat

Testovací data mají stejný formát jako reálný MediaWiki RecentChange stream.
Ukázka jedné položky:

```json
{
"$schema": "/mediawiki/recentchange/1.0.0",
"meta": {
"uri": "...",
"id": "...",
"domain": "en.wikipedia.org",
...
},
"id": 1972599475,
"type": "edit",
"namespace": 2,
"timestamp": 1764972717,
"user": "Monkeysmashingkeyboards",
"server_name": "en.wikipedia.org",
"length": {"old": 35971, "new": 36097}
}
```

Datový soubor obsahuje více událostí z různých wiki domén, různých typů editací od různých autorů.

## Spuštění testů

Z kořenového adresáře projektu:

Minimální výpis

```bash
pytest -v
```

S výpisem DataFrame (df.show())

```bash
pytest -v -s
```

Testy využívají testovací JSON, takže není nutné spouštět Kafka consumer ani mít běžící Spark streaming.

## Ověřované scénáře

### test_server_counts

- ověřuje schéma (server_name, count)
- žádná hodnota není NULL
- výsledků musí být 26
- první řádek má mít count == 141
- výsledky musí být seřazeny sestupně podle count

### test_edits_per_minute

- ověřuje schéma (minute_start, minute_end, edits_per_minute)
- počet editací za minutu není záporný
- délka okna musí být exactly 60s

### test_edits_by_type

- ověřuje schéma (type, count)
- všechny typy musí být ze sady: `"edit", "new", "log", "categorize"`
- žádné NULL hodnoty
- žádná záporná čísla

### test_user_counts

- žádný uživatel není NULL
- activity_count > 0 pro každého uživatele
- výstup je seřazen podle aktivity sestupně

### test_avg_edit_length_change

- query vrací 5 řádků
- okno je přesně 120 sekund
- validuje rozdíl window_end - window_start

## Předpokládané výsledky

- Díky fixním testovacím datům jsou výsledky deterministické:
- Query server-counts má 26 serverů, největší count = 141
- Edit windows u avg-edit-length-change vždy mají přesně 2 minuty
- Všechny DataFramy mají správná schémata a neobsahují neplatné hodnoty
- Testy ověřují nejen obsah, ale i korektnost třídění, časových oken a základní logiku transformace
