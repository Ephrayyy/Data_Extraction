"""Jointure des 7 tables IMDb en un seul CSV : une ligne par titre, tous types confondus
(film, court-métrage, série, épisode, téléfilm, jeu vidéo...), indiqué dans la colonne `titleType`.

Point de départ : title.basics, sans filtre. Chaque autre table est d'abord ramenée
à une ligne par titre (les valeurs multiples sont regroupées dans une cellule, séparées par " | "),
puis rattachée par LEFT JOIN sur tconst : aucun titre n'est perdu ni dupliqué.

- title.basics     : type, titres, années de début/fin, durée, genres, isAdult
- title.ratings    : note moyenne, nombre de votes
- title.crew       : réalisateurs et scénaristes (identifiants remplacés par les noms)
- title.principals : casting et équipe principale, une colonne par métier (noms, dans l'ordre IMDb)
                     + characters joués, alignés sur la colonne actors
- name.basics      : noms des personnes, et infos sur le 1er réalisateur (naissance, décès)
- title.akas       : titre français, nombre de titres alternatifs, pays et langues de diffusion
- title.episode    : pour les épisodes, la série parente (id et titre), la saison et le numéro

Usage : /opt/anaconda3/bin/python3 jointure_titres.py
Entrée : Data/*.tsv     Sortie : output/imdb_titres.csv
"""
import time
from pathlib import Path

import duckdb

RACINE = Path(__file__).resolve().parent
DOSSIER_TSV = RACINE / "Data"
DOSSIER_OUTPUT = RACINE / "output"
DOSSIER_TEMP = DOSSIER_OUTPUT / "temp_duckdb"
SORTIE = DOSSIER_OUTPUT / "imdb_titres.csv"
SEP = " | "  # séparateur à l'intérieur des cellules multi-valeurs
NB_PAQUETS = 16  # les gros regroupements sont faits par paquets de titres pour tenir en mémoire

# IMDb : TSV, pas de guillemets, valeurs manquantes "\N". Tout est lu en texte.
OPTIONS = "delim='\t', quote='', escape='', header=true, nullstr='\\N', all_varchar=true"

# Colonne de sortie -> métiers de title.principals (actor et actress regroupés)
METIERS = {
    "actors": ("actor", "actress"),
    "producers": ("producer",),
    "composers": ("composer",),
    "cinematographers": ("cinematographer",),
    "editors": ("editor",),
    "productionDesigners": ("production_designer",),
    "castingDirectors": ("casting_director",),
    "selfAppearances": ("self",),
    "archiveFootage": ("archive_footage", "archive_sound"),
}


def table(nom):
    return f"read_csv('{DOSSIER_TSV / (nom + '.tsv')}', {OPTIONS})"


def par_paquets(requete):
    """Une requête par paquet de titres : `{paquet}` est remplacé par un filtre sur hash(tconst)."""
    return [requete.replace("{paquet}", f"hash(tconst) % {NB_PAQUETS} = {i}") for i in range(NB_PAQUETS)]


colonnes_metiers = ",\n".join(
    f"string_agg(primaryName, '{SEP}' ORDER BY ordering) "
    f"FILTER (WHERE category IN ({', '.join(repr(c) for c in cats)})) AS {col}"
    for col, cats in METIERS.items()
)

# Étapes intermédiaires, enregistrées comme tables dans une base DuckDB temporaire sur disque.
# Une liste de requêtes = une table remplie paquet par paquet.
ETAPES = {
"titres": f"""
    SELECT tconst, titleType, primaryTitle, originalTitle,
           isAdult::INT AS isAdult, startYear::INT AS startYear, endYear::INT AS endYear,
           runtimeMinutes::INT AS runtimeMinutes, genres
    FROM {table('title.basics')}
""",
"names": f"""
    SELECT nconst, primaryName, birthYear, deathYear, primaryProfession
    FROM {table('name.basics')}
""",
"ratings": f"""
    SELECT tconst, averageRating::DOUBLE AS averageRating, numVotes::INT AS numVotes
    FROM {table('title.ratings')}
""",

# title.crew : les listes de nconst remplacées par les noms, dans le même ordre
"crew_noms": f"""
    WITH crew AS (SELECT * FROM {table('title.crew')}),
    eclate AS (
        SELECT tconst, 'directors' AS role, unnest(string_split(directors, ',')) AS nconst,
               generate_subscripts(string_split(directors, ','), 1) AS pos
        FROM crew WHERE directors IS NOT NULL
        UNION ALL
        SELECT tconst, 'writers', unnest(string_split(writers, ',')),
               generate_subscripts(string_split(writers, ','), 1)
        FROM crew WHERE writers IS NOT NULL
    )
    SELECT e.tconst,
           string_agg(coalesce(n.primaryName, e.nconst), '{SEP}' ORDER BY e.pos)
               FILTER (WHERE role = 'directors') AS directors,
           string_agg(coalesce(n.primaryName, e.nconst), '{SEP}' ORDER BY e.pos)
               FILTER (WHERE role = 'writers') AS writers,
           count(*) FILTER (WHERE role = 'directors') AS numDirectors,
           count(*) FILTER (WHERE role = 'writers') AS numWriters,
           -- 1er réalisateur : infos de name.basics
           first(n.birthYear ORDER BY e.pos) FILTER (WHERE role = 'directors')
               AS director1BirthYear,
           first(n.deathYear ORDER BY e.pos) FILTER (WHERE role = 'directors')
               AS director1DeathYear,
           first(n.primaryProfession ORDER BY e.pos) FILTER (WHERE role = 'directors')
               AS director1Professions
    FROM eclate e LEFT JOIN names n USING (nconst)
    GROUP BY e.tconst
""",

"principals_brut": f"""
    SELECT p.tconst, p.ordering::INT AS ordering, p.category,
           coalesce(n.primaryName, p.nconst) AS primaryName,
           -- ["Rick","Narrator"] -> Rick / Narrator
           nullif(replace(regexp_replace(p.characters, '^\\["|"\\]$', '', 'g'),
                          '","', ' / '), '') AS character
    FROM {table('title.principals')} p
    LEFT JOIN names n USING (nconst)
""",

# title.principals : une colonne par métier ; characters alignés sur les actors
"principals_agg": par_paquets(f"""
    SELECT tconst,
           {colonnes_metiers},
           string_agg(coalesce(character, '?'), '{SEP}' ORDER BY ordering)
               FILTER (WHERE category IN ('actor', 'actress')) AS characters,
           count(*) FILTER (WHERE category IN ('actor', 'actress')) AS numActors,
           count(*) FILTER (WHERE category = 'actress') AS numActresses
    FROM principals_brut
    WHERE {{paquet}}
    GROUP BY tconst
"""),

# title.akas : titre français + résumé des sorties internationales
"akas_brut": f"""
    SELECT titleId AS tconst, ordering::INT AS ordering, title, region, language
    FROM {table('title.akas')}
""",
"akas_agg": par_paquets(f"""
    SELECT tconst,
           first(title ORDER BY ordering) FILTER (WHERE region = 'FR') AS frenchTitle,
           count(*) AS numAkas,
           count(DISTINCT region) AS numRegions,
           array_to_string(list_sort(list(DISTINCT region)), '{SEP}') AS regions,
           array_to_string(list_sort(list(DISTINCT language)), '{SEP}') AS languages,
           array_to_string(list_sort(list(DISTINCT title)), '{SEP}') AS akaTitles
    FROM akas_brut
    WHERE {{paquet}}
    GROUP BY tconst
"""),

# title.episode : série parente (avec son titre), saison, numéro d'épisode
"episodes": f"""
    SELECT e.tconst, e.parentTconst, s.primaryTitle AS parentTitle,
           e.seasonNumber::INT AS seasonNumber, e.episodeNumber::INT AS episodeNumber
    FROM {table('title.episode')} e
    LEFT JOIN titres s ON s.tconst = e.parentTconst
""",

"resultat": """
    SELECT f.tconst, f.titleType,
           f.primaryTitle, f.originalTitle, a.frenchTitle,
           f.startYear, f.endYear, f.runtimeMinutes, f.genres, f.isAdult,
           r.averageRating, r.numVotes,
           c.directors, c.numDirectors,
           c.director1BirthYear, c.director1DeathYear, c.director1Professions,
           c.writers, c.numWriters,
           p.actors, p.characters, p.numActors, p.numActresses,
           p.producers, p.composers, p.cinematographers, p.editors,
           p.productionDesigners, p.castingDirectors, p.selfAppearances, p.archiveFootage,
           a.numAkas, a.numRegions, a.regions, a.languages, a.akaTitles,
           e.parentTconst, e.parentTitle, e.seasonNumber, e.episodeNumber
    FROM titres f
    LEFT JOIN ratings        r USING (tconst)
    LEFT JOIN crew_noms      c USING (tconst)
    LEFT JOIN principals_agg p USING (tconst)
    LEFT JOIN akas_agg       a USING (tconst)
    LEFT JOIN episodes       e USING (tconst)
""",
}

if __name__ == "__main__":
    debut = time.time()
    DOSSIER_TEMP.mkdir(parents=True, exist_ok=True)
    base = DOSSIER_TEMP / "jointure_tmp.duckdb"
    base.unlink(missing_ok=True)
    con = duckdb.connect(str(base))
    con.execute("SET memory_limit = '12GB'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{DOSSIER_TEMP / 'spill'}'")

    for nom, requetes in ETAPES.items():
        t = time.time()
        if isinstance(requetes, str):
            requetes = [requetes]
        con.execute(f"CREATE TABLE {nom} AS {requetes[0]}")
        for requete in requetes[1:]:
            con.execute(f"INSERT INTO {nom} {requete}")
        n = con.sql(f"SELECT count(*) FROM {nom}").fetchone()[0]
        print(f"{nom:<15} {n:>12,} lignes  ({time.time() - t:.0f} s)", flush=True)

    nb_lignes, nb_titres = con.sql("SELECT count(*), count(DISTINCT tconst) FROM resultat").fetchone()
    n_base = con.sql("SELECT count(*) FROM titres").fetchone()[0]
    assert nb_lignes == nb_titres == n_base, "Une jointure n'est pas 1-1 : titres perdus ou dupliqués"

    con.execute(f"COPY (SELECT * FROM resultat ORDER BY tconst) TO '{SORTIE}' (HEADER, DELIMITER ',')")
    con.close()
    base.unlink()
    for f in (DOSSIER_TEMP / "spill").glob("*"):
        f.unlink()
    print(f"\n{nb_lignes:,} titres -> {SORTIE}  ({time.time() - debut:.0f} s)")
