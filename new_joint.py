"""Jointure des tables IMDb en un seul CSV : une ligne par titre.

Colonnes gardées :
- title.akas    : title, region, language -> plusieurs lignes par titre, donc chaque colonne
  devient une liste "v1 | v2 | v3", dans le même ordre (ordering) d'une colonne à l'autre.
  Une valeur manquante dans une liste est notée \\N pour garder l'alignement.
- title.basics  : titleType, isAdult, startYear, endYear, runtimeMinutes, genres
- title.crew    : directors, writers -> remplacés par les noms des personnes (via name.basics)
- title.ratings : averageRating, numVotes

Les identifiants (tconst, nconst) servent uniquement aux jointures et ne sont pas exportés.

Structure attendue du projet :
    projet_imdb/
    ├── tsv/       fichiers IMDb bruts (title.akas.tsv, title.basics.tsv, ...)  <- entrée
    ├── output/    imdb_jointure_complete.csv                                   <- sortie
    │   └── temp_duckdb/   fichiers temporaires (supprimés à la fin)
    ├── data/
    └── Notebook/

Usage (depuis la racine du projet ou depuis Notebook/) : python jointure_complete.py
"""
import time
from pathlib import Path

import duckdb



def trouver_racine():
    """Remonte depuis le dossier du script (ou du notebook) jusqu'au dossier qui contient tsv/."""
    depart = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    for dossier in [depart, *depart.parents]:
        if (dossier / "tsv").is_dir():
            return dossier
    raise FileNotFoundError(f"Dossier tsv/ introuvable au-dessus de {depart}")


RACINE = trouver_racine()
DOSSIER_TSV = RACINE / "tsv"
DOSSIER_OUTPUT = RACINE / "output"
DOSSIER_TEMP = DOSSIER_OUTPUT / "temp_duckdb"
SORTIE = DOSSIER_OUTPUT / "imdb_jointure_complete.csv"
SEP = " | "  # séparateur à l'intérieur des cellules multi-valeurs
NB_PAQUETS = 16  # les regroupements sont faits par paquets de titres pour tenir en mémoire

# IMDb : TSV, pas de guillemets, valeurs manquantes "\N". Tout est lu en texte.
OPTIONS = "delim='\t', quote='', escape='', header=true, nullstr='\\N', all_varchar=true"


def table(nom):
    return f"read_csv('{DOSSIER_TSV / (nom + '.tsv')}', {OPTIONS})"


def par_paquets(requete, cle):
    """Une requête par paquet de titres : `{paquet}` est remplacé par un filtre sur hash(cle)."""
    return [
        requete.replace("{paquet}", f"hash({cle}) % {NB_PAQUETS} = {i}") for i in range(NB_PAQUETS)
    ]


# Étapes intermédiaires, enregistrées comme tables dans une base DuckDB temporaire.
# Une liste de requêtes = une table remplie paquet par paquet.
ETAPES = {
"names": f"SELECT nconst, primaryName FROM {table('name.basics')}",
"basics": f"""
    SELECT tconst, titleType, isAdult, startYear, endYear, runtimeMinutes, genres
    FROM {table('title.basics')}
""",
"ratings": f"SELECT * FROM {table('title.ratings')}",
"crew": f"SELECT * FROM {table('title.crew')}",
"akas_brut": f"SELECT titleId, ordering, title, region, language FROM {table('title.akas')}",

# title.akas : une ligne par titre, colonnes en listes triées par ordering
"akas": par_paquets(f"""
    SELECT tconst,
           array_to_string(list_transform(l, x -> coalesce(x.title, '\\N')), '{SEP}')    AS title,
           array_to_string(list_transform(l, x -> coalesce(x.region, '\\N')), '{SEP}')   AS region,
           array_to_string(list_transform(l, x -> coalesce(x.language, '\\N')), '{SEP}') AS language
    FROM (SELECT titleId AS tconst,
                 list_sort(list({{o: ordering::INT, title: title, region: region,
                                  language: language}})) AS l
          FROM akas_brut
          WHERE {{paquet}}
          GROUP BY titleId)
""", "titleId"),

# title.crew : les listes de nconst remplacées par les noms, dans le même ordre
"crew_noms": f"""
    SELECT c.tconst, r.role,
           string_agg(coalesce(n.primaryName, '\\N'), '{SEP}' ORDER BY r.pos) AS noms
    FROM crew c,
         LATERAL (SELECT 'directors' AS role, unnest(string_split(c.directors, ',')) AS nconst,
                         generate_subscripts(string_split(c.directors, ','), 1) AS pos
                  UNION ALL
                  SELECT 'writers', unnest(string_split(c.writers, ',')),
                         generate_subscripts(string_split(c.writers, ','), 1)) r
    LEFT JOIN names n ON n.nconst = r.nconst
    GROUP BY c.tconst, r.role
""",
}

# La grande table finale (une ligne par titre de title.basics), construite par paquets
ETAPES["resultat"] = par_paquets(f"""
SELECT b.tconst,
       a.title, a.region, a.language,
       b.titleType, b.isAdult, b.startYear, b.endYear, b.runtimeMinutes, b.genres,
       d.noms AS directors,
       w.noms AS writers,
       r.averageRating, r.numVotes
FROM basics b
LEFT JOIN akas      a USING (tconst)
LEFT JOIN ratings   r USING (tconst)
LEFT JOIN crew_noms d ON d.tconst = b.tconst AND d.role = 'directors'
LEFT JOIN crew_noms w ON w.tconst = b.tconst AND w.role = 'writers'
WHERE {{paquet}}
""", "b.tconst")

if __name__ == "__main__":
    debut = time.time()
    # Base DuckDB temporaire sur disque : les tables intermédiaires n'ont pas à tenir en RAM
    DOSSIER_TEMP.mkdir(parents=True, exist_ok=True)
    base = DOSSIER_TEMP / "jointure_tmp.duckdb"
    base.unlink(missing_ok=True)
    con = duckdb.connect(str(base))
    con.execute("SET memory_limit = '12GB'")
    con.execute("SET threads = 4")
    con.execute("SET max_temp_directory_size = '100GB'")
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
        print(f"{nom:<10} {n:>12,} lignes  ({time.time() - t:.0f} s)", flush=True)

    # tconst a servi aux jointures : on vérifie qu'il est unique, puis on ne l'exporte pas
    nb_lignes, nb_titres = con.sql("SELECT count(*), count(DISTINCT tconst) FROM resultat").fetchone()
    assert nb_lignes == nb_titres, "Des titres sont dupliqués : une jointure n'est pas 1-1"
    con.execute(
        f"COPY (SELECT * EXCLUDE (tconst) FROM resultat ORDER BY tconst) "
        f"TO '{SORTIE}' (HEADER, DELIMITER ',')"
    )
    con.close()
    base.unlink()

    taille = SORTIE.stat().st_size / 1e9
    print(f"{SORTIE.name} : {nb_lignes:,} lignes, {taille:.1f} Go, en {time.time() - debut:.0f} s")
