# =================================================================
#  DK-DE-OnPrem-CustomPlatform
#  Script      : generate_ais_data.py
#  Description : Générateur de données AIS simulées
#                pour les ports français
#  Usage       : python generate_ais_data.py
# =================================================================

import json
import random
from datetime import datetime, timedelta

# -----------------------------------------------------------------
# Données de référence
# -----------------------------------------------------------------

PORTS_FRANCE = [
    {"code_locode": "FRMRS", "nom": "Marseille / Fos-sur-Mer", "lat": 43.296, "lon": 5.381},
    {"code_locode": "FRLEH", "nom": "Le Havre",                 "lat": 49.492, "lon": 0.107},
    {"code_locode": "FRURO", "nom": "Rouen",                    "lat": 49.443, "lon": 1.099},
    {"code_locode": "FRDKK", "nom": "Dunkerque",                "lat": 51.035, "lon": 2.377},
    {"code_locode": "FRNTS", "nom": "Nantes / Saint-Nazaire",   "lat": 47.217, "lon": -1.553},
]

TYPES_NAVIRES = [
    "PORTE_CONTENEURS",
    "VRAQUIER",
    "TANKER",
    "RORO",
    "FERRY"
]

PAVILLONS = ["FR", "PA", "LR", "BS", "MT", "CY", "MH", "SG", "NL", "DE",
             "GR", "IT", "ES", "PT", "NO", "DK", "GB", "US", "CN", "JP"]

ARMATEURS = [
    "CMA CGM", "MSC", "Maersk", "Bolloré Logistics",
    "Louis Dreyfus", "Total Energies", "Brittany Ferries",
    "DFDS", "Hapag-Lloyd", "Evergreen", "Yang Ming",
    "ONE", "Cosco", "PIL", "Wan Hai", "OOCL",
    "Zim", "HMM", "SM Line", "TS Lines"
]

PORTS_INTERNATIONAUX = [
    {"code": "NLRTM", "nom": "Rotterdam"},
    {"code": "DEHAM", "nom": "Hambourg"},
    {"code": "CNSHA", "nom": "Shanghai"},
    {"code": "SGSIN", "nom": "Singapour"},
    {"code": "USLAX", "nom": "Los Angeles"},
    {"code": "GBFXT", "nom": "Felixstowe"},
    {"code": "BEANR", "nom": "Anvers"},
    {"code": "ESALG", "nom": "Algeciras"},
    {"code": "CNNGB", "nom": "Ningbo"},
    {"code": "KRPUS", "nom": "Busan"},
    {"code": "JPYOK", "nom": "Yokohama"},
    {"code": "AEDXB", "nom": "Dubai"},
    {"code": "EGPSD", "nom": "Port Said"},
    {"code": "MAPTM", "nom": "Tanger Med"},
    {"code": "ITVCE", "nom": "Venise"},
]

STATUTS_NAVIGATION = [
    "EN_ROUTE", "AU_MOUILLAGE", "A_QUAI",
    "MANŒUVRE", "EN_INSPECTION"
]

PREFIXES_NOMS = {
    "PORTE_CONTENEURS" : ["MSC", "CMA CGM", "EVER", "MAERSK", "ONE",
                          "COSCO", "OOCL", "HAPAG", "YANG MING", "ZIM"],
    "VRAQUIER"         : ["BULK", "CAPE", "PACIFIC", "ATLANTIC", "OCEAN",
                          "NORDIC", "POLAR", "BALTIC", "ASIAN", "GLOBAL"],
    "TANKER"           : ["CRUDE", "PACIFIC", "GULF", "NORDIC", "MARINE",
                          "ALPINE", "ARCTIC", "TITAN", "ATLAS", "COSMOS"],
    "RORO"             : ["CARRIER", "EXPRESS", "LINK", "BRIDGE", "VIKING",
                          "AURORA", "CELTIC", "IBERIAN", "ADRIATIC", "AEGEAN"],
    "FERRY"            : ["NORMANDIE", "BRETAGNE", "ARMORIQUE", "PONT", "MONT",
                          "COTENTIN", "BARFLEUR", "COTENTIN", "ILE", "CAP"]
}

SUFFIXES_NOMS = [
    "STAR", "SPIRIT", "WIND", "SKY", "SEA",
    "I", "II", "III", "PIONEER", "EXPLORER",
    "VICTORY", "GLORY", "PRIDE", "HONOR", "GRACE",
    "ALPHA", "BETA", "GAMMA", "DELTA", "OMEGA",
    "ACE", "LEADER", "CHAMPION", "TITAN", "GIANT"
]


# -----------------------------------------------------------------
# Générateurs
# -----------------------------------------------------------------

def generer_mmsi(index):
    """Génère un MMSI unique basé sur l'index"""
    base = 200000000 + index
    return str(base)


def generer_imo(index):
    """Génère un IMO unique basé sur l'index"""
    base = 9000000 + index
    return str(base)


def generer_nom_navire(type_navire):
    """Génère un nom de navire réaliste"""
    prefix = random.choice(PREFIXES_NOMS.get(type_navire, ["SHIP"]))
    suffix = random.choice(SUFFIXES_NOMS)
    return f"{prefix} {suffix}"


def generer_navires(nb_navires=850):
    """Génère une liste de navires distincts"""
    navires = []
    for i in range(nb_navires):
        type_navire = random.choice(TYPES_NAVIRES)
        armateur    = random.choice(ARMATEURS)
        navires.append({
            "mmsi"              : generer_mmsi(i),
            "imo"               : generer_imo(i),
            "nom_navire"        : generer_nom_navire(type_navire),
            "type_navire"       : type_navire,
            "pavillon"          : random.choice(PAVILLONS),
            "armateur"          : armateur,
            "capacite_teu"      : random.randint(1000, 24000) if type_navire == "PORTE_CONTENEURS" else 0,
            "annee_construction": random.randint(2000, 2023)
        })
    return navires


def generer_mouvement(index, navire, port, date_base):
    """Génère un mouvement de navire dans un port"""

    duree_sejour_h = round(random.uniform(12, 168), 1)
    date_arrivee   = date_base + timedelta(
        days=random.randint(0, 729),   # 2 ans : 2023 + 2024
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    date_depart = date_arrivee + timedelta(hours=duree_sejour_h)

    lat = port["lat"] + random.uniform(-0.05, 0.05)
    lon = port["lon"] + random.uniform(-0.05, 0.05)

    port_origine = random.choice(PORTS_INTERNATIONAUX)

    return {
        "id_mouvement"      : index + 1,
        "mmsi"              : navire["mmsi"],
        "imo"               : navire["imo"],
        "nom_navire"        : navire["nom_navire"],
        "type_navire"       : navire["type_navire"],
        "pavillon"          : navire["pavillon"],
        "armateur"          : navire["armateur"],
        "latitude"          : round(lat, 6),
        "longitude"         : round(lon, 6),
        "vitesse_noeuds"    : round(random.uniform(0, 3), 1),
        "cap_degres"        : round(random.uniform(0, 360), 1),
        "statut_navigation" : random.choice(STATUTS_NAVIGATION),
        "port_destination"  : port["code_locode"],
        "port_origine"      : port_origine["code"],
        "timestamp_position": date_arrivee.strftime("%Y-%m-%d %H:%M:%S"),
        "date_arrivee"      : date_arrivee.strftime("%Y-%m-%d %H:%M:%S"),
        "date_depart"       : date_depart.strftime("%Y-%m-%d %H:%M:%S"),
        "duree_sejour_h"    : duree_sejour_h,
        "poids_brut_tonnes" : round(random.uniform(1000, 200000), 1),
        "nb_conteneurs"     : random.randint(100, 5000) if navire["type_navire"] == "PORTE_CONTENEURS" else 0
    }


# -----------------------------------------------------------------
# Génération principale
# -----------------------------------------------------------------

def generer_dataset(nb_mouvements=15000, nb_navires=850):
    """Génère le dataset complet"""

    print(f"Génération de {nb_navires} navires distincts...")
    navires = generer_navires(nb_navires)

    print(f"Génération de {nb_mouvements} mouvements AIS...")
    date_base  = datetime(2023, 1, 1)
    mouvements = []

    for i in range(nb_mouvements):
        navire    = random.choice(navires)
        port      = random.choice(PORTS_FRANCE)
        mouvement = generer_mouvement(i, navire, port, date_base)
        mouvements.append(mouvement)

        if (i + 1) % 1000 == 0:
            print(f"  {i + 1}/{nb_mouvements} mouvements générés...")

    output = {
        "metadata": {
            "source"          : "SIMULATED_AIS",
            "date_generation" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "nb_mouvements"   : nb_mouvements,
            "nb_navires"      : nb_navires,
            "periode"         : "2023-2024",
            "ports_couverts"  : [p["code_locode"] for p in PORTS_FRANCE]
        },
        "navires"   : navires,
        "mouvements": mouvements
    }

    chemin = "/tmp/ais_simule.json"
    with open(chemin, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)

    taille_mb = round(len(json.dumps(output).encode()) / 1024 / 1024, 2)
    print(f"\n Fichier généré : {chemin}")
    print(f"   Taille         : {taille_mb} MB")
    print(f"   Navires        : {nb_navires}")
    print(f"   Mouvements     : {nb_mouvements}")
    print(f"   Ports couverts : {[p['code_locode'] for p in PORTS_FRANCE]}")

    return chemin, output

# -----------------------------------------------------------------
# Génération hebdomadaire incrémentale
# -----------------------------------------------------------------
def generer_dataset_semaine(date_debut_semaine, nb_navires=850):
    """
    Génère les mouvements d'UNE semaine donnée.
    Volume aléatoire entre 250 et 350 mouvements.

    Args:
        date_debut_semaine (datetime): lundi de la semaine
        nb_navires (int): pool de navires existants (stable)
    """

    nb_mouvements = random.randint(1200, 1500)
    print(f"Génération de {nb_mouvements} mouvements pour la semaine du {date_debut_semaine.strftime('%Y-%m-%d')}")

    # Réutilise les navires stables (MMSI déterministes)
    navires = generer_navires(nb_navires)

    mouvements = []
    for i in range(nb_mouvements):
        navire = random.choice(navires)
        port   = random.choice(PORTS_FRANCE)

        # Mouvement réparti sur les 7 jours de la semaine
        jour_offset    = random.randint(0, 6)
        date_arrivee   = date_debut_semaine + timedelta(
            days=jour_offset,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        mouvement = generer_mouvement(i, navire, port, date_debut_semaine)
        # Surcharge le timestamp avec la date de la semaine
        mouvement["timestamp_position"] = date_arrivee.strftime("%Y-%m-%d %H:%M:%S")
        mouvement["date_arrivee"]       = date_arrivee.strftime("%Y-%m-%d %H:%M:%S")

        mouvements.append(mouvement)

    output = {
        "metadata": {
            "source"          : "SIMULATED_AIS_WEEKLY",
            "date_generation" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "semaine_debut"   : date_debut_semaine.strftime("%Y-%m-%d"),
            "nb_mouvements"   : nb_mouvements,
            "nb_navires"      : nb_navires
        },
        "navires"   : navires,
        "mouvements": mouvements
    }

    chemin = "/tmp/ais_semaine.json"
    with open(chemin, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)

    print(f"Fichier généré : {chemin} ({nb_mouvements} mouvements)")
    return chemin, output

if __name__ == "__main__":
    chemin, data = generer_dataset(nb_mouvements=15000, nb_navires=850)

    print("\n--- Aperçu des 3 premiers mouvements ---")
    for m in data["mouvements"][:3]:
        print(f"""
  Navire  : {m['nom_navire']} ({m['type_navire']})
  Port    : {m['port_destination']}
  Arrivée : {m['date_arrivee']}
  Départ  : {m['date_depart']}
  Durée   : {m['duree_sejour_h']}h
  Poids   : {m['poids_brut_tonnes']} tonnes
        """)

    print("\n--- Répartition par type de navire ---")
    types = {}
    for n in data["navires"]:
        t = n["type_navire"]
        types[t] = types.get(t, 0) + 1
    for t, c in sorted(types.items()):
        print(f"  {t:25} : {c}")

    print("\n--- Répartition par port ---")
    ports = {}
    for m in data["mouvements"]:
        p = m["port_destination"]
        ports[p] = ports.get(p, 0) + 1
    for p, c in sorted(ports.items()):
        print(f"  {p} : {c}")