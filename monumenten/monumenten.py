import asyncio
import csv
import itertools
import os
import signal

import aiofiles
import aiohttp
import pandas as pd
import shapely
from loguru import logger
from rtree import index
from tqdm.asyncio import tqdm

# Bestandslocaties voor invoer en uitvoer CSV's
INVOER_CSV = "temp/verblijfsobjecten.csv"
VERBLIJFSOBJECT_ID_VELDNAAM = "bag_verblijfsobject_id"

UITVOER_CSV = "temp/monumenten.csv"
LOG_BESTAND = "temp/monumenten.log"

# Definieer de batchgrootte voor de queries (hoeveel objecten tegelijk worden verwerkt)
QUERY_BATCH_GROOTTE = 500

# API-sleutel voor toegang tot de Individuele Bevragingen API
INDIVIDUELE_BEVRAGINGEN_API_KEY = os.environ.get("INDIVIDUELE_BEVRAGINGEN_API_KEY")
if not INDIVIDUELE_BEVRAGINGEN_API_KEY:
    raise ValueError(
        "INDIVIDUELE_BEVRAGINGEN_API_KEY environment variable is niet ingesteld."
    )

# Beperken van het aantal gelijktijdige verzoeken via semaphores voor de API's
INDIVIDUELE_BEVRAGINGEN_SEMAPHORE = asyncio.Semaphore(10)
CULTUREEL_ERFGOED_SEMAPHORE = asyncio.Semaphore(2)
KADASTER_SEMAPHORE = asyncio.Semaphore(2)

# SPARQL endpoints voor het opvragen van data over cultureel erfgoed en kadasterobjecten
CULTUREEL_ERFGOED_SPARQL_ENDPOINT = (
    "https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/sparql"
)
KADASTER_SPARQL_ENDPOINT = (
    "https://api.labs.kadaster.nl/datasets/dst/kkg/services/default/sparql"
)

# Endpoint voor Individuele Bevragingen van de BAG (Basisregistratie Adressen en Gebouwen)
INDIVIDUELE_BEVRAGINGEN_ENDPOINT = (
    "https://api.bag.kadaster.nl/lvbag/individuelebevragingen/v2"
)

# Specifieke URL's voor het opvragen van informatie over panden en verblijfsobjecten
PANDEN_URL = f"{INDIVIDUELE_BEVRAGINGEN_ENDPOINT}/panden"
VERBLIJFSOBJECTEN_URL = f"{INDIVIDUELE_BEVRAGINGEN_ENDPOINT}/verblijfsobjecten"

# Headers voor API-verzoeken, inclusief API-sleutel en specificatie van coördinatenstelsels
INDIVIDUELE_BEVRAGINGEN_HEADERS = {
    "Accept-Crs": "epsg:28992",
    "Content-Crs": "epsg:28992",
    "Accept": "application/hal+json",
    "X-Api-Key": INDIVIDUELE_BEVRAGINGEN_API_KEY,
}

LOG_LEVEL = "DEBUG"
logger.remove(0)
logger.add(
    LOG_BESTAND,
    level=LOG_LEVEL,
    colorize=False,
    backtrace=True,
    diagnose=True,
    retention=1,
)

# SPARQL query templates voor het opvragen van Rijksmonumenten en Beschermde Gezichten
RIJKSMONUMENTEN_QUERY_TEMPLATE = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX bag:<http://bag.basisregistraties.overheid.nl/bag/id/>
PREFIX rn:<https://data.cultureelerfgoed.nl/term/id/rn/>
SELECT DISTINCT ?identificatie ?nummer
WHERE {{
    ?monument ceo:heeftJuridischeStatus rn:b2d9a59a-fe1e-4552-9a05-3c2acddff864 ;
              ceo:rijksmonumentnummer ?nummer ;
              ceo:heeftBasisregistratieRelatie ?basisregistratieRelatie .
    ?basisregistratieRelatie ceo:heeftBAGRelatie ?bagRelatie .
    ?bagRelatie ceo:verblijfsobjectIdentificatie ?identificatie .
    VALUES ?identificatie {{ {identificaties} }}
}}
"""

BESCHERMDE_GEZICHTEN_QUERY = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX rn:<https://data.cultureelerfgoed.nl/term/id/rn/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT DISTINCT ?gezicht ?naam ?gezichtWKT
WHERE {{
  ?gezicht
      ceo:heeftGeometrie ?gezichtGeometrie ;
      ceo:heeftGezichtsstatus rn:fd968529-bf70-4afa-8564-7c6c2fcfcc54;
      ceo:heeftNaam/ceo:naam ?naam.
  ?gezichtGeometrie geo:asWKT ?gezichtWKT.
}}
"""

VERBLIJFSOBJECTEN_QUERY_TEMPLATE = """
PREFIX sor: <https://data.kkg.kadaster.nl/sor/model/def/>
PREFIX nen3610: <https://data.kkg.kadaster.nl/nen3610/model/def/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT DISTINCT ?identificatie ?verblijfsobjectWKT
WHERE {{
  ?verblijfsobject sor:geregistreerdMet/nen3610:identificatie ?identificatie .
  ?verblijfsobject geo:hasGeometry/geo:asWKT ?verblijfsobjectWKT.
  FILTER (?identificatie IN ( {identificaties} ))
}}
"""


def vergelijk_adressen(adres1, adres2):
    return (
        adres1["postcode"] == adres2["postcode"]
        and adres1["huisnummer"] == adres2["huisnummer"]
        and adres1.get("huisletter") == adres2.get("huisletter")
    )


# Functie voor het ophalen van data via een API met retries in geval van fouten
async def ophalen_met_retry(sessie, url, headers, params, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            async with INDIVIDUELE_BEVRAGINGEN_SEMAPHORE:
                async with sessie.get(url, headers=headers, params=params) as response:
                    if response.status == 429:
                        rate_limit_reset = int(
                            response.headers.get("RateLimit-Reset", 1)
                        )
                        logger.warning(
                            f"Rate limited. Wachten voor {rate_limit_reset} seconden..."
                        )
                        await asyncio.sleep(rate_limit_reset)
                        retries += 1
                    elif response.status >= 400:
                        logger.error(
                            f"Fout opgetreden met statuscode: {response.status}"
                        )
                        return None
                    else:
                        return await response.json()
        except aiohttp.ClientConnectionError as e:
            logger.error(
                f"Verbindingsfout: {e}. Opnieuw proberen {retries + 1}/{max_retries}..."
            )
            retries += 1
            await asyncio.sleep(2**retries)
    raise Exception(
        f"Fout opgetreden na {max_retries} pogingen door verbindingsproblemen of rate limiting."
    )


# Functie voor het vinden van alternatieve verblijfsobjecten als het originele niet gevonden wordt
async def vind_alternatieve_verblijfsobjecten(oorspronkelijke_verblijfsobject_id):
    async with aiohttp.ClientSession() as sessie:
        panden_data = await ophalen_met_retry(
            sessie,
            PANDEN_URL,
            headers=INDIVIDUELE_BEVRAGINGEN_HEADERS,
            params={
                "adresseerbaarObjectIdentificatie": oorspronkelijke_verblijfsobject_id
            },
        )
        if panden_data:
            panden = panden_data["_embedded"]["panden"]
            for pand in panden:
                pand_id = pand["pand"]["identificatie"]
                verblijfsobjecten_data = await ophalen_met_retry(
                    sessie,
                    VERBLIJFSOBJECTEN_URL,
                    headers=INDIVIDUELE_BEVRAGINGEN_HEADERS,
                    params={"expand": "true", "pandIdentificatie": pand_id},
                )

                if not verblijfsobjecten_data:
                    logger.warning(
                        f"Fout bij het ophalen van verblijfsobjecten voor verblijfsobject {oorspronkelijke_verblijfsobject_id}"
                    )
                    continue

                verblijfsobjecten = verblijfsobjecten_data["_embedded"][
                    "verblijfsobjecten"
                ]

                origineel_adres = next(
                    (
                        verblijfsobject_info["_embedded"]["heeftAlsHoofdAdres"][
                            "nummeraanduiding"
                        ]
                        for verblijfsobject_info in verblijfsobjecten
                        if verblijfsobject_info["verblijfsobject"]["identificatie"]
                        == oorspronkelijke_verblijfsobject_id
                    ),
                    None,
                )

                if not origineel_adres:
                    logger.warning(
                        f"Geen adres gevonden voor verblijfsobject {oorspronkelijke_verblijfsobject_id}"
                    )
                    continue

                gevonden_alternatief = False
                for verblijfsobject_info in verblijfsobjecten:
                    verblijfsobject = verblijfsobject_info["verblijfsobject"]
                    if (
                        verblijfsobject["identificatie"]
                        == oorspronkelijke_verblijfsobject_id
                    ):
                        continue  # Het originele verblijfsobject id slaan we over

                    adres = verblijfsobject_info["_embedded"]["heeftAlsHoofdAdres"][
                        "nummeraanduiding"
                    ]

                    if vergelijk_adressen(adres, origineel_adres):
                        gevonden_alternatief = True
                        yield {
                            "origineel_verblijfsobject_id": oorspronkelijke_verblijfsobject_id,
                            "alternatief_verblijfsobject_id": verblijfsobject[
                                "identificatie"
                            ],
                            "status": verblijfsobject["status"],
                            "postcode": adres.get("postcode"),
                            "huisnummer": adres.get("huisnummer"),
                            "huisletter": adres.get("huisletter") or "",
                        }

                if not gevonden_alternatief:
                    logger.warning(
                        f"Geen alternatief adres gevonden voor verblijfsobject {oorspronkelijke_verblijfsobject_id}"
                    )
        else:
            logger.warning(
                f"Fout bij het ophalen van panden voor verblijfsobject {oorspronkelijke_verblijfsobject_id}"
            )


async def query_rijksmonumenten(sessie, identificaties):
    async with CULTUREEL_ERFGOED_SEMAPHORE:
        identificaties_str = " ".join(
            f'"{identificatie}"' for identificatie in identificaties
        )
        query = RIJKSMONUMENTEN_QUERY_TEMPLATE.format(identificaties=identificaties_str)
        params = {"query": query, "format": "json"}
        retries = 3
        for poging in range(retries):
            try:
                async with sessie.post(
                    CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data=params
                ) as response:
                    response.raise_for_status()
                    resultaat = await response.json()
                    if isinstance(resultaat, list):
                        return {
                            entry["identificatie"]: entry["nummer"]
                            for entry in resultaat
                        }
                    else:
                        logger.warning(
                            f"Onverwacht antwoord bij poging {poging + 1}: {resultaat}"
                        )
                        return {}
            except aiohttp.ClientResponseError as e:
                if poging != retries - 1:
                    logger.warning(
                        f"Poging {poging + 1}/{retries} voor rijksmonumenten query mislukt. Opnieuw proberen over 1 seconde..."
                    )
                    await asyncio.sleep(1)
                else:
                    raise


async def query_verblijfsobjecten(sessie, identificaties):
    async with KADASTER_SEMAPHORE:
        identificaties_str = ", ".join(
            f'"{identificatie}"' for identificatie in identificaties
        )
        query = VERBLIJFSOBJECTEN_QUERY_TEMPLATE.format(
            identificaties=identificaties_str
        )
        params = {"query": query, "format": "json"}
        retries = 3
        for poging in range(retries):
            try:
                async with sessie.post(
                    KADASTER_SPARQL_ENDPOINT, data=params
                ) as response:
                    response.raise_for_status()
                    resultaat = await response.json()
                    if isinstance(resultaat, list):
                        return resultaat
                    else:
                        logger.warning(
                            f"Onverwacht antwoordformaat bij poging {poging + 1}: {resultaat}"
                        )
                        return []
            except aiohttp.ClientResponseError as e:
                if poging != retries - 1:
                    logger.warning(
                        f"Poging {poging + 1}/{retries} voor verblijfsobjecten query mislukt. Opnieuw proberen in 1 seconde..."
                    )
                    await asyncio.sleep(1)
                else:
                    raise


async def query_beschermde_gebieden(sessie):
    retries = 3
    for poging in range(retries):
        try:
            params = {"query": BESCHERMDE_GEZICHTEN_QUERY, "format": "json"}
            async with sessie.post(
                CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data=params
            ) as response:
                response.raise_for_status()
                resultaat = await response.json()
                if isinstance(resultaat, list):
                    return resultaat
                else:
                    logger.warning(
                        f"Onverwacht antwoordformaat bij poging {poging + 1}: {resultaat}"
                    )
                    return []
        except aiohttp.ClientResponseError as e:
            if poging != retries - 1:
                logger.warning(
                    f"Poging {poging + 1}/{retries} voor beschermde gebieden query mislukt. Opnieuw proberen in 1 seconde..."
                )
                await asyncio.sleep(1)
            else:
                raise


async def verzamel_data(sessie, identificaties_batch):
    rijksmonumenten_taak = query_rijksmonumenten(sessie, identificaties_batch)
    verblijfsobjecten_taak = query_verblijfsobjecten(sessie, identificaties_batch)
    rijksmonumenten, verblijfsobjecten = await asyncio.gather(
        rijksmonumenten_taak, verblijfsobjecten_taak
    )
    return rijksmonumenten, verblijfsobjecten


ONTBREKENDE_IDENTIFICATIES = set()
IDENTIFICATIE_MAPPING = {}


async def voer_queries_uit(sessie, df, queue):
    beschermde_gebieden = await query_beschermde_gebieden(sessie)
    beschermde_gezicht_geometrieën = [
        (gebied["naam"], shapely.io.from_wkt(gebied["gezichtWKT"]))
        for gebied in beschermde_gebieden
    ]
    idx = index.Index()
    for pos, (naam, geom) in enumerate(beschermde_gezicht_geometrieën):
        idx.insert(pos, shapely.bounds(geom))
    with tqdm(total=len(df), desc="Verwerking verzoeken ") as pbar:
        for i in range(0, len(df), QUERY_BATCH_GROOTTE):
            batch = df.iloc[i : i + QUERY_BATCH_GROOTTE]
            identificaties_batch = batch[VERBLIJFSOBJECT_ID_VELDNAAM].tolist()

            rijksmonumenten, verblijfsobjecten = await verzamel_data(
                sessie, identificaties_batch
            )
            verblijfsobject_identificaties = {
                verblijfsobject["identificatie"]
                for verblijfsobject in verblijfsobjecten
            }
            ontbrekende_identificaties_batch = (
                set(identificaties_batch) - verblijfsobject_identificaties
            )
            ONTBREKENDE_IDENTIFICATIES.update(ontbrekende_identificaties_batch)

            if ontbrekende_identificaties_batch:
                taken = [
                    vind_alternatieve_verblijfsobjecten(
                        oorspronkelijke_verblijfsobject_id
                    )
                    for oorspronkelijke_verblijfsobject_id in ontbrekende_identificaties_batch
                ]
                fallback_resultaten = list(
                    itertools.chain.from_iterable(
                        await asyncio.gather(*[consume_generator(tak) for tak in taken])
                    )
                )
                for fallback_resultaat in fallback_resultaten:
                    logger.info(
                        f"{fallback_resultaat['origineel_verblijfsobject_id']} werd niet gevonden in het SPARQL endpoint. Alternatief gevonden: {fallback_resultaat['alternatief_verblijfsobject_id']} ({fallback_resultaat['status']})"
                    )
                IDENTIFICATIE_MAPPING.update(
                    {
                        item["origineel_verblijfsobject_id"]: item[
                            "alternatief_verblijfsobject_id"
                        ]
                        for item in fallback_resultaten
                    }
                )
                fallback_identificaties_batch = [
                    fallback_resultaat["alternatief_verblijfsobject_id"]
                    for fallback_resultaat in fallback_resultaten
                ]
                (
                    rijksmonumenten_fallback,
                    verblijfsobjecten_fallback,
                ) = await verzamel_data(sessie, fallback_identificaties_batch)

                rijksmonumenten.update(rijksmonumenten_fallback)
                verblijfsobjecten.extend(verblijfsobjecten_fallback)

            verblijfsobject_geometrieën = [
                (
                    verblijfsobject["identificatie"],
                    shapely.io.from_wkt(verblijfsobject["verblijfsobjectWKT"]),
                )
                for verblijfsobject in verblijfsobjecten
            ]

            def controleer_punt(punt_id, punt_geom):
                mogelijke_overeenkomsten = idx.intersection(shapely.bounds(punt_geom))
                for match in mogelijke_overeenkomsten:
                    naam, geom = beschermde_gezicht_geometrieën[match]
                    if shapely.contains(geom, punt_geom):
                        return naam

            verblijfsobjecten_in_beschermd_gezicht = {
                identificatie: controleer_punt(identificatie, punt_geom)
                for identificatie, punt_geom in verblijfsobject_geometrieën
                if controleer_punt(identificatie, punt_geom) is not None
            }
            pbar.update(len(batch))
            await queue.put(
                {
                    "batch": batch,
                    "rijksmonumenten": rijksmonumenten,
                    "beschermde_gezichten": verblijfsobjecten_in_beschermd_gezicht,
                }
            )
    await queue.put(None)


async def verwerk_batch_resultaten(writer: csv.DictWriter, queue, pbar):
    while True:
        entry = await queue.get()
        if entry is None:
            break
        batch = entry.get("batch")
        rijksmonumenten = entry.get("rijksmonumenten")
        beschermde_gezichten = entry.get("beschermde_gezichten")
        for _, rij in batch.iterrows():
            verblijfsobject_id = IDENTIFICATIE_MAPPING.get(
                rij[VERBLIJFSOBJECT_ID_VELDNAAM], rij[VERBLIJFSOBJECT_ID_VELDNAAM]
            )

            monument_nummer = rijksmonumenten.get(verblijfsobject_id)
            beschermd_gezicht_naam = beschermde_gezichten.get(verblijfsobject_id)
            await writer.writerow(
                {
                    **rij,
                    "is_rijksmonument": monument_nummer is not None,
                    "rijksmonument_url": f"https://www.monumenten.nl/monument/{monument_nummer}"
                    if monument_nummer
                    else "",
                    "is_beschermd_gezicht": beschermd_gezicht_naam is not None,
                    "beschermd_gezicht_naam": beschermd_gezicht_naam or "",
                    "fallback_bag_verblijfsobject_id": (
                        verblijfsobject_id
                        if verblijfsobject_id != rij[VERBLIJFSOBJECT_ID_VELDNAAM]
                        else ""
                    ),
                }
            )
            pbar.update(1)


async def consume_generator(generator):
    resultaten = []
    async for resultaat in generator:
        resultaten.append(resultaat)
    return resultaten


async def main():
    df = pd.read_csv(INVOER_CSV, dtype={VERBLIJFSOBJECT_ID_VELDNAAM: str})

    headers = df.columns.tolist() + [
        "is_rijksmonument",
        "rijksmonument_url",
        "is_beschermd_gezicht",
        "beschermd_gezicht_naam",
        "fallback_bag_verblijfsobject_id",
    ]
    queue = asyncio.Queue()
    async with aiofiles.open(UITVOER_CSV, mode="w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=headers,
        )
        await writer.writeheader()
        async with aiohttp.ClientSession() as sessie:
            query_taak = voer_queries_uit(sessie, df, queue)
            with tqdm(total=len(df), desc="Verwerking resultaten") as pbar:
                proces_taak = verwerk_batch_resultaten(writer, queue, pbar)
                await asyncio.gather(query_taak, proces_taak)


def handle_sigint(signal, frame):
    logger.info("SIGINT ontvangen, taken annuleren...")
    for taak in asyncio.all_tasks():
        taak.cancel()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigint)
    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        logger.info("Taken zijn geannuleerd.")
