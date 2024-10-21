# Monumenten

Dit Python-script verzamelt gegevens over rijksmonumenten en beschermde gezichten. Het maakt gebruik van verschillende SPARQL-eindpunten en de BAG API.

## Dependencies

Zorg ervoor dat je de dependencies hebt geïnstalleerd:

Je kunt deze installeren met pip:

```bash
pip install -r requirements.txt
```

## Configuratie

In het script zijn enkele configuratie-instellingen die je kunt aanpassen:

- `INVOER_CSV`: Het pad naar het invoer CSV-bestand. Default `temp/verblijfsobjecten.csv`.
- `VERBLIJFSOBJECT_ID_VELDNAAM`: De naam van het veld in het CSV-bestand dat de verblijfsobjectidentificatie bevat. Default `bag_verblijfsobject_id`
- `UITVOER_CSV`: Het pad naar het uitvoer CSV-bestand. Default `temp/monumenten.csv`
- `LOG_BESTAND`: Het pad naar het logbestand. Default `temp/monumenten.log`

## Uitvoer CSV

De uitvoer CSV (`temp/monumenten.csv`) bevat alle data van de invoer csv met daar aan toegevoegd de volgende kolommen:

- `is_rijksmonument`: Een boolean waarde (True/False) die aangeeft of het verblijfsobject een rijksmonument is.
- `rijksmonument_url`: Een URL naar de specifieke pagina van het rijksmonument op de website van monumenten.nl, indien van toepassing.
- `is_beschermd_gezicht`: Een boolean waarde (True/False) die aangeeft of het verblijfsobject onderdeel uitmaakt van een beschermd gezicht.
- `beschermd_gezicht_naam`: De naam van het beschermde gezicht, indien van toepassing.
- `fallback_bag_verblijfsobject_id`: Het alternatieve verblijfsobject ID dat is gevonden als het oorspronkelijke ID niet gevonden werd.

| bag_verblijfsobject_id | is_rijksmonument | rijksmonument_url                                                                      | is_beschermd_gezicht | beschermd_gezicht_naam          |
| ---------------------- | ---------------- | -------------------------------------------------------------------------------------- | -------------------- | ------------------------------- |
| 0599010000360091       | True             | [https://www.monumenten.nl/monument/524327](https://www.monumenten.nl/monument/524327) | False                |                                 |
| 0599010000486642       | False            |                                                                                        | False                |                                 |
| 0599010000360022       | True             | [https://www.monumenten.nl/monument/524327](https://www.monumenten.nl/monument/524327) | False                |                                 |
| 0599010000360096       | True             | [https://www.monumenten.nl/monument/524327](https://www.monumenten.nl/monument/524327) | False                |                                 |
| 0599010000183527       | True             | [https://www.monumenten.nl/monument/32807](https://www.monumenten.nl/monument/32807)   | True                 | Rotterdam - Scheepvaartkwartier |
| 0599010400025880       | False            |                                                                                        | False                |                                 |
| 0599010000281115       | False            |                                                                                        | True                 | Kralingen - Midden              |
