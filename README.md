1# Projet Python pour la Data Science
 Auteurs : *Isaline, Romain, Vincent Vasylchenko*  

# Sujet :
<div align="justify">
En 2020, les élections municipales ont vu l'émergence d’Europe Écologie Les Verts dans plusieurs grandes villes de France, auparavant aux mains de partis plus traditionnels. Les élections se tiennent dans un contexte de crise sanitaire liée à la propagation de la Covid-19 dans le pays, les premières municipales pour le groupe macroniste au pouvoir et sont marquées par un taux d’abstention record pour des municipales : 55 % au premier tour et 58 % au second, soit plus de 20 points de plus qu’au second tour des élections municipales de 2014.
L'idée est d'étudier les résultats électoraux aux municipales et de mettre en avant un lien ou non entre l'évolution du vote en faveur des écologistes par rapport à l'évolution de gentrification, en supposant qu'une ville déjà fortement gentrifiée n'évolueront pas spécifiquement leur vote, alors que celles en cours de gentrification voient l'arrivée et l'attractivité d'une population plus enclin à ce poste. Nous nous sommes également demandé si les résultats ne seraient pas le fait de l'évolution d'un vote de gauche en faveur de EELV, portée par leur percée inattendue aux élections européennes de 2019.
Etant donne que les nuances politiques sont indiqués pour les communes de plus de 3500 habitants et que notre étude peut se restreindre à ces communes, nous filtrons sur les communes de plus de 3500 habitants.
Pour cela, les données mobilisées sont celles des résultats électoraux de 2014 et 2020, accessibles sur data.gouv.fr, à croiser avec l'évolution de données socio-démographique comme la catégorie socio-professionnelle, l'éducation, l'emploi.

# Problématique : 
Est-il possible de corréler l'évolution du vote écolo dans les grandes villes avec des variables socio-démographiques ?

# Modèle utilisé : 

# Données utilisées :
- [Ministère de l'intérieur](https://www.data.gouv.fr/pages/donnees-des-elections-et-referendums/) (Base de Données sur les élections et référendums en France), on s'intéresse aux bases de résultats des élections de 2014 et 2020.
- [Meteonet](https://meteonet.umr-cnrm.fr/), données météo fournies par Météo France toutes les 6 minutes pour 532 stations dans le quart Sud-Est de la France.
- [Base de données sur les communes françaises](https://www.data.gouv.fr/fr/datasets/communes-de-france-base-des-codes-postaux/), contenant notamment leurs coordonnées GPS.
- [Base de données Geojson des forêts françaises](https://transcode.geo.data.gouv.fr/services/5e2a1f74fa4268bc255efbc3/feature-types/ms:PARC_PUBL_FR?format=GeoJSON&projection=WGS84)

