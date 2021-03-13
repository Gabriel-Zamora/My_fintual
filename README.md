# My Fintual
## Descripción
Script desarrollado en python que utiliza la [API](https://fintual.cl/api-docs/index.html) de [Fintual](https://fintual.cl/)  para guardar información histórica actualizada de los fondos de Fintual e inversiones personales en una base de datos propia.

La API permite acceder a informacíon actualizada de las inversiones de un usuario (mediante un token de acceso), pero no a su historia. Este script permite realizar consultas periodicas con el propósito de construir información historica de la evolución de una meta.

## API
[El API de Fintual](https://fintualist.com/chile/noticias/el-api-de-fintual/) cuenta con una parte pública y una parte privada, está última requiere contar con un token de acceso (revisar [documentación](https://fintual.cl/api-docs/index.html)). 

El Script accede a la información privada usando las siguientes variables de entorno:
```
FINTUAL_EMAIL="tuemail@tuemail.com"
FINTUAL_TOKEN="abCdeFGHIJKLMNOpQRSt"
```

## Base de datos
La información se guarda en un servidor Postgresql local o remoto. La conexión se puede configurar en el archivo [auth.json](config/auth.json). Donde la contraseña hace referencia a una variable de entorno.

Ejemplo:
```
PSQL_PASS="mypassword"
```
El modelo de datos se puede modificar editando el archivo [BBDD.json](config/BBDD.json):
- Assets: Activos de Fintual
- Funds: Fondos de Fintual por serie FFMM o APV
- Series: Serie de tiempo de precios de fondos de Fintual
- My_goals: Metas de inversión de usuario
- My_series: Serie de tiempo de inversiones de usuario
- My_investments: Serie de tiempo de fondos distribuidos que financian un objetivo de inversión


## Script
- Partida: Ejecutar [start.py](fintual/do/start.py) para crear base de datos, modelo de datos y guardar información pública actualizada.
- Actualización: Ejecutar [update.py](fintual/do/update.py) para guardar información del día tanto pública como privada.
