## get_unemployment_data.py

- El script no admite parametros y siempre produce 2 .csv, tomando datos desde 1990 hasta el ultimo año disponible
- En el caso de solo necesitar un csv se puede comentar la otra funcion
- Modulos necesarios: pandas y urllib
- Algunas versiones de Python pueden no soportar HTTPS y arrojar el siguiente error: urllib.error.URLError: <urlopen error unknown url type: https> 
pero no deberia ser el caso con versiones recientes
- Hay dos .csv en el repo como ejemplo de resultado del script. El tiempo total que tarda en correr es de aprox. 10 min
