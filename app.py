
import os
import requests
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
from flask import Flask, jsonify, request
from matplotlib.ticker import FuncFormatter
from sqlalchemy import create_engine, text, MetaData, Table

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({"Choo Choo": "Welcome to your Flask app 🚅"})

@app.route('/get_demand')
def db():
    #PETICION API REE

    dataframes = []
    group_days = 25

    current_date = datetime.fromisoformat(request.args['fecha_inicio'])
    datetime_fin = datetime.fromisoformat(request.args['fecha_fin'])

    while current_date <= datetime_fin:
        fecha_inicio_actual = current_date
        fecha_fin_actual = current_date + timedelta(days=group_days)

        if fecha_fin_actual > datetime_fin:
            fecha_fin_actual = datetime_fin

        url = f"https://apidatos.ree.es/en/datos/demanda/evolucion?start_date={fecha_inicio_actual.strftime('%Y-%m-%dT%H:%M')}&end_date={fecha_fin_actual.strftime('%Y-%m-%dT%H:%M')}&time_trunc=hour&geo_trunc=electric_system&geo_limit=peninsular&geo_ids=8741"

        response = requests.get(url)
        data = response.json()

        if int(str(response).split("[")[1].split("]")[0]) == 200:
            raw = data["included"][0]["attributes"]["values"]

            value_list = [round(float(r["value"]), 3) for r in raw]
            date_list = [datetime.fromisoformat(r["datetime"].split(".")[0]) for r in raw]
            year_list = [int(r["datetime"].split("-")[0]) for r in raw]
            month_list = [int(r["datetime"].split("-")[1]) for r in raw]
            day_list = [int(r["datetime"].split("-")[2].split("T")[0]) for r in raw]
            hour_list = [int(r["datetime"].split("T")[1].split(".")[0].split(":")[0]) for r in raw]

            dicc_electric_demand = {
                "value": value_list,
                "date": date_list,
                "year": year_list,
                "month": month_list,
                "day": day_list,
                "hour": hour_list
            }
            df_electric_demand = pd.DataFrame(dicc_electric_demand)
            dataframes.append(df_electric_demand)

        else:
            print(response)
            print(data)
            break

        current_date += timedelta(days=group_days)
        current_date += timedelta(days=1)  # Agregar 1 día para evitar solapamiento

    df = pd.concat(dataframes, ignore_index=True)

    time = request.args['time']
    plot_type = request.args['plot_type']

    plt.figure(figsize=(6.5, 4.5))

    if time == "year":
        title = "Años"
    elif time == "month":
        title = "Meses del año"
    elif time == "day":
        title = "Días del mes"
    elif time == "hour":
        title = "Horas del día"

    if plot_type == "barplot":
        sns.barplot(data=df, x=time, y="value", errorbar=None, color= "blue")
    elif plot_type == "lineplot":
        sns.lineplot(data=df, x=time, y="value", errorbar=None, marker="o", color= "blue")

    # Función personalizada para formatear los valores del eje y
    def format_y_axis(value, _):
        if value >= 1000:
            value = f"{value/1000:.0f}K"
        return value

    # Ajustar el formato de los valores del eje y
    formatter = FuncFormatter(format_y_axis)
    plt.gca().yaxis.set_major_formatter(formatter)

    plt.xlabel(f"{title}")
    plt.xticks(fontsize=9)
    plt.ylabel("Consumo (MW)")
    plt.yticks(fontsize=9)
    plt.title(f"Demanda eléctrica peninsular por {title}")
    plt.show()
    
    #Insertar base de datos PostgreSQL

    '''
    ------------------------------------------------------
    Alternativa para almacenar únicamente los datos únicos
    ------------------------------------------------------

    Insertar los nuevos datos en la tabla temporal, incluyendo los duplicados:
    INSERT INTO tabla_temporal (dato) VALUES ('dato1'), ('dato2'), ('dato3')...;

    Eliminar los registros duplicados de la tabla temporal:
    DELETE FROM tabla_temporal WHERE (dato) IN (SELECT dato FROM tabla_original);

    Borrar la tabla original:
    DELETE FROM tabla_original;

    Copiar los datos de la tabla temporal a la tabla original:
    INSERT INTO tabla_original (dato) SELECT dato FROM tabla_temporal;

    Borrar la tabla temporal:
    DROP TABLE tabla_temporal;

    -----------------
    Otra alternativa:
    -----------------
    El procesamiento incremental implica identificar y procesar solo los datos nuevos que se han agregado
    desde el último procesamiento, en lugar de procesar todos los datos cada vez. Esto puede reducir el tiempo
    de procesamiento al evitar la repetición de operaciones en datos que ya han sido procesados anteriormente
    '''

    engine = create_engine(
        'postgresql://postgres:DrJMB39jcIkVMIYR7RNi@containers-us-west-53.railway.app:7081/railway')
    
    metadata = MetaData()
    table = Table('red_electrica', metadata, autoload_with=engine)

    with engine.connect() as conn:
        select_statement = table.select()
        result_set = conn.execute(select_statement)
        db_list = []
        for row in result_set:
            dicc = {'value':row[1], 'date':row[2], 'year':row[3], 'month':row[4], 'day':row[5], 'hour':row[6]}
            db_list.append(dicc)
    db = pd.DataFrame(db_list)
    
    values = df[~df.isin(db).all(axis=1)].values
    
    db2 = pd.DataFrame(values,columns=['value','date','year','month','day','hour'])
    num_rows_inserted = db2.to_sql('ree', if_exists="replace", con=engine)
    return f'{num_rows_inserted} rows inserted'

@app.route('/get_db_data')
def get_db_data():
    
    engine = create_engine('postgresql://postgres:8smvRuYuENexj3ThMtek@containers-us-west-168.railway.app:7117/railway')
    pd.read_sql_query(text("""SELECT * FROM ree"""), con=engine.connect())

@app.route('/wipe_data')
def wipe_data():

    secret = "borrar_bbdd"
    arg_secret = request.args['secret']

    if arg_secret == secret:
        engine = create_engine('postgresql://postgres:8smvRuYuENexj3ThMtek@containers-us-west-168.railway.app:7117/railway')
        pd.read_sql_query(text("""DELETE FROM ree WHERE 1=1"""), con=engine.connect())
    
if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))

