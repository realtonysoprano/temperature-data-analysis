import pandas as pd
import streamlit as st
from client import async_weather_data
from processing import (process_filtered_data, calculate_monthly_stats,
                             calculate_seasonal_stats, is_temperature_normal)
from datetime import datetime
import asyncio
import matplotlib.pyplot as plt

CITIES = ['New York', 'London', 'Paris', 'Tokyo', 'Moscow', 'Sydney',
          'Berlin', 'Beijing', 'Rio de Janeiro', 'Dubai', 'Los Angeles',
          'Singapore', 'Mumbai', 'Cairo', 'Mexico City']


async def main():
    st.title("Анализ температурных данных и мониторинг текущей температуры")
    st.header("Шаг 1: Загрузите csv-файл с данными")

    uploaded_file = st.file_uploader("Выберите файл в формате csv", type=["csv"])

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        df = process_filtered_data(df)
        st.write("Загрузка завершена")
        st.header("Шаг 2: Выберите город")
        selected_city = st.selectbox("Выберите город:", CITIES)
        api_key = st.text_input("Введите API ключ:", type="password")
        if api_key:
            st.header(f"Шаг 3: Статистика температуры в городе {selected_city}")
            with st.form(key='weather_form'):
                submitted = st.form_submit_button("Получить данные о погоде")

                if submitted:
                    stat_weather_data = await async_weather_data([selected_city], api_key)
                    if isinstance(stat_weather_data, str):
                        st.error(stat_weather_data)
                    else:
                        mean_temperature_season, std_temperature_season = calculate_seasonal_stats(df, selected_city)
                        mean_temperature_month, std_temperature_month = calculate_monthly_stats(df, selected_city)
                        is_normal = is_temperature_normal(mean_temperature_month, std_temperature_month,
                                                           stat_weather_data[selected_city])

                        if stat_weather_data:
                            st.write(f"### На текущий момент погода в городе **{selected_city}**:")
                            st.write(f"- Температура: **{stat_weather_data[selected_city]:.1f}°C**")

                            st.write("### Средние температуры для данного города в месяц и в сезон:")
                            st.write(f"- Средняя температура в городе за месяц: **{mean_temperature_month:.1f}°C**")
                            st.write(f"- Средняя температура в городе за сезон: **{mean_temperature_season:.1f}°C**")

                            if is_normal:
                                st.write("### Температура является нормальной для данного месяца.")
                            else:
                                st.write("### Температруа не является нормальной для данного месяца.")
                            plot_with_outliers(df, selected_city)
                            st.pyplot(plt)
        else:
            st.write("Ввведите ваш API ключ")

    else:
        st.write("Загрузите CSV-файл")


def plot_with_outliers(df, city):
    filtered_df = df.query(f'city == "{city}"')

    plt.figure(figsize=(10, 5))
    plt.plot(filtered_df['timestamp'], filtered_df['temperature'], label='Температура', color='red')

    outliers = filtered_df[filtered_df['outlier_flag']]
    plt.scatter(outliers['timestamp'], outliers['temperature'], color='blue', label='Аномалии')

    plt.title(f'Температура в городе {city}')
    plt.xlabel('Дата')
    plt.ylabel('Температура')
    plt.legend()
    plt.grid()


if __name__ == "__main__":
    asyncio.run(main())
