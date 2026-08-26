# Tarea 3: Streaming de Datos Avanzado con Apache Beam

Este repositorio contiene la implementación de un pipeline de streaming de datos con Apache Beam, diseñado para calcular totales confirmados por comercio y por minuto, manejando eventos tardíos, duplicados, datos fuera de orden y escrituras idempotentes.

## 🚀 Instrucciones de Ejecución

Para ejecutar el entorno interactivo de Marimo y las pruebas, se puede utilizar Docker o uv.

### Opción 1: Usando Docker (Recomendado)
El proyecto incluye un docker-compose.yml preconfigurado.

1. Levantar el entorno interactivo de Marimo:
docker compose up --build
*(Accede a http://localhost:2718)*

2. Ejecutar la suite de pruebas automatizadas:
docker compose run --rm notebook uv run pytest

### Opción 2: Usando uv localmente
Si tienes Python y uv instalados en tu máquina:

1. Sincronizar las dependencias del proyecto:
uv sync

2. Ejecutar la suite de pruebas:
uv run pytest

3. Iniciar la interfaz web de Marimo:
uv run marimo edit notebook.py

---

## 🧠 Conceptos Teóricos Aplicados

### 1. Ventanas (Windowing)
Se utilizaron **ventanas fijas (Fixed Windows) de 60 segundos** basadas en el event_time (tiempo en el que ocurrió el evento real, no cuando llegó al sistema). Esto permite agrupar lógicamente los pagos que pertenecen al mismo minuto temporal, independientemente de si los eventos llegan desordenados al pipeline

### 2. Triggers (Disparadores)
Se implementó una política de disparadores combinada (AfterWatermark) para controlar cuándo se emiten los resultados de cada ventana:
* **Early firings:** Emiten resultados especulativos rápidamente a medida que llegan los datos (usando AfterProcessingTime).
* **Late firings:** Permiten actualizar el total emitido si llegan datos atrasados (AfterCount(1)), garantizando que las cifras se corrijan si ingresan pagos que aún están dentro de la tolerancia permitida (lateness de 120 segundos).

### 3. Estado por clave (Stateful Processing)
Para la deduplicación, Beam requiere mantener un registro de los eventos ya procesados. Se utilizó un SetStateSpec para almacenar los event_id asociados a cada merchant_id (la clave). Antes de sumar un pago, el sistema consulta este estado; si el ID ya existe, se ignora, garantizando que un pago duplicado no altere los cálculos monetarios.

### 4. Timer y Expiración de Estado
**¿Por qué un estado sin expiración crece indefinidamente?** 
En un sistema de streaming 24/7, si guardamos cada event_id que procesamos para siempre, la memoria RAM requerida crecerá de forma infinita (generando un Memory Leak) hasta colapsar el sistema. 
Para evitar esto, configuramos un **Timer de Event Time** (TimeDomain.WATERMARK). Este temporizador se programa para dispararse cuando el límite superior de la ventana actual más el margen de tolerancia (lateness) ha superado la marca de agua. Una vez que expira, la función decorada con @on_timer ejecuta un clear() sobre el estado, liberando la memoria con la seguridad matemática de que ya no se aceptarán eventos tan antiguos.

### 5. Idempotencia y Efectos Externos
En sistemas distribuidos, un sink (base de datos destino) puede experimentar fallos de red que obliguen a Beam a reintentar la escritura del mismo resultado varias veces. 
* Si hubiésemos utilizado un modelo Append-Only (POST tradicional), los reintentos duplicarían el ingreso total registrado. 
* Para solucionarlo, construimos una **clave de idempotencia lógica** (merchant_id|window_start) y simulamos operaciones UPSERT. De esta manera, sin importar cuántas veces el pipeline intente escribir el resultado final de una ventana para un comercio, la base de datos simplemente sobreescribirá el mismo registro asociado a esa clave, asegurando una consistencia exacta (Exactly-Once Semantics).

---
*Evidencia de pruebas y ejecución local adjunta en el repositorio.*