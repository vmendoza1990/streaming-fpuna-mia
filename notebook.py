import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable       
    from typing import Any    
    import marimo as mo    

    import apache_beam as beam
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    #from datetime import timezone
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError(f"El timestamp debe ser un string terminado en 'Z'. Recibido: {raw_value}")

        try:

            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
            return parsed
        except ValueError as e:
            raise ValueError(f"No se pudo parsear el timestamp ISO-8601: {raw_value}") from e

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _():
    from datetime import datetime, timezone, timedelta

    UTC = timezone.utc

    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retorna los límites [inicio, fin) de la ventana fija."""
        from datetime import timezone, timedelta

        epoch_seconds = timestamp.timestamp()
        start_seconds = (epoch_seconds // size_seconds) * size_seconds

        start_dt = datetime.fromtimestamp(
            start_seconds,
            tz=timezone.utc
        )
        end_dt = start_dt + timedelta(seconds=size_seconds)

        return start_dt, end_dt

    return assign_fixed_window, datetime


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

        totals_dict = {}
        audit = []
        seen = set()

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]
            status = event["status"]

            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])

            # Cálculos de tiempo
            delay_seconds = (arrival_time - event_time).total_seconds()
            w_start, w_end = assign_fixed_window(event_time, window_seconds)

            # Verificaciones
            is_duplicate = deduplicate and f"{merchant_id}-{event_id}" in seen
            if not is_duplicate and deduplicate:
                seen.add(f"{merchant_id}-{event_id}")

            is_too_late = delay_seconds > allowed_lateness_seconds
            is_accepted = (status == "CONFIRMED") and not is_duplicate and not is_too_late
            is_revision = is_accepted and arrival_time >= w_end

            # Razón
            reason = "accepted"
            if is_too_late:
                reason = "too_late"
            elif is_duplicate:
                reason = "duplicate"
            elif status != "CONFIRMED":
                reason = "not_confirmed"

            audit.append({
                "event_id": event_id,
                "merchant_id": merchant_id,
                "delay_seconds": delay_seconds,
                "duplicate": is_duplicate,
                "too_late": is_too_late,
                "accepted": is_accepted,
                "revision": is_revision,
                "reason": reason
            })

            # Sumar si es aceptado
            if is_accepted:
                key = (merchant_id, w_start.isoformat(), w_end.isoformat())
                totals_dict[key] = totals_dict.get(key, 0.0) + event.get("amount", 0.0)

        totals = [
            {"merchant_id": k[0], "window_start": k[1], "window_end": k[2], "total": v}
            for k, v in totals_dict.items()
        ]

        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, DeduplicatePayments, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        from apache_beam.transforms.window import TimestampedValue, FixedWindows
        from datetime import timezone

        class FormatOutput(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                merchant_id, total = element

                start_str = window.start.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat()
                end_str = window.end.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat()

                yield {
                    "merchant_id": merchant_id,
                    "window_start": start_str,
                    "window_end": end_str,
                    "total": total
                }

        return (
            pipeline
            | "Create events" >> beam.Create(events)
            | "Add timestamps" >> beam.Map(lambda e: TimestampedValue(e, parse_utc(e["event_time"]).timestamp()))
            | "Filter confirmed" >> beam.Filter(lambda e: e["status"] == "CONFIRMED")
            | "Window into fixed" >> beam.WindowInto(FixedWindows(window_seconds))
            | "Key by merchant" >> beam.Map(lambda e: (e["merchant_id"],e))
            | "Deduplicate payments" >> beam.ParDo(DeduplicatePayments())
            | "Extract amount">> beam.Map(lambda x: (x[0], x[1]["amount"]))
            | "Sum per merchant" >> beam.CombinePerKey(sum)
            | "Format results" >> beam.ParDo(FormatOutput())
        )

    return


@app.cell
def _(Any, SetStateSpec, StrUtf8Coder, TimeDomain, TimerSpec, beam, on_timer):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        # Los definimos a nivel de clase
        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            super().__init__()
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,

            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            merchant_id, event = element
            event_id = event["event_id"]

            seen_set = set(seen_ids.read())

            if event_id not in seen_set:
                seen_ids.add(event_id)

                expiry.set(
                    window.end.micros / 1_000_000
                    + self.allowed_lateness_seconds
                )

                yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        import apache_beam as beam
        from apache_beam.transforms.window import FixedWindows
        from apache_beam.transforms import trigger

        policy = beam.WindowInto(
            FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1)
            ),
            allowed_lateness=allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING
        )

        # (size y allowed_lateness) para que expongan el atributo '.seconds'
        class DurationProxy:
            def __init__(self, orig, secs):
                self._orig = orig
                self.seconds = secs
            def __getattr__(self, name):
                return getattr(self._orig, name)

        # proxy al tamaño de la ventana
        orig_size = policy.windowing.windowfn.size
        policy.windowing.windowfn.size = DurationProxy(orig_size, window_seconds)

        # proxy al lateness
        orig_lateness = policy.windowing.allowed_lateness
        policy.windowing.allowed_lateness = DurationProxy(orig_lateness, allowed_lateness_seconds)

        return policy

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

        materialized = []
        audit = []
        upsert_sink = {}

        op_type = "UPSERT" if idempotent else "POST"

        for result in results:
            res_copy = result.copy()
            if "idempotency_key" not in res_copy:
                res_copy["idempotency_key"] = make_idempotency_key(res_copy)

            for attempt in range(attempts):
                audit_row = res_copy.copy()
                audit_row["attempt"] = attempt + 1
                audit_row["operation"] = op_type
                audit.append(audit_row)

                if idempotent:
                    key = audit_row["idempotency_key"]
                    upsert_sink[key] = audit_row
                else:
                    materialized.append(audit_row)
    
        if idempotent:
            materialized = list(upsert_sink.values())

        return materialized, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisión;
    - [ ] un evento demasiado tardío queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    Bloque de validación de prueba de → data/payments.jsonl
    """)
    return


@app.cell
def _(summarize_payments):
    import json

    # archivo payments.jsonl
    events_from_file = []
    with open("data/payments.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            events_from_file.append(json.loads(line))

    # procesar evento utilizando la funcion
    totals_file, audit_file = summarize_payments(events_from_file)

    # resultados
    print("=== TOTALES FINALES ===")
    for total in totals_file:
        print(total)

    print("\n=== LOG DE CADA EVENTO ===")
    for item in audit_file:
        print(f"Evento: {item['event_id']} | Comercio: {item['merchant_id']} | Razón: {item['reason']} | Aceptado: {item['accepted']}")
    return


if __name__ == "__main__":
    app.run()
