# Codex Usage Dashboard

Dashboard local en Python para observar el uso reciente de Codex Desktop: tokens, sesiones, modelos, cache, limites y threads recientes, todo leido en modo solo lectura desde tu instalacion local.

<p align="center">
  <a href="https://github.com/angelporlan/codex-dashboard/raw/main/assets/codex-dashboard-promo.mp4">
    <img src="assets/codex-dashboard-promo.gif" alt="Codex Usage Dashboard promo video" width="900">
  </a>
</p>

<p align="center">
  <a href="https://github.com/angelporlan/codex-dashboard/raw/main/assets/codex-dashboard-promo.mp4">Abrir MP4</a>
</p>

## Que Muestra

| Area | Descripcion |
| --- | --- |
| Tokens en ventana | Suma de input y output detectados en eventos `response.completed`. |
| Llamadas medidas | Numero de respuestas con metricas encontradas en logs locales. |
| Limites de Codex | Porcentaje consumido o restante de ventanas primaria y secundaria cuando existe evento local. |
| Modelos | Uso agregado por modelo: threads, llamadas, tokens, input y output. |
| Threads recientes | Conversaciones locales con filtros por proyecto, modelo, estado, texto y tokens. |
| Presupuesto local | Topes opcionales diarios y mensuales para comparar consumo. |

## Arrancar

```powershell
python app.py
```

Abre:

```text
http://127.0.0.1:8765
```

## Fuentes Locales

La app no modifica bases de datos de Codex; las abre en modo lectura.

- `%USERPROFILE%\.codex\state_5.sqlite`: threads, titulos, proyectos y `tokens_used` acumulado.
- `%USERPROFILE%\.codex\logs_2.sqlite`: eventos `response.completed` con input, output, cache, reasoning y tool tokens.
- Eventos `codex.rate_limits`: porcentajes y resets de ventanas primaria y secundaria cuando estan disponibles.

## Configuracion

`dashboard_config.json` guarda presupuestos locales opcionales:

```json
{
  "token_budget": {
    "daily": 0,
    "monthly": 0
  }
}
```

Estos valores son referencias locales para comparar consumo; no representan necesariamente limites reales de Codex.
