# Codex Usage Dashboard

Dashboard local en Python para ver uso de tokens y limites recientes de Codex Desktop.

## Arrancar

```powershell
python app.py
```

Abre:

```text
http://127.0.0.1:8765
```

## Que lee

- `%USERPROFILE%\.codex\state_5.sqlite`: threads, titulos, proyectos y `tokens_used` acumulado.
- `%USERPROFILE%\.codex\logs_2.sqlite`: eventos `response.completed` con input, output, cache y reasoning tokens.
- Eventos `codex.rate_limits` para mostrar el porcentaje usado de las ventanas primaria y secundaria.

## Configuracion

`dashboard_config.json` guarda presupuestos locales opcionales:

- `daily`: limite diario de referencia.
- `monthly`: limite de referencia para la ventana seleccionada.

No modifica las bases de datos de Codex; las abre en modo lectura.
