# AUTONOMOUS 1:1 STATUS REPORT (2026-02-14)

Проект: `C:\Users\alien\Desktop\10x`
Режим: `BTLZ_UPSTREAM_ENABLED=0` (без чужого backend)

## 1) Что реализовано в коде

### 1.1 Прокси отключен по умолчанию
- `backend_compat/upstream_proxy.py:19`
- `backend_compat/README.md:49`

### 1.2 `wb10xSalesReport_v1/v2` в формате `✅ ОтчетПрод` (13 полей)
- Сборка из WB orders/sales/stocks/cards:
  - `backend_compat/datasets.py:1824` (`_build_sales_report_rows`)
  - роутинг датасета: `backend_compat/datasets.py:2128`
- Поля схемы:
  - `backend_compat/datasets.py:207` (`SALES_REPORT_FIELDS`)

### 1.3 Checklist: полная схема 151 поле
- Базовая схема 151 полей:
  - `backend_compat/datasets.py:223` (`CHECKLIST_FIELDS_DEFAULT`)
- Загрузка схемы из `sheet_headers.json`:
  - `backend_compat/datasets.py:379`
- Обработчик:
  - `backend_compat/datasets.py:2218` (`handle_checklist`)

### 1.4 Analytics daily семантика (sparse по умолчанию)
- Отключено принудительное заполнение всех дат:
  - `backend_compat/datasets.py:73`, `backend_compat/datasets.py:798`
- Добавлен фильтр низкого трафика:
  - `backend_compat/datasets.py:76`, `backend_compat/datasets.py:835`

### 1.5 Actions и update-совместимость
- `/ss/datasets/update` принимает payload только с `{ssId}`:
  - `backend_compat/server.py:281`
- Реализация `processGroupCalculations`:
  - `backend_compat/server.py:324`, `backend_compat/server.py:400`
- Реализация `search/positions`:
  - `backend_compat/server.py:403`
  - `backend_compat/wb_client.py:767` (`fetch_search_positions`)

### 1.6 Улучшение `wbCardsData_v1`
- `basket` в URL-формате:
  - `backend_compat/wb_client.py:165`
- Доп. получение цены карточки из публичного WB cards API (v4):
  - `backend_compat/wb_client.py:26`, `backend_compat/wb_client.py:37`
  - `backend_compat/wb_client.py:228`, `backend_compat/wb_client.py:256`, `backend_compat/wb_client.py:393`
- Фильтр по `nm_ids` для `wbCardsData_v1`:
  - `backend_compat/datasets.py:2073`

## 2) Проверки (локально, на реальном WB токене)

### 2.1 Компиляция
- `python -m py_compile backend_compat/wb_client.py backend_compat/datasets.py backend_compat/server.py backend_compat/upstream_proxy.py`
- Результат: OK.

### 2.2 Checklist
- `GET /api/ss/{sheet}/dataset/wb/checklist?nm_ids=187611270&date_from=2026-02-01`
- Результат: `200`, `13` строк, `151` ключ в строке.

### 2.3 Sales report
- `wb10xSalesReport_v2` для `nm_id=187611270`, `2026-02-06..2026-02-13`
- Результат: `48` строк, `13` ключей (как у `✅ ОтчетПрод`).

### 2.4 Cards data
- `wbCardsData_v1` (полный список): `113` карточек, `14` с ненулевым `discounted_price`.
- Пример:
  - `nm_id=187611270` -> `discounted_price=758.0`, basket URL корректный.

### 2.5 Actions
- `POST /api/ss/datasets/update` с `{ssId}` -> `200`, `success=true`, `dataset=null`, `message=accepted`.
- `POST /api/actions` (`search/positions`) -> `200`, возвращает массив строк.
- `POST /api/actions` (`processGroupCalculations`) -> `200`, возвращает рассчитанные `supply`.

### 2.6 Parity (orig vs autonomous)
- Отчет: `backend_compat/data/parity_report_autonomous_20260213_after_patch.json`
- Датасеты: `wb10xMain_planMonth_v1`, `wb10xSalesFinReportTotal_v1`
- Итог:
  - сохраняется сильный дрейф по `adv_sum*`, `total_to_pay`, `direct_costs_no_tax`, `marg_val_no_tax` и др.

## 3) Что все еще не хватает для строгого "1:1 как у конкурента"

### 3.1 Историческая рекламная семантика (jam_clusters / normquery)
- `wb10xAdvNormqueryStatsByDatesNmIdsAdvertIds_v1`: часто `0` строк или `stats=[]`.
- `wbJamClusters_v1`: `0` строк на проверенных диапазонах.
- Причина: WB endpoint normquery не отдает эквивалент исторического слоя, который у конкурента, вероятно, хранится отдельно.

### 3.2 Экономическая модель конкурента
- По parity видно, что формулы конкурента включают дополнительные правила/калибровки распределения рекламных и прямых расходов.
- Без знания приватной бизнес-логики конкурента и их исторических данных получить математически точный `1:1` невозможно.

### 3.3 Поисковые позиции
- Реализован сбор через публичный WB search, но есть ограничения антибота (`429`) и нестабильность ранжирования.
- Для гарантированного `1:1` нужен собственный стабильный индексатор/историческое хранилище позиций.

## 4) Честный вывод

Система теперь работает автономно и покрывает ключевые API-контракты (checklist 151, sales report 13, actions, cards basket/price enrichment), но **строгое** `1:1` с конкурентом по всем метрикам пока недостижимо без:
1) собственного исторического слоя рекламы/поиска,
2) точного воспроизведения их приватной финансовой модели.
