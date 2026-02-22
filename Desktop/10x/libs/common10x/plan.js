/**
 * Формирует данные для запроса данных по API
 * Делает запрос для получения датасета wb10xMain_planMonth_v1
 * Вставляет данные на текущий лист с планом
 * После обновления сохраняет данные плана на сервер
 */
function updatePlanMonth(SS, SS_ID) {
  requireScopes();
  // Активный лист с планом
  const sheet_plan = SS.getActiveSheet();
  const dataSheetName = sheet_plan.getName();
  const _showToast = (message, delayTime) => showToast(message, dataSheetName, delayTime);

  try {
    _showToast("Подготовка параметров для запроса", -1);

    // Строка, с которой начинаются данные на листе
    const PLAN_SHEET_ROW_START = 7

    const datasetUpdateFields = [
      "avg_price"
      , "days_in_stock"
      , "checklist_orders_sum"
      , "checklist_orders_count"
      , "checklist_buyouts_sum"
      , "checklist_buyouts_count"
      , "orders_ext_perc"
      , "adv_sum_auto_search"
      , "stocks_fbo"
      , "stocks_fbs"
      , "buyout_percent"

      , "sebes_rub"
      , "markirovka_rub"
      , "perc_mp"
      , "delivery_mp_with_buyout_rub"
      , "hranenie_rub"
      , "acquiring_perc"
      , "tax_total_perc"
      , "additional_costs"
      , "priemka_rub"
      , "spp"
    ]

    // Получение dateFrom и dateTo
    const MILLIS_PER_DAY = 1000 * 60 * 60 * 24;
    const dateInput = new Date(sheet_plan.getRange("A2").getValue());
    let dateInputMinusDay = new Date(dateInput.getTime() - 1 * MILLIS_PER_DAY);
    dateInputMinusDay = Utilities.formatDate(dateInputMinusDay, Session.getScriptTimeZone(), "yyyy-MM-dd");

    let dateInputMinus30Days = new Date(dateInput.getTime() - 30 * MILLIS_PER_DAY);
    dateInputMinus30Days = Utilities.formatDate(dateInputMinus30Days, Session.getScriptTimeZone(), "yyyy-MM-dd");

    const today = new Date();
    const todayDt = Utilities.formatDate(today, Session.getScriptTimeZone(), "yyyy-MM-dd");
    let todayMinusDay = new Date(today.getTime() - 1 * MILLIS_PER_DAY);
    todayMinusDay = Utilities.formatDate(todayMinusDay, Session.getScriptTimeZone(), "yyyy-MM-dd");

    let todayMinus30Days = new Date(today.getTime() - 30 * MILLIS_PER_DAY);
    todayMinus30Days = Utilities.formatDate(todayMinus30Days, Session.getScriptTimeZone(), "yyyy-MM-dd");

    let dateFrom = dateInputMinus30Days;
    let dateTo = dateInputMinusDay;
    if (dateInputMinusDay >= todayDt) {
      dateFrom = todayMinus30Days
      dateTo = todayMinusDay
    }

    // Получение списка nm_id
    const planDataRange = `'${dataSheetName}'!A1:ZZ`;
    const sheets = Sheets.Spreadsheets.Values;
    const planData = sheets.get(SS_ID, planDataRange).values;
    const headerIndexes = planData[0];
    const nmIdsIndex = headerIndexes.indexOf("nm_id");

    if (nmIdsIndex === -1) {
      _showToast(`Технические столбцы не найдены`, 5);
      return
    }

    const planNmIdsColumn = planData.map(row => Number(row[nmIdsIndex])); // Перевод в одномерный массив
    const planNmIdsArr = planNmIdsColumn.slice(PLAN_SHEET_ROW_START - 1); // Берём только значащие строки
    const planNmIdsArrFiltered = planNmIdsArr.filter(el => el); // Фильтрация пустых значений
    const nmIds = [... new Set(planNmIdsArrFiltered)]; // Оставляем только уникальные значения массива

    if (!nmIds.length) {
      _showToast(`Нет данных для обновления`);
      console.log(nmIds);
      return
    }

    let getDataRequestPayload = {
      url: "/ss/datasets/data",
      payload: {
        "spreadsheet_id": SS_ID,
        "dataset": {
          "name": "wb10xMain_planMonth_v1",
          "values": {
            "date_from": dateFrom,
            "date_to": dateTo,
            "nm_ids": nmIds
          }
        }
      }
    };
    _showToast("Запрос данных для обновления");

    const data = btlzApi(getDataRequestPayload);
    if (!data) {
      _showToast(`Не удалось получить данные`);
      console.log(data)
      return
    }
    if (!data?.length) {
      _showToast(`Нет данных для обновления`);
      console.log(data)
      return
    }

    const nmIdsDataToInsert = planNmIdsArr.map(nm_id => data.find(row => row.nm_id === nm_id));

    let updates = [];
    for (let field of datasetUpdateFields) {

      const columnIndex = headerIndexes.indexOf(field);
      if (columnIndex === -1) {
        _showToast(`Технические столбцы не найдены`, 5);
        return
      }

      const columnLetter = letter(columnIndex + 1);
      const col_range = `'${dataSheetName}'!${columnLetter}${PLAN_SHEET_ROW_START}:${columnLetter}`;
      const col_values = nmIdsDataToInsert.map(row => {

        if (row && row.nm_id) {
          if (row[field]) {
            return [row[field]]
          } else {
            return [0]
          }
        } else {
          return [""]
        }

      })

      updates.push({
        "range": col_range,
        "values": col_values
      })
    }

    Sheets.Spreadsheets.Values.batchUpdate(
      {
        data: updates.map((update) => ({
          range: update.range,
          values: update.values,
        })),
        valueInputOption: "USER_ENTERED",
      },
      SS_ID
    );

    _showToast(`Обновление успешно завершено`, 5);

    // =============================================
    // Сохранение данных плана на сервер
    // =============================================
    _showToast("Подготовка данных для сохранения", -1);

    // Маппинг столбцов (буква -> название поля)
    const columnMapping = {
      "I": "days_in_stock",
      "J": "checklist_orders_sum",
      "K": "checklist_orders_count",
      "L": "checklist_buyouts_sum",
      "M": "checklist_buyouts_count",
      "N": "orders_ext_perc",
      "P": "adv_sum_auto_search",
      "Y": "orders_sum",
      "Z": "orders_count",
      "AA": "buyouts_sum",
      "AB": "buyouts_count",
      "AC": "adv_percent",
      "AE": "adv_sum",
      "AF": "stocks_fbo",
      "AG": "stocks_fbs",
      "AJ": "avg_price",
      "AK": "buyout_percent",
      "AM": "sebes_rub",
      "AN": "markirovka_rub",
      "AO": "perc_mp",
      "AP": "delivery_mp_with_buyout_rub",
      "AQ": "hranenie_rub",
      "AR": "acquiring_perc",
      "AS": "tax_total_perc",
      "AT": "priemka_rub",
      "AU": "additional_costs",
      "AV": "spp",
      "AW": "marg_with_drr",
      "BJ": "profit_with_drr"
    };

    // Получение даты для сохранения
    const dateStr = Utilities.formatDate(dateInput, Session.getScriptTimeZone(), "yyyy-MM-dd");

    // Получение данных с листа (UNFORMATTED_VALUE для получения реальных значений)
    const planDataForSave = sheets.get(SS_ID, planDataRange, { valueRenderOption: "UNFORMATTED_VALUE" }).values;
    const headerRow = planDataForSave[0];

    // Функция для получения индекса столбца по букве
    function columnLetterToIndex(letter) {
      let index = 0;
      for (let i = 0; i < letter.length; i++) {
        index = index * 26 + (letter.charCodeAt(i) - 64);
      }
      return index - 1;
    }

    // Функция для парсинга числового значения
    function parseNumericValue(value) {
      if (value === undefined || value === "" || value === null) return null;
      if (typeof value === "number") return value;
      const numValue = Number(value);
      return isNaN(numValue) ? null : numValue;
    }

    // Находим индекс nm_id из заголовка
    const nmIdIndex = headerRow.indexOf("nm_id");
    if (nmIdIndex === -1) {
      _showToast("Столбец nm_id не найден в заголовке", 5);
      return;
    }

    // Индекс столбца orders_sum для проверки
    const ordersSumIndex = columnLetterToIndex("Y");

    // Собираем данные для отправки
    const dataToSave = [];
    const dataRows = planDataForSave.slice(PLAN_SHEET_ROW_START - 1);

    for (const row of dataRows) {
      const nmId = Number(row[nmIdIndex]);
      if (!nmId) continue;

      // Проверяем orders_sum - пропускаем если 0 или отсутствует
      const ordersSumValue = parseNumericValue(row[ordersSumIndex]);
      if (!ordersSumValue || ordersSumValue === 0) continue;

      const rowData = {
        nm_id: nmId,
        date: dateStr
      };

      for (const [colLetter, fieldName] of Object.entries(columnMapping)) {
        const colIndex = columnLetterToIndex(colLetter);
        const numValue = parseNumericValue(row[colIndex]);
        if (numValue !== null) {
          rowData[fieldName] = numValue;
        }
      }

      dataToSave.push(rowData);
    }

    if (!dataToSave.length) {
      _showToast("Нет данных для сохранения", 5);
      return;
    }

    _showToast(`Сохранение ${dataToSave.length} записей...`, -1);

    // Отправка на сервер
    const saveRequestPayload = {
      url: "/ss/datasets/upload",
      payload: {
        "spreadsheet_id": SS_ID,
        "dataset": {
          "name": "wb10xPlanMonthSave",
          "values": {
            "data": dataToSave
          }
        }
      }
    };

    const result = btlzApi(saveRequestPayload);

    if (result !== null && result !== undefined) {
      _showToast(`Успешно сохранено ${result} записей`, 5);
    } else {
      _showToast("Ошибка при сохранении данных", 5);
      console.log("Save result:", result);
    }

  } catch (e) {
    _showToast("Ошибка: что-то пошло не так", -1);
    console.log(e);
  }
}
