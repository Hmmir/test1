const SHEET_NAME = "🚚 Расчет поставки"
const ROW_START = 6
const NAME_COLUMN = "Допоставка"

/**
 * Формирует данные для запроса данных по API\
 * Делает запрос для получения датасета wb10xSalesReport_v1\
 * Вставляет данные на лист '✅ ОтчетПрод'\
 * Делает дополнительные действия над листом '🚚 Расчет поставки'
 */
function updateSalesReport() {
  const dataSheetName = "✅ ОтчетПрод"
  const sourceSheetName = "🚚 Расчет поставки"
  const showToast = (message) => common10x.showToast(message, dataSheetName, -1);
  showToast("Готовим данные для запроса");

  const SS = SpreadsheetApp.getActiveSpreadsheet();
  const SS_ID = SS.getId();
  const sourceSheet = SS.getSheetByName(sourceSheetName);

  const dateFrom = Utilities.formatDate(new Date(sourceSheet.getRange("A1").getValue()), Session.getScriptTimeZone(), "yyyy-MM-dd");
  const dateTo = Utilities.formatDate(new Date(sourceSheet.getRange("B1").getValue()), Session.getScriptTimeZone(), "yyyy-MM-dd");
  const nmIdsDictDB = new DB({ sheetName: "Справочный лист nmID", dataRowFirst: 3, headersRow: 1 });
  const nmIds = nmIdsDictDB.getDataItemsArray().filter(i => i.nm_id).map(i => i.nm_id);

  let getDataRequestPayload = {
    url: "/ss/datasets/data",
    payload: {
      "spreadsheet_id": SS_ID,
      "dataset": {
        "name": "wb10xSalesReport_v2",
        "values": {
          "date_from": dateFrom,
          "date_to": dateTo,
          "nm_ids": nmIds
        }
      }
    }
  };
  showToast("Запрашиваем данные для обновления");

  const data = common10x.btlzApi(getDataRequestPayload);
  if (!data) {
    showToast(`Не удалось получить данные`);
    console.log(data)
    return
  }
  if (!data?.length) {
    showToast(`Нет данные для обновления`);
    console.log(data)
    return
  }

  showToast(`Вставляем данные в таблицу`);
  console.log(common10x.batchUpdateDataSheet({
    sheetName: dataSheetName,
    data,
    dataRowFirst: 3,
    headersRow: 1,
    ss: SS
  }))

  clearAdditionalDeliveriesUtil(SpreadsheetApp.getActive())
  sourceSheet.getRange("E1").setValue(new Date());

  common10x.showToast(`Обновление успешно завершено`, dataSheetName, 5);
}


/**
 * Очищает столбцы с указанным названием начиная с заданной строки.
 */
function clearAdditionalDeliveries() {
  const dataSheetName = "🚚 Расчет поставки"
  var ui = SpreadsheetApp.getUi();
  var response = ui.alert(getMessegeAlertForClear(), ui.ButtonSet.YES_NO);

  if (response == ui.Button.YES) {
    try {
      common10x.showToast("Выполняем очистку допоставок", dataSheetName, -1);
      clearAdditionalDeliveriesUtil(SpreadsheetApp.getActive())
      common10x.showToast('Допоставки очищены', dataSheetName, 5);
    } catch (error) {
      console.log(error)
      common10x.showMessage("", ['Ошибка при очистке допоставки']);
    }
  } else {
    common10x.showToast("Выполнение скрипта прервано", dataSheetName, 5);
  }
}

/**
 * Очищает столбцы с указанным названием начиная с заданной строки.
 * 
 * @param {SpreadsheetApp.Spreadsheet} ss - Спредшит, в котором происходит очистка.
 * @param {string} sheetName - Название листа, в котором осуществляется поиск и очистка столбцов.
 * @param {string} columnName - Название столбца, который нужно очистить.
 * @param {number} rowStart - Строка, с которой начинается поиск заголовков и очистка данных.
 */
function clearAdditionalDeliveriesUtil(ss, columnName = NAME_COLUMN, sheetName = SHEET_NAME, rowStart = ROW_START) {

  let sheet = ss.getSheetByName(sheetName)
  var headers = sheet.getRange(rowStart, 1, 1, sheet.getLastColumn()).getValues()[0];
  var lastRow = sheet.getLastRow();

  var columnIndexes = [];
  headers.forEach(function (header, index) {
    if (header === columnName) {
      columnIndexes.push(index + 1);
    }
  });
  if (columnIndexes.length > 0) {
    var rangesToClear = columnIndexes.map(function (columnIndex) {
      return sheetName + "!" + getColumnLetter(columnIndex) + (rowStart + 1) + ":" + getColumnLetter(columnIndex) + lastRow;
    });

    Sheets.Spreadsheets.Values.batchClear(
      {
        ranges: rangesToClear
      },
      ss.getId()
    )

  } else {
    Logger.log("Колонки с названием '" + columnName + "' не найдены.");
  }
}



/**
 * Преобразует числовой индекс колонки в буквенный.
 * 
 * @param {number} columnIndex - Индекс колонки (начинается с 1).
 * @returns {string} - Буквенное представление колонки (например, 1 -> A, 27 -> AA).
 */
function getColumnLetter(columnIndex) {
  var letter = "";
  while (columnIndex > 0) {
    var temp = (columnIndex - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    columnIndex = (columnIndex - temp - 1) / 26;
  }
  return letter;
}

/**
 * Выполняет расчет поставок для всех групп товаров.
 * 
 * @param {SpreadsheetApp.Spreadsheet} ss - Спредшит, в котором выполняются расчеты.
 * @param {string} sheetName - Название листа, на котором происходит расчет.
 */
function processPostavka(ss, sheetName = SHEET_NAME) {

  let sheet = ss.getSheetByName(sheetName)

  // Получаем диапазоны данных
  var fullDataRange = sheet.getRange(1, 1, sheet.getLastRow(), sheet.getLastColumn()).getValues();
  
  // Разделяем данные на части
  let rowSecondAndThird = fullDataRange.slice(1, 3)
  var constantsRange = rowSecondAndThird.map(row => row.slice(3, 8)); // Диапазон D2:H3
  var additionalData = rowSecondAndThird.map(row => [row[10], row[8]]); // Столбцы K2 и I3
  var dataRange = fullDataRange.slice(ROW_START + 1); // Основные строки для обработки

  // Строки и заголовки
  var isCheckedRow = fullDataRange[0];  // Строка 1
  var headersRow = fullDataRange[3];      // Строка 4
  var groupRow = fullDataRange[4];        // Строка 5
  
  // Извлекаем столбцы данных
  var sku = dataRange.map(row => row[1]);                      // SKU из столбца C
  var barcodes = dataRange.map(row => row[2]);                      // Баркоды из столбца D
  var auto_calculation = dataRange.map(row => row[0]);         // Авторасчет из столбца A
  var redemption_percentage = dataRange.map(row => row[8]);    // Процент выкупа из столбца I
  var seasonality_coefficient = dataRange.map(row => row[17]); // Коэфф. сезонности из столбца R
  var monopallet_multiplicity = dataRange.map(row => row[15]); // Кратность Монопаллет из столбца P
  var boxes_multiplicity = dataRange.map(row => row[16]);      // Кратность коробов из столбца Q
  var remains_ff = dataRange.map(row => row[19]);              // Есть на ФФ
  // var remains_ff = dataRange.map(row => {                      // Остаток ФФ (после расчета) Считаем в скрипте, иначе значения некорректные
  //   let diff = row[19]-row[20]
  //   if (diff && diff > 0){
  //     return diff
  //   } else {
  //     return 0
  //   }
  // });

  // Константы для формул
  var reportDays = constantsRange[0][0];                // На сколько дней отчет
  var sortCalculationDays = constantsRange[1][0];       // На сколько считаем подсорт
  var considerDeliveryTime = constantsRange[0][2];      // Учитывать время на доставку?
  var productionAndDeliveryTime = constantsRange[1][2]; // Время на производство и доставку
  var sortThreshold = constantsRange[0][4];             // Считать к подсорту более, чем
  var roundingPrecision = constantsRange[1][4];         // Округлять до
  let needToDistribute = rowSecondAndThird[0][16];      // Распределить все остатки?

  // Дополнительные данные
  var considerPurchaseRate = additionalData[0][0]; // Учитывать процент выкупа?
  var deliveryType = additionalData[1][1];         // Тип поставки

  // Создаем объект для групп
  var groups = createGroups(headersRow, groupRow, isCheckedRow);
  //clearSupplyColumns(sheet, headersRow);
  

  // Выполняем расчеты для каждой группы
  for (var group in groups) {
    if (!groups.hasOwnProperty(group)) continue;

    var groupData = groups[group];
    if (groupData.sales_col && groupData.remains_col && groupData.additional_delivery_col) {

      groups[group]["sales"] = dataRange.map(row => row[groupData.sales_col - 1])
      groups[group]["remains"] = dataRange.map(row => row[groupData.remains_col - 1])
      groups[group]["delivery"] = dataRange.map(row => row[groupData.additional_delivery_col - 1])

      // processGroupCalculations(sheet, groupData, sku, auto_calculation, redemption_percentage, seasonality_coefficient, sales, remains, delivery, considerPurchaseRate, reportDays, sortCalculationDays, productionAndDeliveryTime, considerDeliveryTime, sortThreshold, roundingPrecision, deliveryType, monopallet_multiplicity, boxes_multiplicity);
    }
  }
  const data = {
    "act": "processGroupCalculations",
    "id": ss.getId(),
    "sheetName": SHEET_NAME,
    "groups": groups,
    "sku": sku,
    "barcodes": barcodes,
    "auto_calculation": auto_calculation,
    "redemption_percentage": redemption_percentage,
    "seasonality_coefficient": seasonality_coefficient,
    "remains_ff": remains_ff,
    "deliveryType": deliveryType,
    "monopallet_multiplicity": monopallet_multiplicity,
    "boxes_multiplicity": boxes_multiplicity,
    "reportDays": reportDays,
    "sortCalculationDays": sortCalculationDays,
    "productionAndDeliveryTime": productionAndDeliveryTime,
    "considerDeliveryTime": considerDeliveryTime,
    "sortThreshold": sortThreshold,
    "needToDistribute": needToDistribute,
    "roundingPrecision": roundingPrecision,
    "considerPurchaseRate": considerPurchaseRate,
    "rowStart": ROW_START
  }
  // sendToWeb(data); return;
  
  const response = sendToWeb(data)
  // console.log(response)
  if (response.success) {
    return response.result
  }
  else return response.error
}

/**
 * Создает объект групп с соответствующими колонками для каждой группы.
 * 
 * @param {Array} isCheckedRow - Строка выбран склад или нет (строка 1).
 * @param {Array} headersRow - Строка заголовков (строка 4).
 * @param {Array} groupRow - Строка групп (строка 5).
 * @returns {Object} - Объект групп с соответствующими колонками.
 */
function createGroups(headersRow, groupRow, isCheckedRow) {
  var groups = {};

  headersRow.forEach((headerValue, col) => {
    var groupName = groupRow[col];
    if (!groupName) return;

    if (!groups[groupName]) {
      groups[groupName] = {
        "sales_col": null,
        "remains_col": null,
        "additional_delivery_col": null,
        "supply_col": null,
        "is_stock_checked": isCheckedRow[col],
        "stock_accepts_chosen_type": false
      };
    }

    switch (headerValue) {
      case "sales":
        groups[groupName].sales_col = col + 1;
        break;
      case "remains":
        groups[groupName].remains_col = col + 1;
        break;
      case "additional_delivery":
        groups[groupName].additional_delivery_col = col + 1;
        break;
      case "supply":
        groups[groupName].supply_col = col + 1;
        break;
      case true:
        groups[groupName].stock_accepts_chosen_type = headerValue;
        break;
      case false:
        groups[groupName].stock_accepts_chosen_type = headerValue;
        break;
    }
  });

  return groups;
}

/**
 * Очищает все столбцы с заголовком "supply", начиная с 7-й строки.
 * 
 * @param {Sheet} sheet - Лист, в котором происходит очистка.
 * @param {Array} headersRow - Строка заголовков (строка 4).
 */
function clearSupplyColumns(sheet, headersRow) {
  let rages = []
  headersRow.forEach((header, col) => {
    if (header === "supply") {
      rages.push(`${sheet.getName()}!${getColumnLetter(col + 1)}${ROW_START + 1}:${getColumnLetter(col + 1)}`)
    }
  });
  sheet.getRangeList(rages).clearContent(); // Очищаем начиная с 7-й строки
}

function checkIfListIsInSheet(ss, sheetName = SHEET_NAME) {
  let sheets = ss.getSheets().map(x => x.getName())
  return sheets.includes(sheetName)
}
function getMessegeAlertForClear() {
  return "Вы уверены в том, что хотите очистить Допоставки?"
}

/**
 * Скрывает столбцы на указанном листе и выборочно отображает столбцы, соответствующие указанному названию склада.
 * @param {GoogleAppsScript.Spreadsheet.Sheet} - Лист для изменений.
 * @param {string}  - Название региона для поиска. Столбцы с этим значением будут показаны.
 * @param {number}  - Начальный столбец для проверки и скрытия/отображения.
 */
function hideStocks(sheet, region, startColumn = 25) {
  let lc = sheet.getLastColumn()
  // sheet.expandAllColumnGroups()
  sheet.hideColumns(startColumn, lc - startColumn + 1)

  let sheet_vals = sheet.getRange(1, startColumn, 4, lc - startColumn + 1).getValues()
  let storages = sheet_vals[0].flat()
  let values = sheet_vals[1].flat()
  let headers = sheet_vals[3].flat()
  
  // кол-во столбцов в одном блоке
  const columns_in_one_block = 7
  
  values.forEach((value, key) => {

    // идём по блокам
    // проверяем, нужно ли сравнивать регион
    let need_to_compare_region = false
    if (key % columns_in_one_block === 0) need_to_compare_region = true;

    // текущий регион
    let region_cur = values[key + 3]

    // прошёл ли склад фильтр по паллетам/коробам
    let filter_by_type_box_or_palettes_passed = headers[key + 6]

    // отмечен ли склад галочкой
    let is_storage_chosen = storages[key]

    let is_filter_by_region_passed = false
    if (region == region_cur || region == "Все" || region == "Все выбранные" || region == "Все невыбранные"){
      is_filter_by_region_passed = true;
    }

    let is_filter_by_chosen_passed = true
    if (region == "Все выбранные" && !is_storage_chosen){
      is_filter_by_chosen_passed = false;
    }

    if (region == "Все невыбранные" && is_storage_chosen){
      is_filter_by_chosen_passed = false;
    }

    if (
        region_cur
        && need_to_compare_region
        && filter_by_type_box_or_palettes_passed
        && is_filter_by_region_passed
        && is_filter_by_chosen_passed
      ) {
      sheet.showColumns(key + startColumn, columns_in_one_block)
    }
  })
}

/** 
 * Расчёт поставки
 */ 
function setSupply() {
  try {
    const dataSheetName = "🚚 Расчет поставки"
    common10x.showToast("Выполняем расчёт поставки", dataSheetName, -1);
    let ss = SpreadsheetApp.getActive()
    let result = processPostavka(ss)
    ss.getActiveSheet().getRange("A3").setValue(new Date())
    common10x.showToast(result, dataSheetName, 5);
  } catch (error) {
    console.log(error)
    common10x.showMessage("", ['Ошибка при расчёте продаж']);
  }
}





/** 
 * Функция для скрытия/отображения блоков складов по выбранному округу/региону
 */
function onEditSupply(e) {

  let sheet = e.range.getSheet(),
    sheetName = sheet.getName(),
    columnNumber = e.range.getColumn(),
    rowNumber = e.range.getRow()
  let objectForOnEdit = {
    shetName: SHEET_NAME,
    columnStart: 21,
    row: 2,
    rowType: 3,
    columnType: 9,
  };
  if (sheetName == objectForOnEdit.shetName && columnNumber == objectForOnEdit.columnStart && rowNumber == objectForOnEdit.row) {
    const dataSheetName = "🚚 Расчет поставки"
    common10x.showToast("Отображаем выбранные склады", dataSheetName, -1);
    let region = e.range.getValue()
    // Logger.log(region)
    hideStocks(sheet, region, objectForOnEdit.columnStart+4)
    common10x.showToast("Готово", dataSheetName, 5);
  } 
  
}
