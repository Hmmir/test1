function updateClusters(SS = SpreadsheetApp.getActiveSpreadsheet(), SS_ID = SS.getId(), host = undefined) {
  requireScopes();
  
  const activeSheet = SS.getActiveSheet();
  const sheetName = activeSheet.getName();
  const _showToast = (message, delayTime) => showToast(message, sheetName, delayTime);
  
  try {
    _showToast("Запрос данных по кластерам за последние 30 дней...", -1);
    
    // Датасет автоматически выгружает данные за последние 30 дней
    // Не требуется указывать период или фильтры
    const payload = {
      url: "/ss/datasets/data",
      payload: {
        "spreadsheet_id": SS_ID,
        "dataset": {
          "name": "wb10xAdvNormqueryStatsByDatesNmIdsAdvertIds_v1",
          "values": {}
        }
      }
    };
    
    console.log(JSON.stringify(payload.payload, null, 2));
    
    const data = btlzApi(payload, host);
    
    if (!data) {
      _showToast("Не удалось получить данные", 5);
      console.log(data);
      return;
    }
    
    if (!data?.length) {
      _showToast("Нет кластеров с показами > 100 за выбранный период", 5);
      console.log(data);
      return;
    }
    
    _showToast(`Получено ${data.length} строк данных. Запись в таблицу...`);
    
    // Технический лист для записи данных
    const techSheetName = "clusters_tehn";
    let techSheet = SS.getSheetByName(techSheetName);
    
    if (!techSheet) {
      _showToast(`Лист "${techSheetName}" не найден. Создайте лист с заголовками.`, 5);
      return;
    }
    
    // Получаем заголовки из первой строки
    const sheets = Sheets.Spreadsheets.Values;
    const techSheetRange = `'${techSheetName}'!A1:ZZ1`;
    const headersData = sheets.get(SS_ID, techSheetRange);
    
    if (!headersData || !headersData.values || headersData.values.length === 0) {
      _showToast(`Заголовки не найдены на листе "${techSheetName}"`, 5);
      return;
    }
    
    const headers = headersData.values[0];
    
    // Преобразуем данные в двумерный массив
    const rows = data.map(row => {
      return headers.map(header => {
        const value = row[header];
        return value !== undefined && value !== null ? value : "";
      });
    });
    
    // Проверяем наличие данных
    if (rows.length === 0) {
      _showToast("Нет данных для записи. Старые данные сохранены.", 5);
      return;
    }
    
    // Сначала очищаем весь лист (кроме первых двух строк), затем записываем данные
    // Используем только Advanced Sheets Service API для избежания конфликтов
    const maxRows = techSheet.getMaxRows();
    
    // Очищаем старые данные с 3-й строки через Advanced Sheets Service
    if (maxRows > 2) {
      const clearRange = `'${techSheetName}'!A3:${letter(headers.length)}${maxRows}`;
      Sheets.Spreadsheets.Values.clear({}, SS_ID, clearRange);
    }
    
    // Записываем новые данные с 3-й строки
    const targetRange = `'${techSheetName}'!A3:${letter(headers.length)}${rows.length + 2}`;
    
    Sheets.Spreadsheets.Values.update(
      {
        values: rows
      },
      SS_ID,
      targetRange,
      {
        valueInputOption: "USER_ENTERED"
      }
    );
    
    _showToast(`Обновление успешно завершено. Загружено ${rows.length} строк.`, 5);
    
  } catch (e) {
    _showToast("Ошибка: что-то пошло не так", 5);
    console.log(e);
  }
}


function letter(colNum) {
  if (colNum < 1) throw new Error(`Wrong column number (${colNum})`)
  let remain,
    string = "";
  if (colNum < 27) return String.fromCharCode(64 + colNum);
  do {
    remain = colNum % 26;
    if (remain == 0) {
      remain = 26;
      colNum -= remain
    }
    colNum = Math.floor(colNum / 26);
    string = String.fromCharCode(64 + remain) + string;
  } while (colNum > 0);
  return string;
}
