const PLAN_ACTION_CONFIG = {
  SHEET_NAME: 'План Действий',
  
  // Структура блоков
  FIRST_NM_ID_ROW: 3, // Первая строка с nm_id (D3, D34, D65...)
  BLOCK_STEP: 31, // Шаг между блоками nm_id (34-3=31, 65-34=31)
  DANGERS_ROWS: 30, // Количество строк для задач (3-32, 34-63)
                    // ЛИМИТ: Если задач > 30, показываются только первые 30!
  
  // Колонки месяца и nm_id
  MONTH_CELL: 'D1',         // Выбранный месяц (формат "Сент - Окт")
  LAST_MONTH_CELL: 'A1',    // Последний обновленный месяц
  NM_ID_COL: 'D',           // Текущий nm_id (может быть изменен пользователем)
  ORIGINAL_NM_ID_COL: 'B',  // Оригинальный nm_id после последней загрузки из БД
  TASK_IDS_COL: 'A',        // Колонка для task_id (A3:A32)
  
  // Колонки блока "Задачи"
  TASKS_START_ROW_OFFSET: 0, // Смещение от nm_id до начала данных задач (0 = та же строка)
  TASKS_COLS: {
    NUM: 'J',           // Нумерация (1-30)
    STAFF: 'N',         // Ответственный
    DATE_TO: 'O',       // Дата конца
    INDICATOR: 'K',     // Показатель
    PROBLEM: 'L',       // Проблема
    HYPOTHESIS: 'M',    // Задача
    STATUS: 'P'         // Готово (чекбокс)
  },
  
  // Колонки блока "Логи действий" (календарь)
  LOGS_START_COL: 'R',  // Начало блока логов (R3:CA32)
  LOGS_DAYS: 62,        // Количество дней (2 месяца, максимум 31+31)
  
  // API
  SS_ID: null
};

/**
 * Главная функция обновления - вызывается кнопкой "Обновить"
 * 
 * ЛОГИКА ОБРАБОТКИ ИЗМЕНЕНИЯ nm_id:
 * 1. Если nm_id в D изменился (D != B):
 *    - Очищаются task_id в колонке A (предотвращает переприсвоение старых задач)
 *    - Загружаются задачи для нового nm_id
 *    - Старые задачи остаются в БД со старым nm_id
 * 2. Если nm_id удален из D (D пусто, B заполнена):
 *    - Блок очищается визуально
 *    - Задачи остаются в БД (не удаляются)
 */
function updateActionPlan() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  PLAN_ACTION_CONFIG.SS_ID = ss.getId();
  
  const planSheet = ss.getSheetByName(PLAN_ACTION_CONFIG.SHEET_NAME);
  
  if (!planSheet) {
    ss.toast(`Не найден лист "${PLAN_ACTION_CONFIG.SHEET_NAME}"`, '❌ Ошибка', 5);
    return;
  }
  
  try {
    const startTime = new Date().getTime();

    const logsEndCol = columnLetterToNumber(PLAN_ACTION_CONFIG.LOGS_START_COL) + PLAN_ACTION_CONFIG.LOGS_DAYS - 1;
    const logsEndColLetter = letter(logsEndCol);

    const ranges = [
      `'${PLAN_ACTION_CONFIG.SHEET_NAME}'!A:${logsEndColLetter}`
    ];

    const batchGetResponse = Sheets.Spreadsheets.Values.batchGet(PLAN_ACTION_CONFIG.SS_ID, { ranges: ranges });
    const valueRanges = batchGetResponse.valueRanges;

    const allSheetData = valueRanges[0].values || [];

    const workingData = allSheetData.map(row => [...row]);
    let hasChanges = false;

    const colIdx = {
      lastMonth: 0,        // A1 (row 0)
      originalNmId: 1,     // B
      currentMonth: 3,     // D1 (row 0)
      nmId: 3,             // D
      taskId: 0,           // A
      taskNum: 9,          // J
      taskIndicator: 10,   // K
      taskProblem: 11,     // L
      taskHypothesis: 12,  // M
      taskStaff: 13,       // N
      taskDateTo: 14,      // O
      taskStatus: 15,      // P
      logsStart: 17        // R (колонка 18, 0-based = 17)
    };

    // 1. Проверяем изменился ли месяц (из уже прочитанных данных)
    const lastMonth = allSheetData[0][colIdx.lastMonth];
    const currentMonth = allSheetData[0][colIdx.currentMonth];
    const monthChanged = lastMonth !== currentMonth && lastMonth !== '';

    // 2. Находим все блоки (из уже прочитанных данных)
    const blocksResult = findAllBlocksFromArray(allSheetData, colIdx);
    const blocks = blocksResult.blocks;
    const duplicatesCount = blocksResult.duplicatesCount;

    // 2.1. Обрабатываем блоки где nm_id изменился (D != B)
    const changedBlocks = findBlocksWithChangedNmIdFromArray(allSheetData, colIdx, blocks);
    let savedChangedCount = 0;
    if (changedBlocks.length > 0) {
      // СНАЧАЛА сохраняем данные для НОВОГО nm_id (из allSheetData, там ещё не очищено!)
      // Парсим выбранный месяц для сохранения
      const currentMonthValue = allSheetData[0][colIdx.currentMonth];
      const selectedMonthsForSave = parseMonthString(currentMonthValue);
      
      // Преобразуем changedBlocks в формат для saveTasksToServerFromArray
      const changedBlocksForSave = changedBlocks.map(cb => ({
        startRow: cb.startRow,
        nmId: cb.newNmId  // Используем НОВЫЙ nm_id из D
      }));
      
      // Сохраняем данные с новым nm_id
      savedChangedCount = saveTasksToServerFromArray(allSheetData, colIdx, changedBlocksForSave, selectedMonthsForSave, null);
      Logger.log(`Сохранено задач для блоков с изменённым nm_id: ${savedChangedCount}`);
      
      // ПОТОМ очищаем workingData (чтобы загрузить свежие данные с сервера)
      clearChangedBlocksInArray(workingData, colIdx, changedBlocks);
      hasChanges = true;
    }

    // 3. Если месяц изменился - сохраняем данные для СТАРОГО месяца
    let deletedOldCount = 0;
    if (monthChanged) {
      Logger.log(`⚠️ Месяц изменился: "${lastMonth}" → "${currentMonth}"`);
      Logger.log('Сохраняем данные для старого месяца перед переключением');
      
      // Парсим СТАРЫЙ месяц из A1
      const oldMonths = parseMonthString(lastMonth);
      Logger.log(`Старые месяцы: ${oldMonths.join(', ')}`);
      
      // Загружаем данные с сервера для старого месяца (чтобы сохранить date_to)
      const serverDataOld = loadTasksAndLogsFromServer(blocks, oldMonths);
      
      // Сохраняем задачи для старого месяца (из массива!)
      const savedOldCount = saveTasksToServerFromArray(allSheetData, colIdx, blocks, oldMonths, serverDataOld);
      Logger.log(`Сохранено задач для старого периода: ${savedOldCount}`);
      
      // Удаляем удалённые логи для СТАРОГО периода (до очистки!)
      deletedOldCount = detectAndDeleteRemovedItemsFromArray(allSheetData, colIdx, blocks, serverDataOld, oldMonths);
      Logger.log(`Удалено элементов для старого периода: ${deletedOldCount}`);
      
      // Очищаем календарь логов (в массиве!)
      Logger.log('Очищаем календарь логов перед переключением');
      clearAllLogsCalendarInArray(workingData, colIdx, blocks);
      hasChanges = true;
    }
    
    // 5. Парсим и синхронизируем НОВЫЙ месяц (в массиве!)
    const selectedMonth = planSheet.getRange(PLAN_ACTION_CONFIG.MONTH_CELL).getValue();
    workingData[0][colIdx.lastMonth] = selectedMonth; // Записываем в A1
    const selectedMonths = parseMonthString(selectedMonth);
    hasChanges = true;
    
    // 6. Обновляем заголовки дат для НОВОГО месяца (в массиве!)
    updateDateHeadersInArray(workingData, colIdx, selectedMonths);
    hasChanges = true;
    
    // 7. Загружаем данные с сервера для НОВОГО месяца (для отслеживания удалений)
    const serverDataBefore = loadTasksAndLogsFromServer(blocks, selectedMonths);
    
    // 8. Сохраняем изменения на сервер (только если месяц НЕ изменился) - из массива!
    // ВАЖНО: Используем workingData, а не allSheetData, чтобы НЕ сохранять task_id из очищенных блоков!
    let savedCount = 0;
    if (!monthChanged) {
      savedCount = saveTasksToServerFromArray(workingData, colIdx, blocks, selectedMonths, serverDataBefore);
    }
    
    // 9. Определяем и удаляем удаленные элементы (только если месяц НЕ изменился) - из массива!
    // ВАЖНО: Используем workingData, а не allSheetData, чтобы учесть очистку task_id в измененных блоках!
    let deletedCount = 0;
    if (!monthChanged) {
      deletedCount = detectAndDeleteRemovedItemsFromArray(workingData, colIdx, blocks, serverDataBefore, selectedMonths);
    }
    
    // 9. Загружаем свежие данные с сервера (после всех изменений и удалений)
    const serverData = loadTasksAndLogsFromServer(blocks, selectedMonths);
    
    // 10. Обновляем данные блоков (в массиве!)
    const warnings = updateBlocksDataInArray(workingData, colIdx, blocks, serverData, selectedMonths);
    hasChanges = true;
    
    // 11. Находим блоки для очистки (где есть nm_id в B, но пусто в D) - из массива!
    // ВАЖНО: Эти блоки НЕ проверялись в detectAndDeleteRemovedItems, поэтому задачи остаются в БД
    const blocksToClean = findBlocksToCleanFromArray(allSheetData, colIdx);
    if (blocksToClean.length > 0) {
      cleanBlocksInArray(workingData, colIdx, blocksToClean);
      hasChanges = true;
    }
    
    // 12. Синхронизируем nm_id: записываем из D в B (в массиве!)
    syncNmIdsInArray(workingData, colIdx, blocks);
    hasChanges = true;
    
    // 13. ОПТИМИЗИРОВАННАЯ ЗАПИСЬ ЧЕРЕЗ SHEETS API BATCHUPDATE!
    if (hasChanges) {
      writeAllChangesWithSheetsAPI(planSheet, workingData, colIdx, blocks, blocksToClean);
    }
    
    const endTime = new Date().getTime();
    const duration = (endTime - startTime) / 1000;

    const totalSavedCount = savedCount + savedChangedCount;
    
    let toastMessage = `✅ Обновлено: ${blocks.length} блоков`;
    if (!monthChanged && totalSavedCount > 0) {
      toastMessage += `, ${totalSavedCount} задач`;
    }
    const totalDeletedCount = deletedCount + deletedOldCount;
    if (totalDeletedCount > 0) {
      toastMessage += `, удалено: ${totalDeletedCount}`;
    }
    if (duplicatesCount > 0) {
      toastMessage += ` ⚠️ дубли: ${duplicatesCount}`;
    }
    toastMessage += ` (${duration.toFixed(1)} сек)`;

    if (warnings && warnings.length > 0) {
      toastMessage += ` ⚠️ ${warnings.length} предупр.`;
    }

    ss.toast(toastMessage, '✅ План Действий обновлен', 5);

    let fullMessage;
    if (monthChanged) {
      fullMessage = `Обновление завершено (месяц изменён)!\nПериод: ${selectedMonths.join(', ')}\nБлоков: ${blocks.length}`;
      if (savedChangedCount > 0) {
        fullMessage += `\nСохранено для новых nm_id: ${savedChangedCount}`;
      }
      if (deletedOldCount > 0) {
        fullMessage += `\nУдалено для старого периода: ${deletedOldCount}`;
      }
      if (duplicatesCount > 0) {
        fullMessage += `\n⚠️ Пропущено дубликатов nm_id: ${duplicatesCount}`;
      }
      fullMessage += `\nВремя: ${duration.toFixed(2)} сек.`;
    } else if (blocks.length === 0 && blocksToClean.length > 0) {
      fullMessage = `Обновление завершено!\nВсе блоки очищены: ${blocksToClean.length}\nВремя: ${duration.toFixed(2)} сек.`;
    } else {
      fullMessage = `Обновление завершено!\nБлоков: ${blocks.length}, Задач: ${totalSavedCount}, Удалено: ${totalDeletedCount}, Изменено nm_id: ${changedBlocks.length}, Очищено: ${blocksToClean.length}`;
      if (duplicatesCount > 0) {
        fullMessage += `\n⚠️ Пропущено дубликатов nm_id: ${duplicatesCount}`;
      }
      fullMessage += `\nВремя: ${duration.toFixed(2)} сек.`;
    }
    
    if (warnings && warnings.length > 0) {
      fullMessage += `\n⚠️ ПРЕДУПРЕЖДЕНИЯ:\n` + warnings.join('\n');
    }

  } catch (error) {
    ss.toast('❌ ' + error.message, '⚠️ Ошибка обновления', 10);
    Logger.log('Error: ' + error.message);
    Logger.log('Stack trace: ' + error.stack);
  }
}

/**
 * НОВАЯ ОПТИМИЗИРОВАННАЯ версия: Находит все блоки из массива данных
 * Работает с уже прочитанными данными без дополнительных запросов
 * ВАЖНО: Дубликаты nm_id игнорируются - обрабатывается только первый блок
 * @param {Array} allData - Массив всех данных листа (уже прочитан)
 * @param {Object} colIdx - Объект с индексами колонок
 * @returns {Object} - { blocks: Array, duplicatesCount: number }
 */
function findAllBlocksFromArray(allData, colIdx) {
  const blocks = [];
  const seenNmIds = new Set(); // Для отслеживания уже добавленных nm_id
  let emptyCount = 0;
  const maxEmptyBlocks = 3;
  let duplicatesCount = 0;

  for (let i = 0; i < allData.length; i += PLAN_ACTION_CONFIG.BLOCK_STEP) {
    const row = PLAN_ACTION_CONFIG.FIRST_NM_ID_ROW + i;
    if (row > allData.length) break;
    
    const rowIndex = row - 1; // 0-based для массива
    const nmId = allData[rowIndex][colIdx.nmId];
    
    if (nmId && !isNaN(nmId) && nmId > 0) {
      emptyCount = 0;
      const nmIdStr = nmId.toString();
      
      // Проверяем на дубликат
      if (seenNmIds.has(nmIdStr)) {
        duplicatesCount++;
        Logger.log(`⚠️ Дубликат nm_id=${nmIdStr} в строке D${row} - игнорируется (первый в D${blocks.find(b => b.nmId === nmIdStr).startRow})`);
        continue; // Пропускаем дубликат
      }
      
      seenNmIds.add(nmIdStr);
      blocks.push({
        startRow: row,
        nmId: nmIdStr
      });
    } else {
      emptyCount++;
      
      if (emptyCount >= maxEmptyBlocks) {
        Logger.log(`Остановка поиска: ${maxEmptyBlocks} пустых блока подряд после строки ${row - PLAN_ACTION_CONFIG.BLOCK_STEP * (maxEmptyBlocks - 1)}`);
        break;
      }
    }
  }
  
  if (duplicatesCount > 0) {
    Logger.log(`⚠️ Пропущено дубликатов nm_id: ${duplicatesCount}`);
  }
  
  return { blocks, duplicatesCount };
}

/**
 * НОВАЯ ОПТИМИЗИРОВАННАЯ версия: Находит блоки где nm_id изменился (D != B)
 * Работает с уже прочитанными данными без дополнительных запросов
 * @param {Array} allData - Массив всех данных листа
 * @param {Object} colIdx - Объект с индексами колонок
 * @param {Array} blocks - Массив активных блоков (с nm_id в D)
 * @returns {Array} - Массив блоков с измененным nm_id
 */
function findBlocksWithChangedNmIdFromArray(allData, colIdx, blocks) {
  if (blocks.length === 0) return [];

  const changedBlocks = [];

  blocks.forEach(block => {
    const rowIndex = block.startRow - 1; // 0-based для массива
    const originalNmId = allData[rowIndex][colIdx.originalNmId];
    
    // Проверяем: если B пустая или B != D
    if (!originalNmId || originalNmId === '' || String(originalNmId) !== String(block.nmId)) {
      changedBlocks.push({
        startRow: block.startRow,
        oldNmId: originalNmId || 'пусто',
        newNmId: block.nmId
      });
    }
  });
  
  return changedBlocks;
}

/**
 * НОВАЯ ОПТИМИЗИРОВАННАЯ версия: Находит блоки для очистки из массива данных
 * Работает с уже прочитанными данными без дополнительных запросов
 * @param {Array} allData - Массив всех данных листа
 * @param {Object} colIdx - Объект с индексами колонок
 * @returns {Array} - Массив блоков для очистки
 */
function findBlocksToCleanFromArray(allData, colIdx) {
  const blocksToClean = [];
  let emptyCount = 0;
  const maxEmptyBlocks = 3;

  for (let i = 0; i < allData.length; i += PLAN_ACTION_CONFIG.BLOCK_STEP) {
    const row = PLAN_ACTION_CONFIG.FIRST_NM_ID_ROW + i;
    if (row > allData.length) break;
    
    const rowIndex = row - 1; // 0-based для массива
    const originalNmId = allData[rowIndex][colIdx.originalNmId];
    const currentNmId = allData[rowIndex][colIdx.nmId];
    
    // ЛОГИКА: В B есть nm_id, но в D пусто (или другой nm_id)
    if (originalNmId && !isNaN(originalNmId) && originalNmId > 0) {
      // Проверяем, что в D пусто или отличается от B
      if (!currentNmId || currentNmId === '' || String(currentNmId).trim() === '') {
        emptyCount = 0;
        
        blocksToClean.push({
          startRow: row,
          originalNmId: String(originalNmId)
        });
        
        Logger.log(`Блок для очистки: nm_id=${originalNmId} в B${row}, D${row} пусто`);
      } else {
        emptyCount = 0;
      }
    } else {
      emptyCount++;
      
      if (emptyCount >= maxEmptyBlocks) {
        break;
      }
    }
  }
  
  return blocksToClean;
}

/**
 * Парсит строку месяца в массив
 * @param {string} monthStr - строка месяца в формате "Сент - Окт"
 * @returns {string[]} - массив месяцев в формате YYYY-MM
 */
function parseMonthString(monthStr) {
  if (!monthStr || monthStr === '') {
    const now = new Date();
    return [now.toISOString().slice(0, 7)];
  }

  const baseYear = 2026;

  const monthMap = {
    'Янв': '01', 'Фев': '02', 'Февр': '02', 'Март': '03', 'Апр': '04',
    'Май': '05', 'Июнь': '06', 'Июль': '07', 'Авг': '08',
    'Сент': '09', 'Окт': '10', 'Нояб': '11', 'Ноя': '11', 'Дек': '12'
  };

  const monthParts = String(monthStr).trim().split('-').map(m => m.trim());
  const months = [];

  // Особый случай: "Дек - Янв" = декабрь 2025 + январь 2026
  const isDecJan = monthParts.length === 2 && 
                   monthParts[0] === 'Дек' && 
                   monthParts[1] === 'Янв';

  monthParts.forEach(monthName => {
    const monthNum = monthMap[monthName];
    if (monthNum) {
      let year = baseYear;
      
      // Для "Дек - Янв": декабрь = 2025, январь = 2026
      if (isDecJan && monthName === 'Дек') {
        year = baseYear - 1; // 2025
      }
      
      months.push(`${year}-${monthNum}`);
    }
  });

  if (months.length === 0) {
    const now = new Date();
    months.push(now.toISOString().slice(0, 7));
  }
  
  return months;
}

/**
 * НОВАЯ ОПТИМИЗИРОВАННАЯ версия: Определяет и удаляет удаленные элементы из массива
 * Работает с уже прочитанными данными без дополнительных запросов
 * @param {Array} allData - Массив всех данных листа
 * @param {Object} colIdx - Объект с индексами колонок
 * @param {Array} blocks - Массив блоков
 * @param {Object} serverDataBefore - Данные с сервера ДО изменений
 * @param {Array} months - Месяцы
 * @returns {number} - Количество удаленных элементов
 */
function detectAndDeleteRemovedItemsFromArray(allData, colIdx, blocks, serverDataBefore, months) {
  if (blocks.length === 0) return 0;
  
  const logsToDelete = [];
  const taskIdsToDelete = [];

  const dates = generateDatesForMonths(months);
        const numDays = Math.min(dates.length, PLAN_ACTION_CONFIG.LOGS_DAYS);

  blocks.forEach(block => {
    const serverTasks = serverDataBefore[block.nmId] || [];
    const startRow = block.startRow + PLAN_ACTION_CONFIG.TASKS_START_ROW_OFFSET;
    
    // 1. Проверяем задачи: если есть task_id, но нет hypothesis - удаляем
    const taskIdMap = {}; // Для быстрого поиска строки по task_id
    
    for (let i = 0; i < PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
      const rowIndex = (startRow + i) - 1; // 0-based для массива
      if (rowIndex >= allData.length) continue;
      
      const rowData = allData[rowIndex];
      const taskId = rowData[colIdx.taskId];
      const hypothesis = rowData[colIdx.taskHypothesis];
      const row = startRow + i;
      
      if (taskId) {
        const numericTaskId = Number(taskId);
        taskIdMap[numericTaskId] = { row: row, rowIndex: rowIndex };
        
        if (!hypothesis || String(hypothesis).trim() === '') {
          Logger.log(`Задача ${numericTaskId} помечена на удаление (есть ID, но нет hypothesis)`);
          taskIdsToDelete.push(numericTaskId);

          const serverTask = serverTasks.find(t => t.task_id === numericTaskId);
          if (serverTask && serverTask.calendar_data) {
            const serverCalendarData = typeof serverTask.calendar_data === 'string'
              ? JSON.parse(serverTask.calendar_data)
              : serverTask.calendar_data;

            Object.keys(serverCalendarData).forEach(date => {
              Logger.log(`Лог удален (задача без hypothesis): task_id=${numericTaskId}, date=${date}`);
              logsToDelete.push({ task_id: numericTaskId, date: date });  
            });
          }
        }
      }
    }

    
    // 2. Проверяем логи: сравниваем с сервером (только для задач, которые НЕ помечены на удаление)
    serverTasks.forEach(serverTask => {
      const taskId = serverTask.task_id;
      
      // Если задача уже помечена на полное удаление - пропускаем проверку логов
      if (taskIdsToDelete.includes(taskId)) {
        return;
      }
      
      // Находим строку с этим task_id из предварительно прочитанной карты
      const taskInfo = taskIdMap[taskId];
      
      // Если задача не на листе - пропускаем проверку логов (задача может быть из другого периода)
      if (!taskInfo) {
        return;
      }

      if (serverTask.calendar_data) {
        const serverCalendarData = typeof serverTask.calendar_data === 'string'
          ? JSON.parse(serverTask.calendar_data)
          : serverTask.calendar_data;

        if (Object.keys(serverCalendarData).length === 0) {
          return;
        }

        const rowData = allData[taskInfo.rowIndex];

        const sheetLogs = {};
        for (let i = 0; i < numDays; i++) {
          const cellValue = rowData[colIdx.logsStart + i];
          if (cellValue && String(cellValue).trim() !== '') {
            sheetLogs[dates[i]] = String(cellValue).trim();
          }
        }
        
        // Проверяем какие логи были на сервере, но удалены на листе
        Object.keys(serverCalendarData).forEach(date => {
          if (!sheetLogs[date]) {
            Logger.log(`Лог удален: task_id=${taskId}, date=${date}`);
            logsToDelete.push({ task_id: taskId, date: date });
          }
        });
      }
    });
  });

  if (taskIdsToDelete.length === 0 && logsToDelete.length === 0) {
    Logger.log('Нет элементов для удаления');
    return 0;
  }

  Logger.log(`Удаление: задач=${taskIdsToDelete.length}, логов=${logsToDelete.length}`);

  const payload = {
    dataset: {
      name: 'wbActionPlanDelete',
      values: {}
    },
    ssId: PLAN_ACTION_CONFIG.SS_ID
  };

  if (taskIdsToDelete.length > 0) {
    payload.dataset.values.task_ids = taskIdsToDelete;
  }
  if (logsToDelete.length > 0) {
    payload.dataset.values.logs_to_delete = logsToDelete;
  }
  
  const result = btlzApi({
    url: '/ss/datasets/upload',
    payload: payload
  });

  if (result != null && typeof result === 'number') {
    Logger.log(`Удалено элементов: ${result}`);
    return result;
  }

  Logger.log('Ошибка удаления элементов: API не вернул результат');
  return 0;
}

/**
 * Загружает задачи и логи с сервера
 */
function loadTasksAndLogsFromServer(blocks, months) {
  if (!blocks || blocks.length === 0) {
    return {};
  }

  const nm_ids = blocks.map(b => Number(b.nmId));
  
  const result = btlzApi({
    url: '/ss/datasets/data',
    payload: {
      dataset: {
        name: 'wbActionPlan',
        values: {
          nm_ids: nm_ids,
          months: months
        }
      },
      ssId: PLAN_ACTION_CONFIG.SS_ID
    }
  });

  if (!result || !Array.isArray(result)) {
    Logger.log(`Ошибка загрузки данных с сервера: result=${result}, isArray=${Array.isArray(result)}`);
    return {};
  }

  Logger.log(`Загружено ${result.length} задач с сервера для ${blocks.length} блоков (${months.join(', ')})`);

  const dataByNmId = {};
  result.forEach(item => {
    const nmId = String(item.nm_id);
    if (!dataByNmId[nmId]) {
      dataByNmId[nmId] = [];
    }
    dataByNmId[nmId].push(item);
  });
  
  return dataByNmId;
}

/**
 * НОВАЯ ОПТИМИЗИРОВАННАЯ версия: Сохраняет задачи на сервер из массива
 * Работает с уже прочитанными данными без дополнительных запросов
 * @param {Array} allData - Массив всех данных листа
 * @param {Object} colIdx - Объект с индексами колонок
 * @param {Array} blocks - Массив блоков
 * @param {Array} month - Месяцы
 * @param {Object} serverDataBefore - Данные с сервера (для сохранения date_to)
 * @returns {number} - Количество сохраненных задач
 */
function saveTasksToServerFromArray(allData, colIdx, blocks, month, serverDataBefore) {
  if (blocks.length === 0) return 0;
  
  const dataToSave = [];
  const months = Array.isArray(month) ? month : [month];
  const firstMonth = months[0];
  
  const dates = generateDatesForMonths(months);
  const numDays = Math.min(dates.length, PLAN_ACTION_CONFIG.LOGS_DAYS);
  
  // Находим строки с задачами (проверяем hypothesis)
  const rowsWithTasks = [];
  blocks.forEach(block => {
    const startRow = block.startRow + PLAN_ACTION_CONFIG.TASKS_START_ROW_OFFSET;
    
  for (let i = 0; i < PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
      const rowIndex = (startRow + i) - 1; // 0-based для массива
      if (rowIndex >= allData.length) continue;
      
      const hypothesis = allData[rowIndex][colIdx.taskHypothesis];
      
      if (hypothesis && String(hypothesis).trim() !== '') {
        rowsWithTasks.push({
          block: block,
          rowIndex: rowIndex,
          row: startRow + i
        });
      }
    }
  });

  if (rowsWithTasks.length === 0) {
    return 0;
  }

  rowsWithTasks.forEach(item => {
    const rowData = allData[item.rowIndex];

    const taskId = rowData[colIdx.taskId];
    const staff = rowData[colIdx.taskStaff];
    const dateToFromSheet = rowData[colIdx.taskDateTo];
    const indicator = rowData[colIdx.taskIndicator];
    const problem = rowData[colIdx.taskProblem];
    const hypothesis = rowData[colIdx.taskHypothesis];
    const status = rowData[colIdx.taskStatus];

    const calendar_data = {};
    for (let j = 0; j < numDays; j++) {
      const cellValue = rowData[colIdx.logsStart + j];
      if (cellValue && String(cellValue).trim() !== '') {
        calendar_data[dates[j]] = String(cellValue).trim();
      }
    }

    const task = {
      nm_id: Number(item.block.nmId),
      indicator: indicator && String(indicator).trim() !== '' ? String(indicator).trim() : '',
      problem: problem && String(problem).trim() !== '' ? String(problem).trim() : '',
      hypothesis: String(hypothesis).trim(),
      staff: staff && String(staff).trim() !== '' ? String(staff).trim() : null,
      status: status === true || status === 'TRUE' || status === 1,
      calendar_data: calendar_data
    };
    
    // Обработка date_from и date_to
    if (taskId && !isNaN(taskId) && taskId > 0) {
      task.task_id = Number(taskId);
      
      // Если date_to указана в таблице, используем её (конвертируем из формата таблицы в формат API)
      if (dateToFromSheet) {
        const contextYear = firstMonth.split('-')[0]; // Извлекаем год из firstMonth (YYYY-MM)
        task.date_to = formatDateForAPI(dateToFromSheet, contextYear);
      } else {
        // Проверяем date_to в БД
        let existingDateTo = null;
        if (serverDataBefore && serverDataBefore[item.block.nmId]) {
          const serverTask = serverDataBefore[item.block.nmId].find(t => t.task_id === Number(taskId));
          if (serverTask && serverTask.date_to) {
            existingDateTo = serverTask.date_to;
          }
        }

        if (task.status) {
          // Если задача завершена, устанавливаем date_to (если её ещё нет)
          if (existingDateTo) {
            // Сохраняем существующую date_to из БД
            task.date_to = existingDateTo;
    } else {
            // Устанавливаем первое число первого месяца диапазона
            task.date_to = `${firstMonth}-01`;
          }
        }
      }
    } else {
      // Новая задача (нет task_id)
      const today = new Date();
      const currentMonth = today.toISOString().slice(0, 7);
      task.date_from = firstMonth === currentMonth
        ? Utilities.formatDate(today, Session.getScriptTimeZone(), 'yyyy-MM-dd')
        : `${firstMonth}-01`;

      // Обработка date_to для новой задачи
      if (dateToFromSheet) {
        // Если пользователь указал date_to в таблице
        const contextYear = firstMonth.split('-')[0];
        task.date_to = formatDateForAPI(dateToFromSheet, contextYear);
      } else if (task.status) {
        // Если задача сразу помечена как выполненная, устанавливаем date_to
        task.date_to = `${firstMonth}-01`;
      }
    }
    
    dataToSave.push(task);
  });
  
  // Суммарный лог по блокам (только с задачами)
  const blocksWithTasks = blocks.filter(block => {
    const count = dataToSave.filter(t => t.nm_id === Number(block.nmId)).length;
    return count > 0;
  });

  if (dataToSave.length === 0) {
    return 0;
  }
  
  const payload = {
    dataset: {
      name: 'wbActionPlanUpload',
      values: {
        data: dataToSave
      }
    },
    ssId: PLAN_ACTION_CONFIG.SS_ID
  };
  
  const result = btlzApi({
    url: '/ss/datasets/upload',
    payload: payload
  });

  if (result != null && typeof result === 'number') {
    Logger.log(`Сохранено задач на сервер: ${result}`);
    return result;
  }

  Logger.log('Ошибка сохранения задач: API не вернул результат');
  return 0;
}


/**
 * Генерирует массив дат для выбранных месяцев
 * @param {string|string[]} months - месяц в формате "YYYY-MM" или массив месяцев
 * @returns {string[]} - массив дат в формате "YYYY-MM-DD"
 */
function generateDatesForMonths(months) {
  const monthsArray = Array.isArray(months) ? months : [months];
  const dates = [];

  monthsArray.forEach(monthStr => {
    const [year, month] = monthStr.split('-').map(Number);

    const daysInMonth = new Date(year, month, 0).getDate();
    
    for (let day = 1; day <= daysInMonth; day++) {
      dates.push(`${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`);
    }
  });
  
  return dates;
}

/**
 * Преобразует букву колонки в номер (A -> 1, B -> 2, etc.)
 */
function columnLetterToNumber(letter) {
  let column = 0;
  const length = letter.length;
  for (let i = 0; i < length; i++) {
    column += (letter.charCodeAt(i) - 64) * Math.pow(26, length - i - 1);
  }
  return column;
}

/**
 * Конвертирует дату из формата API (YYYY-MM-DD) в серийный номер Google Sheets
 * @param {string} dateString - Дата в формате YYYY-MM-DD
 * @returns {number} - Серийный номер даты для Google Sheets
 */
function dateToSerialNumber(dateString) {
  if (!dateString || typeof dateString !== 'string') return null;

  const parts = dateString.split('-');
  if (parts.length !== 3) return null;

  const year = parseInt(parts[0], 10);
  const month = parseInt(parts[1], 10) - 1; // месяцы в JS начинаются с 0
  const day = parseInt(parts[2], 10);

  // Используем UTC, чтобы избежать смещения на 1 день из-за таймзоны
  const utcDateMs = Date.UTC(year, month, day);
  // Google Sheets epoch: 30 декабря 1899 (UTC)
  const epochUtcMs = Date.UTC(1899, 11, 30);
  const diff = utcDateMs - epochUtcMs;
  const serialNumber = Math.round(diff / (1000 * 60 * 60 * 24));

  return serialNumber;
}

/**
 * Конвертирует серийный номер Google Sheets обратно в формат API (YYYY-MM-DD)
 * @param {number} serialNumber - Серийный номер даты
 * @returns {string} - Дата в формате YYYY-MM-DD
 */
function serialNumberToDate(serialNumber) {
  if (!serialNumber || typeof serialNumber !== 'number') return '';

  // Google Sheets epoch: 30 декабря 1899 (UTC)
  const epochUtcMs = Date.UTC(1899, 11, 30);
  const date = new Date(epochUtcMs + serialNumber * 24 * 60 * 60 * 1000);

  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');

  return `${year}-${month}-${day}`;
}

/**
 * Конвертирует дату из формата таблицы (01.09 или 01.09.2025) в формат API (2025-09-01)
 * @param {string|number} sheetDate - Дата в формате DD.MM, DD.MM.YYYY или серийный номер
 * @param {string} contextYear - Год для использования если не указан в дате (опционально)
 * @returns {string} - Дата в формате YYYY-MM-DD
 */
function formatDateForAPI(sheetDate, contextYear) {
  // Если это число (серийный номер), конвертируем его
  if (typeof sheetDate === 'number') {
    return serialNumberToDate(sheetDate);
  }
  // Если это объект Date (Apps Script может вернуть Date)
  if (sheetDate instanceof Date) {
    return Utilities.formatDate(sheetDate, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }

  if (!sheetDate || typeof sheetDate !== 'string') return '';
  const parts = sheetDate.split('.');

  if (parts.length === 2) {
    // Формат DD.MM - добавляем год из контекста или текущий
    const year = contextYear || new Date().getFullYear().toString();
    return `${year}-${parts[1]}-${parts[0]}`;
  } else if (parts.length === 3) {
    // Формат DD.MM.YYYY
    return `${parts[2]}-${parts[1]}-${parts[0]}`;
  }

  return sheetDate;
}

/**
 * Загружает nm_id из листа "UNIT" в "План Действий"
 * Проверяет существующие nm_id и добавляет только новые
 * Использует batch-операции для максимальной производительности
 */
function loadNmIdsFromUnit() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  PLAN_ACTION_CONFIG.SS_ID = ss.getId();

  const unitSheet = ss.getSheetByName('UNIT');
  const planSheet = ss.getSheetByName(PLAN_ACTION_CONFIG.SHEET_NAME);

  if (!unitSheet) {
    ss.toast('Не найден лист "UNIT"', '❌ Ошибка', 5);
    return;
  }

  if (!planSheet) {
    ss.toast(`Не найден лист "${PLAN_ACTION_CONFIG.SHEET_NAME}"`, '❌ Ошибка', 5);
    return;
  }

  try {
    const startTime = new Date().getTime();

    const nmIdColNum = columnLetterToNumber(PLAN_ACTION_CONFIG.NM_ID_COL);

    const ranges = [
      'UNIT!F6:F',  // nm_id из UNIT (открытый диапазон от F6 до конца)
      `'${PLAN_ACTION_CONFIG.SHEET_NAME}'!D${PLAN_ACTION_CONFIG.FIRST_NM_ID_ROW}:D` // nm_id из План Действий (открытый диапазон)
    ];

    const batchGetResponse = Sheets.Spreadsheets.Values.batchGet(PLAN_ACTION_CONFIG.SS_ID, { ranges: ranges });
    const valueRanges = batchGetResponse.valueRanges;

    // 1. Обрабатываем nm_id из листа UNIT
    const nmIdsData = valueRanges[0].values || [];

    if (nmIdsData.length === 0) {
      ss.toast('Нет данных в листе "UNIT" (F6:F)', '⚠️ Предупреждение', 5);
      return;
    }

    // Используем Set для автоматического удаления дубликатов
    const unitNmIdsSet = new Set();
    for (let i = 0; i < nmIdsData.length; i++) {
      const nmId = nmIdsData[i][0];
      if (nmId && !isNaN(nmId) && nmId > 0) {
        unitNmIdsSet.add(Number(nmId));
      }
    }

    if (unitNmIdsSet.size === 0) {
      ss.toast('Нет валидных nm_id в листе "UNIT" (F6:F)', '⚠️ Предупреждение', 5);
      return;
    }

    const unitNmIds = Array.from(unitNmIdsSet);
    const duplicatesCount = nmIdsData.filter(row => row[0] && !isNaN(row[0]) && row[0] > 0).length - unitNmIds.length;

    // 2. Обрабатываем существующие nm_id из листа "План Действий" (D3, D34, D65...)
    const planNmIdsData = valueRanges[1].values || [];

    const existingNmIds = new Set();
    const existingBlocks = []; // Массив объектов {row, nmId}

    if (planNmIdsData.length > 0) {
      let emptyCount = 0;
      const maxEmptyBlocks = 5; // Останавливаемся после 5 пустых блоков подряд

      // Проходим по блокам (D3, D34, D65...)
      for (let i = 0; i < planNmIdsData.length; i += PLAN_ACTION_CONFIG.BLOCK_STEP) {
        const row = PLAN_ACTION_CONFIG.FIRST_NM_ID_ROW + i;
        const nmId = planNmIdsData[i] ? planNmIdsData[i][0] : null;

        if (nmId && !isNaN(nmId) && nmId > 0) {
          existingNmIds.add(Number(nmId));
          existingBlocks.push({ row: row, nmId: Number(nmId) });
          emptyCount = 0;
        } else {
          emptyCount++;
          // Оптимизация: прекращаем поиск после нескольких пустых блоков
          if (emptyCount >= maxEmptyBlocks) {
            Logger.log(`Остановка чтения после ${maxEmptyBlocks} пустых блоков (строка ${row})`);
            break;
          }
        }
      }
    }

    // 3. Определяем новые nm_id (которых нет на листе)
    const newNmIds = unitNmIds.filter(nmId => !existingNmIds.has(nmId));

    if (newNmIds.length === 0) {
      ss.toast(`Все ${unitNmIds.length} nm_id уже есть на листе`, 'ℹ️ Загрузка не требуется', 5);
      return;
    }

    // 4. Находим первую свободную позицию для добавления новых nm_id
    let nextFreeRow;
    if (existingBlocks.length === 0) {
      // Если на листе нет блоков, начинаем с D3
      nextFreeRow = PLAN_ACTION_CONFIG.FIRST_NM_ID_ROW;
    } else {
      // Находим последний блок и добавляем после него
      const lastBlock = existingBlocks[existingBlocks.length - 1];
      nextFreeRow = lastBlock.row + PLAN_ACTION_CONFIG.BLOCK_STEP;
    }
    
    // 5. Формируем batch-запросы для записи nm_id
    const addedRows = [];
    const requests = [];
    const sheetId = planSheet.getSheetId();
    
    for (let i = 0; i < newNmIds.length; i++) {
      const targetRow = nextFreeRow + i * PLAN_ACTION_CONFIG.BLOCK_STEP;
      const nmId = newNmIds[i];
      addedRows.push(targetRow);
      
      // Записываем nm_id в колонку D
      requests.push({
        updateCells: {
          range: {
            sheetId: sheetId,
            startRowIndex: targetRow - 1,
            endRowIndex: targetRow,
            startColumnIndex: nmIdColNum - 1, // D (0-based)
            endColumnIndex: nmIdColNum
          },
          rows: [{
            values: [{ userEnteredValue: { numberValue: nmId }}]
          }],
          fields: 'userEnteredValue'
        }
      });
    }

    // 6. Выполняем все запросы одним batchUpdate
    if (requests.length > 0) {
      Sheets.Spreadsheets.batchUpdate({ requests: requests }, PLAN_ACTION_CONFIG.SS_ID);
    }
    
    const endTime = new Date().getTime();
    const duration = (endTime - startTime) / 1000;
    
    // Краткое уведомление для toast
    let toastMessage = `✅ Добавлено ${newNmIds.length} nm_id`;
    if (duplicatesCount > 0) {
      toastMessage += ` (пропущено дубл.: ${duplicatesCount})`;
    }
    toastMessage += ` (${duration.toFixed(1)} сек)`;
    
    ss.toast(toastMessage, '✅ Загрузка из UNIT завершена', 5);
    
    // Полная информация в логи
    let fullMessage = `Загрузка завершена!\nУникальных nm_id в UNIT: ${unitNmIds.length}`;
    if (duplicatesCount > 0) {
      fullMessage += `\nДубликатов пропущено: ${duplicatesCount}`;
    }
    fullMessage += `\nУже было на листе: ${existingNmIds.size}\nДобавлено новых: ${newNmIds.length}`;
    fullMessage += `\nСтроки: ${addedRows.map(r => 'D' + r).join(', ')}\nВремя: ${duration.toFixed(2)} сек.`;
    
  } catch (error) {
    ss.toast('❌ ' + error.message, '⚠️ Ошибка загрузки', 10);
    Logger.log('Error: ' + error.message);
    Logger.log('Stack trace: ' + error.stack);
  }
}

// ============================================================================
// НОВЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С МАССИВОМ (БЕЗ ЗАПИСИ В ЛИСТ)
// ============================================================================

/**
 * Обновляет заголовки дат в массиве (вместо записи в лист)
 * @param {Array} workingData - Рабочий массив данных
 * @param {Object} colIdx - Индексы колонок
 * @param {Array} months - Массив месяцев
 */
function updateDateHeadersInArray(workingData, colIdx, months) {
  const dates = generateDatesForMonths(months);
  const numDays = Math.min(dates.length, PLAN_ACTION_CONFIG.LOGS_DAYS);

  for (let i = 0; i < numDays; i++) {
    const date = dates[i];
    const parts = date.split('-');
    const formattedDate = `${parts[2]}.${parts[1]}`;
    workingData[0][colIdx.logsStart + i] = formattedDate;
  }

  for (let i = numDays; i < PLAN_ACTION_CONFIG.LOGS_DAYS; i++) {
    workingData[0][colIdx.logsStart + i] = '';
  }
  
  Logger.log(`Обновлены заголовки дат в массиве: ${numDays} дней (${dates[0]} - ${dates[numDays-1]})`);
}

/**
 * Очищает задачи и логи для измененных блоков в массиве
 * @param {Array} workingData - Рабочий массив данных
 * @param {Object} colIdx - Индексы колонок
 * @param {Array} changedBlocks - Блоки с измененным nm_id
 */
function clearChangedBlocksInArray(workingData, colIdx, changedBlocks) {
  if (changedBlocks.length === 0) return;

  changedBlocks.forEach(block => {
    const startRow = block.startRow + PLAN_ACTION_CONFIG.TASKS_START_ROW_OFFSET;

    for (let i = 0; i < PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
      const rowIndex = (startRow + i) - 1;
      if (rowIndex >= workingData.length) continue;

      workingData[rowIndex][colIdx.originalNmId] = '';
      workingData[rowIndex][colIdx.taskId] = '';
      workingData[rowIndex][colIdx.taskIndicator] = '';
      workingData[rowIndex][colIdx.taskProblem] = '';
      workingData[rowIndex][colIdx.taskHypothesis] = '';
      workingData[rowIndex][colIdx.taskStaff] = '';
      workingData[rowIndex][colIdx.taskDateTo] = '';
      workingData[rowIndex][colIdx.taskStatus] = '';

      for (let j = 0; j < PLAN_ACTION_CONFIG.LOGS_DAYS; j++) {
        workingData[rowIndex][colIdx.logsStart + j] = '';
  }

      workingData[rowIndex][colIdx.taskNum] = i + 1;
    }
  });
  
  Logger.log(`Очищены данные в массиве для ${changedBlocks.length} блоков с измененным nm_id`);
}

/**
 * Обновляет данные блоков (задачи) в массиве
 * @param {Array} workingData - Рабочий массив данных
 * @param {Object} colIdx - Индексы колонок
 * @param {Array} blocks - Массив блоков
 * @param {Object} serverData - Данные задач с сервера
 * @param {Array} months - Массив месяцев
 * @returns {Array} - Массив предупреждений
 */
function updateBlocksDataInArray(workingData, colIdx, blocks, serverData, months) {
  const warnings = [];
  const dates = generateDatesForMonths(months);
  const numDays = Math.min(dates.length, PLAN_ACTION_CONFIG.LOGS_DAYS);
  
  blocks.forEach(block => {
    // Задачи с сервера
    const tasks = serverData[block.nmId] || [];
    
    if (tasks.length > PLAN_ACTION_CONFIG.DANGERS_ROWS) {
      warnings.push(`nm_id ${block.nmId}: Задач ${tasks.length}, показано ${PLAN_ACTION_CONFIG.DANGERS_ROWS}`);
    }
    
    const tasksStartRow = block.startRow + PLAN_ACTION_CONFIG.TASKS_START_ROW_OFFSET;
    
    // Сначала очищаем блок задач и логов
    for (let i = 0; i < PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
      const rowIndex = (tasksStartRow + i) - 1;
      if (rowIndex >= workingData.length) continue;
      
      workingData[rowIndex][colIdx.taskId] = '';
      workingData[rowIndex][colIdx.taskIndicator] = '';
      workingData[rowIndex][colIdx.taskProblem] = '';
      workingData[rowIndex][colIdx.taskHypothesis] = '';
      workingData[rowIndex][colIdx.taskStaff] = '';
      workingData[rowIndex][colIdx.taskDateTo] = '';
      workingData[rowIndex][colIdx.taskStatus] = false;
      
      // Очищаем логи
      for (let j = 0; j < numDays; j++) {
        workingData[rowIndex][colIdx.logsStart + j] = '';
      }
      
      // Восстанавливаем нумерацию
      workingData[rowIndex][colIdx.taskNum] = i + 1;
    }
    
    // Записываем задачи
    for (let i = 0; i < Math.min(tasks.length, PLAN_ACTION_CONFIG.DANGERS_ROWS); i++) {
      const task = tasks[i];
      const rowIndex = (tasksStartRow + i) - 1;
      if (rowIndex >= workingData.length) continue;
      
      workingData[rowIndex][colIdx.taskId] = task.task_id || '';
      workingData[rowIndex][colIdx.taskIndicator] = task.indicator || '';
      workingData[rowIndex][colIdx.taskProblem] = task.problem || '';
      workingData[rowIndex][colIdx.taskHypothesis] = task.hypothesis || '';
      workingData[rowIndex][colIdx.taskStaff] = task.staff || '';
      workingData[rowIndex][colIdx.taskDateTo] = task.date_to ? dateToSerialNumber(task.date_to) : '';
      workingData[rowIndex][colIdx.taskStatus] = task.status || false;
      
      // Записываем логи
      const calendarData = typeof task.calendar_data === 'string' 
        ? JSON.parse(task.calendar_data) 
        : (task.calendar_data || {});
      
      for (let j = 0; j < numDays; j++) {
        const value = calendarData[dates[j]];
        workingData[rowIndex][colIdx.logsStart + j] = value || '';
      }
    }
  });
  
  Logger.log(`Обновлены данные ${blocks.length} блоков в массиве`);
  return warnings;
}

/**
 * Синхронизирует nm_id (D -> B) в массиве
 * @param {Array} workingData - Рабочий массив данных
 * @param {Object} colIdx - Индексы колонок
 * @param {Array} blocks - Массив блоков
 */
function syncNmIdsInArray(workingData, colIdx, blocks) {
  if (blocks.length === 0) return;
  
  blocks.forEach(block => {
    // Записываем nm_id во ВСЕ 30 строк блока
    for (let i = 0; i < PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
      const rowIndex = (block.startRow + i) - 1; // 0-based
      if (rowIndex >= workingData.length) continue;
      
      workingData[rowIndex][colIdx.originalNmId] = block.nmId;
    }
  });
  
  Logger.log(`Синхронизировано ${blocks.length} nm_id (D -> B) в массиве`);
}

/**
 * Очищает блоки в массиве
 * @param {Array} workingData - Рабочий массив данных
 * @param {Object} colIdx - Индексы колонок
 * @param {Array} blocksToClean - Блоки для очистки
 */
function cleanBlocksInArray(workingData, colIdx, blocksToClean) {
  if (blocksToClean.length === 0) return;
  
  blocksToClean.forEach(block => {
    const startRow = block.startRow;
    
    for (let i = 0; i < PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
      const rowIndex = (startRow + i) - 1;
      if (rowIndex >= workingData.length) continue;
      
      // Очищаем: B, A, K-P, R-CA (НЕ трогаем E-G - там формула опасностей)
      workingData[rowIndex][colIdx.originalNmId] = '';
      workingData[rowIndex][colIdx.taskId] = '';
      workingData[rowIndex][colIdx.taskIndicator] = '';
      workingData[rowIndex][colIdx.taskProblem] = '';
      workingData[rowIndex][colIdx.taskHypothesis] = '';
      workingData[rowIndex][colIdx.taskStaff] = '';
      workingData[rowIndex][colIdx.taskDateTo] = '';
      workingData[rowIndex][colIdx.taskStatus] = '';
      
      // Очищаем логи
      for (let j = 0; j < PLAN_ACTION_CONFIG.LOGS_DAYS; j++) {
        workingData[rowIndex][colIdx.logsStart + j] = '';
      }
      
      // Восстанавливаем нумерацию
      workingData[rowIndex][colIdx.taskNum] = i + 1;
    }
  });
    
  Logger.log(`Очищено блоков в массиве: ${blocksToClean.length}`);
}

/**
 * Очищает календарь логов в массиве
 * @param {Array} workingData - Рабочий массив данных
 * @param {Object} colIdx - Индексы колонок
 * @param {Array} blocks - Массив блоков
 */
function clearAllLogsCalendarInArray(workingData, colIdx, blocks) {
  if (blocks.length === 0) return;
  
  blocks.forEach(block => {
    const startRow = block.startRow + PLAN_ACTION_CONFIG.TASKS_START_ROW_OFFSET;
    
    for (let i = 0; i < PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
      const rowIndex = (startRow + i) - 1;
      if (rowIndex >= workingData.length) continue;
      
      // Очищаем логи
      for (let j = 0; j < PLAN_ACTION_CONFIG.LOGS_DAYS; j++) {
        workingData[rowIndex][colIdx.logsStart + j] = '';
      }
    }
  });
  
  Logger.log(`Очищен календарь логов в массиве для ${blocks.length} блоков`);
}

/**
 * Записывает все изменения через Sheets API batchUpdate
 * @param {Sheet} sheet - Лист Google Sheets
 * @param {Array} workingData - Рабочий массив данных
 * @param {Object} colIdx - Индексы колонок
 * @param {Array} blocks - Массив блоков
 * @param {Array} blocksToClean - Блоки для очистки
 */
function writeAllChangesWithSheetsAPI(sheet, workingData, colIdx, blocks, blocksToClean) {
  const sheetId = sheet.getSheetId();
  const requests = [];
  
  // 1. Записываем заголовок A1 (lastMonth) - НЕ трогаем H1 (там формула "В работе")!
  requests.push({
    updateCells: {
      range: {
        sheetId: sheetId,
        startRowIndex: 0,
        endRowIndex: 1,
        startColumnIndex: colIdx.lastMonth,
        endColumnIndex: colIdx.lastMonth + 1
      },
      rows: [{
        values: [{
          userEnteredValue: { stringValue: String(workingData[0][colIdx.lastMonth] || '') }
        }]
      }],
      fields: 'userEnteredValue'
    }
  });
  
  // 2. Записываем заголовки дат (R1:CA1) - НЕ трогаем H1!
  const headerDates = workingData[0].slice(colIdx.logsStart, colIdx.logsStart + PLAN_ACTION_CONFIG.LOGS_DAYS);
  const headerRows = [{
    values: headerDates.map(date => 
      date ? { userEnteredValue: { stringValue: String(date) }} : {}
    )
  }];
  
  requests.push({
    updateCells: {
      range: {
        sheetId: sheetId,
        startRowIndex: 0,
        endRowIndex: 1,
        startColumnIndex: colIdx.logsStart,
        endColumnIndex: colIdx.logsStart + PLAN_ACTION_CONFIG.LOGS_DAYS
      },
      rows: headerRows,
      fields: 'userEnteredValue'
    }
  });
  
  // 3. Записываем данные для каждого блока (БЕЗ колонок C, D, E-G, H - там формулы!)
  blocks.forEach(block => {
    const startRowIndex = block.startRow - 1; // 0-based
    const endRowIndex = startRowIndex + PLAN_ACTION_CONFIG.DANGERS_ROWS;
    
    // Извлекаем данные блока из workingData
    const rowsAB = [];    // A-B (task_id, originalNmId)
    const rowsJ = [];     // J (нумерация)
    const rowsKN = [];    // K-N (indicator, problem, hypothesis, staff)
    const rowsO = [];     // O (date_to)
    const rowsP = [];     // P (status)
    const rowsQBZ = [];   // R-CA (логи)
    
    for (let i = 0; i < PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
      const rowIndex = startRowIndex + i;
      if (rowIndex >= workingData.length) continue;
      
      const row = workingData[rowIndex];
      
      // A-B
      rowsAB.push({
        values: [
          row[colIdx.taskId] ? { userEnteredValue: { numberValue: Number(row[colIdx.taskId]) }} : {},
          row[colIdx.originalNmId] ? { userEnteredValue: { numberValue: Number(row[colIdx.originalNmId]) }} : {}
        ]
      });
      
      // J (нумерация)
      rowsJ.push({
        values: [{ userEnteredValue: { numberValue: i + 1 }}]
      });
      
      // K-N (indicator, problem, hypothesis, staff) - текст без форматирования
      rowsKN.push({
        values: [
          row[colIdx.taskIndicator] ? { userEnteredValue: { stringValue: String(row[colIdx.taskIndicator]) }} : {},
          row[colIdx.taskProblem] ? { userEnteredValue: { stringValue: String(row[colIdx.taskProblem]) }} : {},
          row[colIdx.taskHypothesis] ? { userEnteredValue: { stringValue: String(row[colIdx.taskHypothesis]) }} : {},
          row[colIdx.taskStaff] ? { userEnteredValue: { stringValue: String(row[colIdx.taskStaff]) }} : {}
        ]
      });

      // O (date_to) - только значение (формат применяется ко всему столбцу в конце)
      rowsO.push({
        values: [
          row[colIdx.taskDateTo] ? { userEnteredValue: { numberValue: row[colIdx.taskDateTo] }} : {}
        ]
      });

      // P (status) - checkbox без форматирования
      rowsP.push({
        values: [
          { userEnteredValue: { boolValue: Boolean(row[colIdx.taskStatus]) }}
        ]
      });
      
      // R-CA (логи)
      rowsQBZ.push({
        values: row.slice(colIdx.logsStart, colIdx.logsStart + PLAN_ACTION_CONFIG.LOGS_DAYS).map(log => 
          log ? { userEnteredValue: { stringValue: String(log) }} : {}
        )
      });
    }
    
    // Записываем каждый диапазон отдельно (НЕ трогаем C, D, E-G, H - там формулы!)
    
    // A-B
    requests.push({
      updateCells: {
        range: {
          sheetId: sheetId,
          startRowIndex: startRowIndex,
          endRowIndex: endRowIndex,
          startColumnIndex: 0, // A
          endColumnIndex: 2    // B
        },
        rows: rowsAB,
        fields: 'userEnteredValue'
      }
    });
    
    // J (нумерация)
    requests.push({
      updateCells: {
        range: {
          sheetId: sheetId,
          startRowIndex: startRowIndex,
          endRowIndex: endRowIndex,
          startColumnIndex: 9,  // J
          endColumnIndex: 10
        },
        rows: rowsJ,
        fields: 'userEnteredValue'
      }
    });
    
    // K-N (indicator, problem, hypothesis, staff) - только значения
    requests.push({
      updateCells: {
        range: {
          sheetId: sheetId,
          startRowIndex: startRowIndex,
          endRowIndex: endRowIndex,
          startColumnIndex: 10, // K
          endColumnIndex: 14    // N
        },
        rows: rowsKN,
        fields: 'userEnteredValue'
      }
    });

    // O (date_to) - только значения (формат применяется ко всему столбцу в конце)
    requests.push({
      updateCells: {
        range: {
          sheetId: sheetId,
          startRowIndex: startRowIndex,
          endRowIndex: endRowIndex,
          startColumnIndex: 14, // O
          endColumnIndex: 15    // O
        },
        rows: rowsO,
        fields: 'userEnteredValue'
      }
    });

    // P (status) - только значения
    requests.push({
      updateCells: {
        range: {
          sheetId: sheetId,
          startRowIndex: startRowIndex,
          endRowIndex: endRowIndex,
          startColumnIndex: 15, // P
          endColumnIndex: 16    // P
        },
        rows: rowsP,
        fields: 'userEnteredValue'
      }
    });
    
    // R-CA (логи)
    requests.push({
      updateCells: {
        range: {
          sheetId: sheetId,
          startRowIndex: startRowIndex,
          endRowIndex: endRowIndex,
          startColumnIndex: colIdx.logsStart,
          endColumnIndex: colIdx.logsStart + PLAN_ACTION_CONFIG.LOGS_DAYS
        },
        rows: rowsQBZ,
        fields: 'userEnteredValue'
      }
    });
  });
  
  // 4. Очищаем блоки для удаления (D пусто, B заполнена) - только визуально!
  // НЕ трогаем E-G - там формула опасностей
  if (blocksToClean && blocksToClean.length > 0) {
    blocksToClean.forEach(block => {
      const startRowIndex = block.startRow - 1;
      const endRowIndex = startRowIndex + PLAN_ACTION_CONFIG.DANGERS_ROWS;
      
      // Очищаем A-B
      requests.push({
        updateCells: {
          range: {
            sheetId: sheetId,
            startRowIndex: startRowIndex,
            endRowIndex: endRowIndex,
            startColumnIndex: 0, // A
            endColumnIndex: 2    // B
          },
          fields: 'userEnteredValue'
        }
      });
      
      // Восстанавливаем нумерацию J
      const numRows = [];
      for (let i = 1; i <= PLAN_ACTION_CONFIG.DANGERS_ROWS; i++) {
        numRows.push({
          values: [{ userEnteredValue: { numberValue: i }}]
        });
      }
      
      requests.push({
        updateCells: {
          range: {
            sheetId: sheetId,
            startRowIndex: startRowIndex,
            endRowIndex: endRowIndex,
            startColumnIndex: 9,  // J
            endColumnIndex: 10
          },
          rows: numRows,
          fields: 'userEnteredValue'
        }
      });
      
      // Очищаем K-P (задачи)
      requests.push({
        updateCells: {
          range: {
            sheetId: sheetId,
            startRowIndex: startRowIndex,
            endRowIndex: endRowIndex,
            startColumnIndex: 10, // K
            endColumnIndex: 16    // P
          },
          fields: 'userEnteredValue'
        }
      });
      
      // Очищаем R-CA (логи)
      requests.push({
        updateCells: {
          range: {
            sheetId: sheetId,
            startRowIndex: startRowIndex,
            endRowIndex: endRowIndex,
            startColumnIndex: colIdx.logsStart,
            endColumnIndex: colIdx.logsStart + PLAN_ACTION_CONFIG.LOGS_DAYS
          },
          fields: 'userEnteredValue'
        }
      });
    });
      
    Logger.log(`Подготовлено очистки для ${blocksToClean.length} блоков (только визуально, данные на сервере сохранены)`);
  }
  
  // В КОНЦЕ форматируем ВЕСЬ столбец O (date_to) с форматом 'dd.mm' и центрированием
  const dateToColIndex = columnLetterToNumber(PLAN_ACTION_CONFIG.TASKS_COLS.DATE_TO) - 1; // O = 14
  requests.push({
    repeatCell: {
      range: {
        sheetId: sheetId,
        startColumnIndex: dateToColIndex,
        endColumnIndex: dateToColIndex + 1
        // Весь столбец (без startRowIndex/endRowIndex)
      },
      cell: {
        userEnteredFormat: {
          numberFormat: {
            type: 'DATE',
            pattern: 'dd"."mm'
          },
          horizontalAlignment: 'CENTER',
          verticalAlignment: 'MIDDLE'
        }
      },
      fields: 'userEnteredFormat(numberFormat,horizontalAlignment,verticalAlignment)'
    }
  });
  
  // 5. Выполняем все запросы одним batchUpdate
  if (requests.length > 0) {
    Sheets.Spreadsheets.batchUpdate({
      requests: requests
    }, PLAN_ACTION_CONFIG.SS_ID);
    const cleanedText = blocksToClean.length > 0 ? `, очищено ${blocksToClean.length} блоков` : '';
    Logger.log(`Записано ${blocks.length} блоков (A-B, J, K-P, R-CA)${cleanedText} за одну Sheets API операцию (${requests.length} запросов). Колонки C, D, E-G, H не тронуты.`);
  }
}
