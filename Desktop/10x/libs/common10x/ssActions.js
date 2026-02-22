
function getEffectiveUser(postData) {
  return Session.getActiveUser().getEmail()
}

/**
 * 
 * @param { Object } postData 
 * @param { string } postData.email
 * @param { string } postData.projectName
 * @returns 
 */
function create(postData) {
  const { template, client_name, spreadsheet_name } = postData;
  if (!template) throw Error("template must be defined");
  if (!client_name) throw Error("client_name must be defined");
  if (!spreadsheet_name) throw Error("spreadsheet_name must be defined");
  const CLIENTS_FOLDER_URL = "https://drive.google.com/drive/u/0/folders/1WqYiFHXjkNpud9s3Ls9R9AuAQpgXyrL3";

  const folder = this._createFolder(client_name, CLIENTS_FOLDER_URL);
  const spreadsheet = this._createSpreadsheet(template, spreadsheet_name, folder);
  // this._setPermissions(folder, sheet, adminEmail, email)
  return { folderUrl: folder.getUrl(), sheetUrl: spreadsheet.getUrl(), sheetId: spreadsheet.getId() }
}

/**
 * 
 * @param { Object } postData 
 * @param { string } postData.ssId
 * @param { string } postData.email
 * @returns 
 */
function spreadsheetSetSharing(postData) {
  const { ssId, accessType, permissionType } = postData;
  if (!ssId) throw Error("ssId is required");
  if (!accessType) throw Error("accessType is required");
  if (!(accessType in DriveApp.Access)) throw Error("invalit accessType");
  if (!permissionType) throw Error("permissionType is required");
  if (!(permissionType in DriveApp.Permission)) throw Error("invalit permissionType");
  const file = DriveApp.getFileById(ssId);
  file.setSharing(DriveApp.Access[accessType], DriveApp.Permission[permissionType])
  return `sharings set to ${accessType}, ${permissionType}`
}

/**
 * 
 * @param { Object } postData 
 * @param { string } postData.email
 * @param { string } postData.projectName
 * @returns 
 */
function setEditors(postData) {
  const { ssId, editors } = postData;
  if (!ssId) throw Error(`ssId is not defined`);
  if (!editors) throw Error(`editors is not defined`);
  if (!Array.isArray(editors)) throw Error(`editors is not array`);

  const spreadsheet = SpreadsheetApp.openById(ssId);
  const currentEditors = spreadsheet.getEditors().map(e => e.getEmail());
  const owner = spreadsheet.getOwner().getEmail();

  const editorsToAdd = [];
  const editorsToRemove = [];

  for (const editor of editors) {
    if (editor === owner) continue;
    if (!currentEditors.includes(editor)) { editorsToAdd.push(editor); }
  }
  for (const editor of currentEditors) {
    if (editor === owner) continue;
    if (!editors.includes(editor)) { editorsToRemove.push(editor); }
  }
  for (const editor of editorsToRemove) {
    spreadsheet.removeEditor(editor)

  }
  spreadsheet.addEditors(editorsToAdd);
  let message = [];
  if (editorsToAdd.length) message.push(`editors ${editorsToAdd.join(", ")} added`);
  if (editorsToRemove.length) message.push(`editors ${editorsToRemove.join(", ")} removed`)
  if (message.length === 0) message.push(`editors not changed`)
  return message.join(", ")
}
/**
 * 
 * @param { Object } postData 
 * @param { string } postData.email
 * @param { string } postData.projectName
 * @returns 
 */
function addEditors(postData) {
  const { ssId, editors } = postData;
  if (!ssId) throw Error(`ssId is not defined`);
  if (!editors) throw Error(`editors is not defined`);
  if (!Array.isArray(editors)) throw Error(`editors is not array`);

  const spreadsheet = SpreadsheetApp.openById(ssId);
  const currentEditors = spreadsheet.getEditors().map(e => e.getEmail());
  const owner = spreadsheet.getOwner().getEmail();

  const editorsToAdd = [];

  for (const editor of editors) {
    if (editor === owner) continue;
    if (!currentEditors.includes(editor)) { editorsToAdd.push(editor); }
  }
  spreadsheet.addEditors(editorsToAdd);
  let message = [];
  if (editorsToAdd.length) message.push(`editors ${editorsToAdd.join(", ")} added`);
  if (message.length === 0) message.push(`editors not changed`)
  return message.join(", ")
}

/**
 * 
 * @param { Object } postData 
 * @param { string } postData.email
 * @param { string } postData.projectName
 * @returns 
 */
function removeEditors(postData) {
  const { ssId, editors } = postData;
  if (!ssId) throw Error(`ssId is not defined`);
  if (!editors) throw Error(`editors is not defined`);
  if (!Array.isArray(editors)) throw Error(`editors is not array`);
  const spreadsheet = SpreadsheetApp.openById(ssId);
  const currentEditors = spreadsheet.getEditors().map(e => e.getEmail());
  const owner = spreadsheet.getOwner().getEmail();

  const editorsToRemove = [];

  for (const editor of editors) {
    if (editor === owner) continue;
    if (currentEditors.includes(editor)) { editorsToRemove.push(editor); }
  }
  for (const editor of editorsToRemove) {
    spreadsheet.removeEditor(editor)

  }
  let message = [];
  if (editorsToRemove.length) message.push(`editors ${editorsToRemove.join(", ")} removed`)
  if (message.length === 0) message.push(`editors not changed`)
  return message.join(", ")
}

/**
 * 
 * @param { Object } postData 
 * @param { string } postData.email
 * @param { string } postData.projectName
 * @returns 
 */
function pruneEditors(postData) {
  const { ssId } = postData;
  if (!ssId) throw Error(`ssId is not defined`);

  const spreadsheet = SpreadsheetApp.openById(ssId);
  const currentEditors = spreadsheet.getEditors().map(e => e.getEmail());
  const owner = spreadsheet.getOwner().getEmail();

  const editorsToRemove = [];

  for (const editor of currentEditors) {
    if (editor === owner) continue;
    editorsToRemove.push(editor)
  }
  for (const editor of editorsToRemove) {
    spreadsheet.removeEditor(editor)

  }
  let message = [];
  if (editorsToRemove.length) message.push(`editors ${editorsToRemove.join(", ")} removed`)
  if (message.length === 0) message.push(`editors not changed`)
  return message.join(", ")
}

/**
 * 
 * @param { Object } postData 
 * @param { string } postData.ssId
 * @param { string } postData.sheetName
 * @param { {[key: string]: any}[] } postData.dataItems
 * @returns 
 */
function insertData(postData) {
  // action to replace WB reports
  // body: ssId <string> - id таблицы проекта; sheetName <string> - название листа выгрузки отчета; dataItems <string> - данные выгрузки; 
  const { ssId, sheetName, keysRow = 1, dataRowFirst = 3, dataItems } = postData;
  const spreadsheet = Sheets.Spreadsheets.get(ssId);
  new DB({ sheetName, ssId, keysRow, dataRowFirst }).insertDataItems(dataItems);
  return "data inserted succesfull";
}

/**
 * 
 * @param { Object } postData 
 * @param { string } postData.ssId
 * @param { string } postData.sheetName
 * @param { {[key: string]: any}[] } postData.dataItems
 * @returns 
 */
function replaceData(postData) {
  // action to replace WB reports
  // body: ssId <string> - id таблицы проекта; sheetName <string> - название листа выгрузки отчета; dataItems <string> - данные выгрузки; 
  const { ssId, sheetName, keysRow = 1, dataRowFirst = 3, dataItems } = postData;
  const spreadsheet = Sheets.Spreadsheets.get(ssId);

  new DB({ sheetName, ssId, keysRow, dataRowFirst }).replaceData(dataItems);
  return "data replace succesfull";
}
/**
 * 
 * @param { Object } postData 
 * @param { string } postData.ssId
 * @param { string } postData.sheetName
 * @param { {[key: string]: any}[] } postData.dataItems
 * @returns 
 */
function mergeData(postData) {
  // action to replace WB reports
  // body: ssId <string> - id таблицы проекта; sheetName <string> - название листа выгрузки отчета; dataItems <string> - данные выгрузки; 
  const { ssId, sheetName, keysRow = 1, dataRowFirst = 3, dataItems } = postData;
  const spreadsheet = Sheets.Spreadsheets.get(ssId);

  if (!postData.useKeys || !Array.isArray(postData.useKeys)) throw Error("invalid useKeys")
  const option = {}
  if (postData.ignoreKeys) option.ignoreKeys = postData.ignoreKeys
  return new DB({ sheetName, ssId, keysRow, dataRowFirst }).mergeDataItems(dataItems, postData.useKeys, option);
}

/**
 * @param { Object } postData 
 * @param { string } postData.ssId
 * @param { string } postData.sheetName
 * @param { number } postData.keysRow
 * @param { number } postData.dataRowFirst
 * @param { {[key: string]: any}[] } postData.dataItems
 * @returns 
 */
function getDataItems(postData) {
  // action to replace WB reports
  // body: ssId <string> - id таблицы проекта; sheetName <string> - название листа выгрузки отчета; dataItems <string> - данные выгрузки; 
  const { ssId, sheetName, keysRow = 1, dataRowFirst = 3, } = postData;
  const spreadsheet = Sheets.Spreadsheets.get(ssId);

  return new DB({ sheetName, ssId, keysRow, dataRowFirst }).getDataItems();
}

/**
* 
* @param { Object } postData 
* @param { [string] } postData.ssId
* @param { string } postData.ranges
* @returns 
*/

function batchGet(postData) {
  // action to get values from sheets
  // body: ssId <string> - id таблицы проекта; ranges <[string]> - диапозоны; 

  const { ssId, ranges, majorDimension, valueRenderOption, dateTimeRenderOption } = postData;
  if (!ssId) throw Error(`ssId is not defined`);
  if (!ranges) throw Error(`ranges is not defined`);
  if (!Array.isArray(ranges)) throw Error(`ranges is not array`);
  if (!ranges.length) throw Error(`ranges is empty`);
  const optionalArgs = { ranges: ranges };
  if (majorDimension) {
    if (!(majorDimension in MajorDimension)) throw Error(`invalid majorDimension`);
    optionalArgs.majorDimension = majorDimension;
  }
  if (valueRenderOption) {
    if (!(valueRenderOption in ValueRenderOption)) throw Error(`invalid valueRenderOption`);
    optionalArgs.valueRenderOption = valueRenderOption;
  }
  if (dateTimeRenderOption) {
    if (!(dateTimeRenderOption in DateTimeRenderOption)) throw Error(`invalid dateTimeRenderOption`);
    optionalArgs.dateTimeRenderOption = dateTimeRenderOption;
  }

  let batchResponse = Sheets.Spreadsheets.Values.batchGet(ssId, optionalArgs)
  return batchResponse
}

/**
 * 
 * @param { Object } postData 
 * @param { [string] } postData.ssId
 * @param { [Object] } postData.requestBatchUpdaterData
 * @param { string } postData.requestBatchUpdaterData[i].sheetName
 * @param { [[string|number]] } postData.requestBatchUpdaterData[i].values
 * @param { number } postData.requestBatchUpdaterData[i].dataRowFirst
 * @returns 
 */
function batchUpdate(postData) {

  // action to delete rows and place values to sheets
  // body: ssId <string> - id таблицы проекта; ranges <[string]> - диапозоны; 
  const { ssId, requestBatchUpdaterData } = postData;
  const majorDimension = 'ROWS';
  const valueInputOption = 'USER_ENTERED';
  // const responseValueRenderOption = 'FORMATTED_VALUE';

  const ssSchema = Sheets.Spreadsheets.get(ssId);
  const sheetsGridProps = ssSchema.sheets.reduce((res, sheet) => {
    res[sheet.properties.title] = sheet.properties.gridProperties;
    return res
  }, {})

  // const deleteRanges = [];
  const dummyValue = "";

  /*
    Обновлен алгоритм подготовки данных
    Так чтобы не было дополнительных операций,
    только вставка данных.
    Лишние строки листа затираются ''.
  */
  const data = requestBatchUpdaterData.reduce((res, item) => {
    const dummy = item.values.length ? item.values[0].map(v => dummyValue) : Array.from({ length: sheetsGridProps[item.sheetName].columnCount }, () => dummyValue)
    res.push({
      range: `${item.sheetName}!A${item.dataRowFirst}`,
      majorDimension,
      values: [
        //Вставка values
        ...(item.values?.length ? item.values : []),
        //Заполенние остатка листа при необходимости
        ...(item.values?.length < sheetsGridProps[item.sheetName].rowCount - item.dataRowFirst + 1
          ? Array.from({ length: sheetsGridProps[item.sheetName].rowCount - item.dataRowFirst + 1 - item.values.length ?? 0 }, () => dummy)
          : []
        ),
      ]
    });

    // Удаление строк(*)
    // deleteRanges.push(this.createRangeBatchUpdateDelete(ssId, sheetData.sheetName, sheetData.dataRowFirst ?? 3))

    return res
  }, [])

  // Удаление строк(*)
  // let resource = {
  //   requests: deleteRanges
  // };
  // Sheets.Spreadsheets.batchUpdate(resource, ssId)


  // Блок записи данных
  let requestBatchUpdaterBody = {
    data: data,
    valueInputOption: valueInputOption,
    // responseValueRenderOption: responseValueRenderOption
  }
  // Вызываем метод spreadsheets.values.batchUpdate
  Sheets.Spreadsheets.Values.batchUpdate(requestBatchUpdaterBody, ssId);
  return "data batchUpdate succesfull";
}

function batchClear({ ssId, range }) {
  Sheets.Spreadsheets.Values.batchClear({ ranges: [range] }, ssId);
  return "data clear successful";
}


// Создание папки
function _createFolder(folderName, parentFolderUrl) {
  let parentFolder = DriveApp.getFolderById(this.getFolderIdFromUrl(parentFolderUrl))
  let folders = parentFolder.getFoldersByName(folderName)
  if (folders.hasNext()) {
    return folders.next()
  }
  return parentFolder.createFolder(folderName)
}

// Создание таблицы
function _createSpreadsheet(templateId, sheetName, folder) {
  let templateCopy = DriveApp.getFileById(templateId).makeCopy(sheetName, folder)
  // .setSharing(DriveApp.Access.ANYONE, DriveApp.Permission.EDIT)
  return templateCopy
}

// Создание доступов
function _setPermissions(folder, sheet, admin, client) {

  folder.addEditor(admin)
  folder.addEditor(client)

  sheet.addEditor(admin)
  sheet.addEditor(client)

  DriveApp.getFileById(sheet.getId()).setSharing(DriveApp.Access.PRIVATE, DriveApp.Permission.EDIT)
}

// Получение id по url
function getFolderIdFromUrl(folderUrl) {
  return folderUrl.match(/[-\w]{25,}/)
}

// Формирование данных о строках к удалению
function createRangeBatchUpdateDelete(spreadsheetId, sheetName, startIndex) {
  let sheet = SpreadsheetApp.openById(spreadsheetId).getSheetByName(sheetName),
    lr = sheet.getRange(`A: A`).getLastRow()

  if (startIndex + 1 > lr) sheet.insertRowsAfter(lr, startIndex + 1 - lr)

  let deleteDimension = {
    range: {
      sheetId: sheet.getSheetId(),
      dimension: 'ROWS',
      startIndex: startIndex, // Начать удаление с n-й строки (индексация начинается с 0)
      // endIndex: sheet.getLastRow()+1 // Удалить строки до N-й строки (не включая её)
    }
  }

  console.log(sheet.getRange(`A: A`).getLastRow())
  return { deleteDimension }
}

// Формирование данных о строках к очистке
function createRangeBatchUpdateClear(values) {
  let maxColumnLength = 0;
  let columnLetter = '';

  // Находим максимальную длину подмассивов и соответствующую букву столбца
  values.forEach(dataRow => {
    if (dataRow.length > maxColumnLength) {
      maxColumnLength = dataRow.length;
    }
  });
  if (maxColumnLength <= 26) {
    columnLetter = String.fromCharCode(64 + maxColumnLength);
  } else {
    const firstLetter = String.fromCharCode(64 + Math.floor(maxColumnLength / 26));
    const secondLetter = String.fromCharCode(64 + maxColumnLength % 26);
    columnLetter = firstLetter + secondLetter;
  }
  console.log("Максимальная длина подмассивов: " + maxColumnLength)
  console.log("Буквенное обозначение колонки: " + columnLetter)
  return `${sheetData.sheetName} !A3:${columnLetter} `
}

/**
 *
 * @param { Object } params
 * @param { string } params.ssId - ss id таблицы
 * @param { protectionConfig[] } params.protectionConfigs
 * @returns
 */
function setSheetsProtection(postData) {
  const { ssId, protectionConfigs } = postData;
  if (!ssId) throw Error("ssId is required");
  if (!protectionConfigs) throw Error("protectionConfigs is required");
  if (!Array.isArray(protectionConfigs)) throw Error("protectionConfigs is not array");
  for (const c of protectionConfigs) {
    if (!c.sheetName) throw Error("protectionConfigs must contain sheetName");
  }
  const targetSs = Sheets.Spreadsheets.get(ssId);
  const prefix = `[${targetSs.properties.title}]`;
  const targetSheets = targetSs.sheets.reduce((obj, sheet) => {
    obj[sheet.properties.title] = sheet;
    return obj;
  }, {});

  // protectionConfigs = [
  //   {
  //     sheetName: "protection", //имя листа
  //     protectedRanges: [
  //       {
  //         // range: "F10",
  //         // unprotectedRanges: ["F10", "F12"],
  //         editors: ["lucard9808@gmail.com", "dev007.btlz@gmail.com", "dev005.btlz@gmail.com", "dev008.btlz@gmail.com"],
  //       },
  //     ],
  //   },
  // ];
  const requests = [];
  for (const protectionConfig of protectionConfigs) {
    const sheetName = protectionConfig.sheetName;
    if (sheetName in targetSheets) {
      const targetSheetId = targetSheets[sheetName].properties.sheetId;
      if (targetSheets[sheetName].protectedRanges) {
        for (const protectedRange of targetSheets[sheetName].protectedRanges) {
          requests.push({
            "deleteProtectedRange": {
              "protectedRangeId": protectedRange.protectedRangeId,
            },
          });
        }
      }
      if (protectionConfig.protectedRanges) {
        for (const protectedRange of protectionConfig.protectedRanges) {
          requests.push({
            "addProtectedRange": {
              "protectedRange": {
                "range": toGridRange({ sheetId: targetSheetId, a1: protectedRange.range }),
                "editors": { users: protectedRange.editors },
                "unprotectedRanges": protectedRange.range ? undefined : protectedRange.unprotectedRanges?.map((a1) => toGridRange({ a1, sheetId: targetSheetId })),
              },
            },
          });
        }
      }
    }
  }
  if (!requests.length) {
    const message = `protecton set to sheets skipped`
    console.log(message);
    return message

  }
  const response = Sheets.Spreadsheets.batchUpdate({ requests }, ssId);
  const message = `protecton set to sheets ${protectionConfigs.map(s => `'${s.sheetName}'`).join(";")} `
  return message

}
/**
 *
 * @param { Object } params
 * @param { string } params.ssId - ss id таблицы
 * @param { protectionConfig[] } params.protectionConfigs
 * @returns
 */
function getSpreadsheet(postData) {
  const { ssId } = postData;
  if (!ssId) throw Error("ssId is required");
  const targetSs = Sheets.Spreadsheets.get(ssId);
  return targetSs
}


/**
 * @param {object} params
 * @param {string} sheetName
 * @param {Record<string,any>[]} data
 * @param {number} [dataRowFirst=3] Default is `3`
 * @param {number} [headersRow=1] Default is `1`
 * @param {SpreadsheetApp.Spreadsheet} [ss=SpreadsheetApp.getActiveSpreadsheet()] Default is `SpreadsheetApp.getActiveSpreadsheet()`
 */
function batchUpdateDataSheet({ sheetName, data, dataRowFirst = 3, headersRow = 1, ss = SpreadsheetApp.getActiveSpreadsheet() }) {
  const dataSheet = ss.getSheetByName(sheetName);
  if (!dataSheet) {
    throw new Error(`Лист '${sheetName}' не найден`);
  }
  const dataSheetHeader = ss.getSheetByName(sheetName).getRange(`${headersRow}:${headersRow}`).getValues()?.[0]
  if (!dataSheetHeader) {
    throw new Error(`Заголовок для листа '${sheetName}' не найден`);
  }

  const values = data.map(item => {
    const row = []
    for (let i = 0, key; i < dataSheetHeader.length; i++) {
      key = dataSheetHeader[i]
      if (!key || !(key in item) || item[key] === null) { row[i] = ''; continue }
      row[i] = item[key];
    }
    return row
  })

  /**
   * @typedef {object} requestBatchUpdaterDataItem
   * @property {string} sheetName
   * @property {number} dataRowFirst
   * @property {any[][]} values
   */
  /**
   * @typedef {object} ssActionsBatchUpdateRequest
   * @property {string} ssId
   * @property {requestBatchUpdaterDataItem[]} requestBatchUpdaterData
   */
  /** @type {ssActionsBatchUpdateRequest} */
  const request = {
    ssId: ss.getId(), requestBatchUpdaterData: [
      {
        sheetName,
        dataRowFirst,
        values
      }
    ]
  }
  return batchUpdate(request)
}
