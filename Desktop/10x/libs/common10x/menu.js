
const menuConfig = {
  "title": "📚 MENU_TEST",
  "type": "menu",
  "items": [

    {
      "type": "item", "title": "🔄 Обновить Джем за период", "functionName": "menu__menuUpdateJamClusters",
      "functionProperties": {
        "function": "common1OxMenuHandler",
        "params": { cb: "menuUpdateJamClusters", params: {} }
      },
    },
  ]
}

function getMenuItems() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ssId = ss.getId();
  const sheets = ss.getSheets().map(s => s.getName())
  const menuItems = {
    updareVoronka: {
      "type": "item",
      "title": "🔄 Обновить отчеты ВП 7Д, ВП 30Д",
      "functionName": "menu__voronka_button",
      "functionProperties": {
        "function": "common1OxMenuHandler",
        "params": { cb: menuUpdateVoronka.name, params: { ssId } }
      },

    },
    updateJamClusters: {
      "type": "item",
      "title": "🔄 Обновить Джем за период",
      "functionName": "menu__menuUpdateJamClusters",
      "functionProperties": {
        "function": "common1OxMenuHandler",
        "params": { cb: menuUpdateJamClusters.name, params: { ssId } }
      },
    },
    updateSalesReport: {
      "type": "item",
      "title": "🔄 Обновить ✅ ОтчетПрод за 14 дней",
      "functionName": "menu__btlz_api__ss_datasets_update_sales_report_7_days",
      "functionProperties": {
        "function": "common1OxMenuHandler",
        "params": { cb: menuUpdateSalesReport.name, params: { ssId } }
      },
    },
    updateDatasets: {
      "type": "item",
      "title": "⏩ Обновить данные по SKU",
      "functionName": "menu__btlz_api__ss_datasets_update",
      "functionProperties": {
        "function": "common1OxMenuHandler",
        "params": { cb: menuUpdateDatasets.name, params: { ssId } }
      },
    },
  }
  const result = [menuItems.updareVoronka];
  if (sheets.includes(`✅ ОтчетПрод`)) result.push(menuItems.updateSalesReport);
  if (sheets.includes(`jam_clusters`) && sheets.includes(`Джем`)) result.push(menuItems.updateJamClusters);

  result.push(menuItems.updateDatasets);

  return result;
}

function common1OxMenuHandler({ cb, params }) {
  globalThis[cb](params);
}

function onOpen(t) {
  MenuParser.bindFunctions(menuConfig, t)
  MenuParser.parseMenu(menuConfig)
}

/**
 * @param { Object } params - параметры
 * @param { string } params.url - часть url после корневой api
 * @param { Object } params.payload - часть url после корневой api
 * @param { string } [params.method] - по умолчание - post
 * @param { string } [host]
 * @returns{ UrlFetchApp.HTTPResponse }
 */
function btlzApi(params, host) {
  let response, responseCode, responceContent;
  for (let a = 1; a <= 5; a++) {
    response = BtlzApi.fetch(params, host);
    responseCode = response.getResponseCode();
    responceContent = JSON.parse(response.getContentText())
    if (responseCode === 200) break;
    Utilities.sleep(250);
  }

  if (responseCode !== 200) {
    const title = `API`;
    const messages = []
    if (responseCode === 429) {
      messages.push(`Ошибка получения данных, слишком частые запросы`)
    } else {
      messages.push(responceContent.message)
    }
    showMessage(title, messages)
    return
  }
  if (responceContent.message) console.log(responceContent.message)
  return responceContent
}


// TODO исправление ошибки "is not a valid JSON"
/**
 * @param { Object } params - параметры
 * @param { string } params.url - часть url после корневой api
 * @param { Object } params.payload - часть url после корневой api
 * @param { string } [params.method] - по умолчание - post
 * @returns{ UrlFetchApp.HTTPResponse }
 */
function btlzApiTest(params) {
  let response, responseCode, responceContent;
  for (let a = 1; a <= 5; a++) {
    if (a === 5) {
      console.log("Сервер не ответил после 5 попыток");
      return [];
    }
    response = BtlzApi.fetch(params);
    responseCode = response.getResponseCode();
    try {
      responceContent = JSON.parse(response.getContentText());
    } catch (e) {
      console.error(e);
      Utilities.sleep(5000);
      continue;
    }
    if (responseCode === 200) break;
    Utilities.sleep(250);
  }

  if (responseCode !== 200) {
    const title = `API`;
    const messages = []
    if (responseCode === 429) {
      messages.push(`Ошибка получения данных, слишком частые запросы`)
    } else {
      messages.push(responceContent.message)
    }
    showMessage(title, messages)
    return
  }
  if (responceContent.message) console.log(responceContent.message)
  return responceContent
}

/**
 * Формирует данные для запроса данных по API\
 * Делает запрос для получения датасета wb10xSalesReport_v1\
 * Вставляет данные на лист 'jam_clusters'\
 */
function menuUpdateJamClusters() {
  const SS = SpreadsheetApp.getActiveSpreadsheet();
  const SS_ID = SS.getId();
  const dataSheetName = "jam_clusters"
  const sourceSheetName = "Джем"
  const _showToast = (message) => {
    showToast(message, sourceSheetName, 5)
    console.log(message);
  };
  _showToast("Готовим данные для запроса");


  const sourceSheet = SS.getSheetByName(sourceSheetName);

  const dateFrom = Utilities.formatDate(new Date(sourceSheet.getRange("C1").getValue()), Session.getScriptTimeZone(), "yyyy-MM-dd");
  const dateTo = Utilities.formatDate(new Date(sourceSheet.getRange("D1").getValue()), Session.getScriptTimeZone(), "yyyy-MM-dd");
  const nmIds = [sourceSheet.getRange("A3").getValue()];

  let getDataRequestPayload = {
    url: "/ss/datasets/data",
    payload: {
      "spreadsheet_id": SS_ID,
      "dataset": {
        "name": "wbJamClusters_v1",
        "values": {
          "date_from": dateFrom,
          "date_to": dateTo,
          "nm_ids": nmIds
        }
      }
    }
  };
  console.log(JSON.stringify(getDataRequestPayload))
  _showToast("Запрашиваем данные для обновления");

  const data = btlzApi(getDataRequestPayload);
  if (!data) {
    _showToast(`Не удалось получить данные`);
    console.log(data)
    return
  }
  if (!data?.length) {
    _showToast(`Нет данные для обновления`);
    console.log(data)
    return
  }

  _showToast(`Вставляем данные в таблицу`);
  console.log(batchUpdateDataSheet({
    sheetName: dataSheetName,
    data,
    dataRowFirst: 3,
    headersRow: 1,
    ss: SS
  }))

  _showToast(`Обновление успешно завершено`);
}

function menuUpdateVoronka({ ssId }) {
  requireScopes();
  try {
    menuVoronkaButton7Days({ ssId })
  } catch (error) {
    console.log(error)
    showToast("ВП 30Д не обновлена")
  }
  showToast("ВП 7Д обновлена")

  try {
    menuVoronkaButton30Days({ ssId })
  } catch (error) {
    console.log(error)
    showToast("ВП 30Д не обновлена")
  }
  showToast("ВП 30Д обновлена")

}

function menuVoronkaButton7Days({ ssId }) {
  const postData = {
    action: "wb/analytics/nm-report/refresh_v2",
    ssId,
    analyticsSheetName: "ВставкаВП"
  }
  const result = web_app_wb_analytics_nmreport_refresh_v2(postData);
  console.log(result)
}

function menuVoronkaButton30Days({ ssId }) {
  const postData = {
    action: "wb/analytics/nm-report/refresh_v2",
    ssId,
    analyticsSheetName: "ВставкаВП30",
  }
  const result = web_app_wb_analytics_nmreport_refresh_v2(postData);
  console.log(result)
}

function menuUpdateSalesReport({ ssId }) {
  requireScopes();
  return btlzApi({
    url: "/ss/datasets/update",
    payload: {
      "spreadsheet_id": ssId,
      "dataset": {
        // ✅ ОтчетПрод
        "name": "wb10xSalesReport_v1",
        "values": {
          "date_from": Utilities.formatDate(offsetDate(new Date(), { days: -14 }), "0300", "yyyy-MM-dd")
        }
      }
    }
  })
}

function menuUpdateDatasets({ ssId }) {
  requireScopes();
  return btlzApi({
    url: "/ss/datasets/update",
    payload: { ssId }
  });
}

// Когда-то использовался для обновления данных по воронке продаж
// function wb10xWebApp(postData) {
//   const responce = UrlFetchApp
//     .fetch("https://script.google.com/macros/s/AKfycbysoE9eeVcoEWH1V2gBcmj5lOnn_72iq0nGyytJAM6muzvRaqb_3k_fK4JEFtKMZstYUw/exec", {
//       'method': 'post',
//       'contentType': 'application/json',
//       'muteHttpExceptions': false,
//       'payload': JSON.stringify(postData)
//     });
//   responce.responseCode = responce.getResponseCode();
//   responce.content = responce.getContent();
//   responce.contentText = responce.getContentText();
//   return JSON.parse(responce.getContentText());
// }