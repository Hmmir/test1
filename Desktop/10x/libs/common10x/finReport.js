
// RENAME sheet_name values(target sheet) WHEN PROD
function updateFinReport(SS, SS_ID, host = undefined) {
  requireScopes();
  // Активный лист с Фин отчетом
  const sheet_finreport = SS.getActiveSheet();
  const dataSheetName = sheet_finreport.getName();
  const _showToast = (message, delayTime) => showToast(message, dataSheetName, delayTime);
  const totalRowsCount = SS.getActiveSheet().getLastRow() - 6;

  try {
    _showToast("Подготовка параметров для запроса", -1);

    const CHUNK_SIZE = 1000
    // Строка, с которой начинаются данные на листе
    const FO_SHEET_ROW_START = 7

    const datasetUpdateFields = [
      "nm_id",
      "tags",
      "subject_name",
      "vendor_code",

      "buyout_sum_rub",
      "buyout_count",
      "cancel_sum_rub",
      "cancel_count",
      "income_sum_rub",
      "income_perc",
      "log_sum_rub",
      "log_perc",
      "warehouse_price",
      "warehouse_perc",
      "acceptance_by_nm_id",
      "acceptance_perc",
      "penalty_sum_rub",
      "penalty_perc",
      "additional_payment_total",
      "additional_payment_perc",
      "deduction_sum_total",
      "deduction_perc",
      "acquiring_sum_rub",
      "acquiring_perc",
      "commission_wb",
      "commission_wb_perc",
      "sebes_rub",
      "sebes_perc",
      "promos_sum",
      "promos_perc",
      "external_costs",
      "external_costs_perc",
      "adv_sum",
      "adv_perc",
      "total_wb_comission",
      "total_to_pay",
      "total_to_pay_perc",
      "tax",
      "tax_perc",
      // "marg_val",
      // "ros",
      // "roi",
      "total_wb_comission",
      "total_wb_comission_perc",
      "direct_costs_no_tax",
      "direct_costs_no_tax_perc",
      "marg_val_no_tax",
      "marg_val_no_tax_perc",
    ];

    // Получение dateFrom и dateTo
    const dateFromInput = new Date(sheet_finreport.getRange("B2").getValue());
    const dateToInput = new Date(sheet_finreport.getRange("C2").getValue());
    const dateFrom = Utilities.formatDate(dateFromInput, Session.getScriptTimeZone(), "yyyy-MM-dd");
    const dateTo = Utilities.formatDate(dateToInput, Session.getScriptTimeZone(), "yyyy-MM-dd");

    // Получение sid
    const cabinetName = sheet_finreport.getRange("B3").getValue();
    const sheets = Sheets.Spreadsheets.Values;
    let sid;
    if (cabinetName === "Все") {
      sid = "";
    } else {
      const sidsDataRange = `'sids'!A1:ZZ`;
      const sidsData = sheets.get(SS_ID, sidsDataRange);
      console.log(sidsData.values)
      const sidData = sidsData.values.find((item) => {
        if (item[1] === cabinetName) {
          return item
        }
      })

      if (!sidsData) {
        _showToast(`SID не найден`, 5);
        return;
      }
      sid = sidData[0]
    }

    let getNmidsRequestPayload = {
      url: "/ss/datasets/data",
      payload: {
        "spreadsheet_id": SS_ID,
        "dataset": {
          "name": "wbCardsData_v1",
          "values": {
            "sid": sid
          }
        }
      }
    };
    _showToast("Запрос данных об SKU");

    const nmidsData = btlzApi(getNmidsRequestPayload, host);
    if (!nmidsData) {
      _showToast(`Не удалось получить данные`);
      console.log(nmidsData)
      return
    }
    if (!nmidsData?.length) {
      _showToast(`Нет данных для обновления`);
      console.log(nmidsData)
      return
    }
    const data = []

    if (nmidsData.length > CHUNK_SIZE) {
      const requestsCount = Math.ceil(nmidsData.length / CHUNK_SIZE)
      for (let i = 0; i < requestsCount; i++) {
        const nmids = nmidsData.slice(i * CHUNK_SIZE, (i + 1) === requestsCount ? undefined : (i + 1) * CHUNK_SIZE).map(item => item.nm_id);

        if (i === 0) {
          nmids.unshift(0)
        }

        const getDataRequestPayload = {
          url: "/ss/datasets/data",
          payload: {
            "spreadsheet_id": SS_ID,
            "dataset": {
              "name": "wb10xSalesFinReportTotal_v1",
              "values": {
                "date_from": dateFrom,
                "date_to": dateTo,
                "sid": "",
                "nm_ids": nmids
              }
            }
          }
        };
        console.log(JSON.stringify(getDataRequestPayload.payload, null, 2))
        _showToast(`Запрос данных для обновления. Этап ${i + 1}/${requestsCount}`)

        const batchData = btlzApi(getDataRequestPayload, host)
        if (!batchData) {
          _showToast(`Не удалось получить данные`);
          console.log(batchData)
          return
        }
        if (!batchData?.length) {
          _showToast(`Нет данных для обновления`);
          console.log(batchData)
          return
        }

        data.push(...batchData)
      }
    } else {
      const getDataRequestPayload = {
        url: "/ss/datasets/data",
        payload: {
          "spreadsheet_id": SS_ID,
          "dataset": {
            "name": "wb10xSalesFinReportTotal_v1",
            "values": {
              "date_from": dateFrom,
              "date_to": dateTo,
              "sid": sid,
              "forced": true,
            }
          }
        }
      };
      _showToast(`Запрос данных для обновления.`)
      const batchData = btlzApi(getDataRequestPayload, host)
      if (!batchData) {
        _showToast(`Не удалось получить данные`);
        console.log(batchData)
        return
      }
      if (!batchData?.length) {
        _showToast(`Нет данных для обновления`);
        console.log(batchData)
        return
      }

      data.push(...batchData)
    }

    const finReportDataRange = `'${dataSheetName}'!A1:ZZ`;
    const finReportData = sheets.get(SS_ID, finReportDataRange).values;
    const headerIndexes = finReportData[0];
    const nmIdsIndex = headerIndexes.indexOf("nm_id");

    if (nmIdsIndex === -1) {
      _showToast(`Технические столбцы не найдены`, 5);
      return
    }

    let updates = [];
    let tagsRange = [];
    for (let field of datasetUpdateFields) {

      const columnIndex = headerIndexes.indexOf(field);
      if (columnIndex === -1) {
        _showToast(`Технические столбцы не найдены`, 5);
        return
      }
      const columnLetter = letter(columnIndex + 1);
      const col_range = `'${dataSheetName}'!${columnLetter}${FO_SHEET_ROW_START}:${columnLetter}`;
      const col_values = data.map((row, i) => {
        if (row && row.hasOwnProperty("nm_id")) {
          if ((field === 'tax_perc' || field === 'roi') && tagsRange[i][0] === 'Нет в UNIT') {
            return ['-']
          } else if (row[field] !== undefined && row[field] !== null) {
            return [row[field]];
          } else if (row[field] === null && field === 'tax') {
            return ['-'];
          } else {
            return [0]
          }
        } else {
          return [""];
        }

      })

      // clear
      if (col_values.length < totalRowsCount) {
        for (let i = col_values.length; i < totalRowsCount; i++) {
          col_values.push([""]);
        }
      }

      if (field === "tags") {
        tagsRange = col_values;
      }
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
  } catch (e) {
    _showToast("Ошибка: что-то пошло не так", -1);
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
